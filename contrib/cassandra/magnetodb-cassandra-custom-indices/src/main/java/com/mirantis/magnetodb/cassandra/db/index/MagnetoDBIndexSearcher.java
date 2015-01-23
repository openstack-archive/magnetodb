package com.mirantis.magnetodb.cassandra.db.index;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

public class MagnetoDBIndexSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MagnetoDBIndexSearcher.class);

    private static Set<Operator> leftSideOperators = new HashSet<Operator>(Arrays.asList(new Operator[]{
            Operator.EQ, Operator.GT, Operator.GTE
    }));

    private static Set<Operator> rightSideOperators = new HashSet<Operator>(Arrays.asList(new Operator[]{
            Operator.EQ, Operator.LT, Operator.LTE
    }));

    private static Set<Operator> strictOperators = new HashSet<Operator>(Arrays.asList(new Operator[]{
            Operator.GT, Operator.LT
    }));

    public MagnetoDBIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        super(indexManager, columns);
    }

    @Override
    protected IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
    {
        for (IndexExpression expression : clause)
        {
            // skip columns belonging to a different index type
            if (!columns.contains(expression.column))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column);
            if (index != null)
                return  expression;

        }

        return null;
    }

    @Override
    public boolean canHandleIndexClause(List<IndexExpression> clause) {
        return true;
    }

    @Override
    public List<Row> search(ExtendedFilter filter)
    {
        assert filter.getClause() != null && !filter.getClause().isEmpty();
        // TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try* to delete stale entries, without blocking if there's no room
        // as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room being made

        Map<String, Object> query_options = null;
        MagnetoDBLocalSecondaryIndex indexToSearch = null;

        ByteBuffer indexedColumnName = null;

        Iterator<IndexExpression> iter = filter.getClause().iterator();
        while (iter.hasNext()) {
            IndexExpression expr = iter.next();
            MagnetoDBLocalSecondaryIndex index =
                    (MagnetoDBLocalSecondaryIndex) indexManager.getIndexForColumn(expr.column);
            if (index.isQueryPropertiesField) {
                assert query_options == null;
                try {
                    String sqo = ByteBufferUtil.string(expr.value);
                    query_options = MagnetoDBLocalSecondaryIndex.QueryOptions.parse(sqo);
                } catch (CharacterCodingException e) {
                    throw new RuntimeException(e);
                }
                iter.remove();
            } else {
                assert (indexedColumnName == null) || (indexedColumnName == expr.column);
                indexedColumnName = expr.column;
                indexToSearch = index;
            }
        }

        try (OpOrder.Group writeOp = baseCfs.keyspace.writeOrder.start(); OpOrder.Group baseOp = baseCfs.readOrdering.start(); OpOrder.Group indexOp = indexToSearch.getIndexCfs().readOrdering.start())
        {
            return baseCfs.filter(getIndexedIterator(writeOp, filter, indexToSearch, query_options), filter);
        }
    }

    private Composite makePrefix(MagnetoDBLocalSecondaryIndex index, ByteBuffer partition_key,
                                 ExtendedFilter filter, boolean isStart)
    {
        SliceQueryFilter columnFilter = (SliceQueryFilter) filter.columnFilter(partition_key);
        Composite baseColumnName = isStart ? columnFilter.start() : columnFilter.finish();
        for (IndexExpression indexRestriction : filter.getClause()) {
            if (!index.columnDef.name.bytes.equals(indexRestriction.column)) {
                continue;
            }
            if (isStart && !leftSideOperators.contains(indexRestriction.operator)) {
                continue;
            }
            if (!isStart && !rightSideOperators.contains(indexRestriction.operator)) {
                continue;
            }
            CBuilder prefixBuilder = index.getIndexComparator().prefixBuilder().add(indexRestriction.value);
            if (strictOperators.contains(indexRestriction.operator) ||
                    baseColumnName.equals(Composites.EMPTY)) {
                Composite prefix = prefixBuilder.build();
                switch (indexRestriction.operator){
                    case GT: return prefix.end();
                    case GTE: return prefix;
                    case EQ: return isStart ? prefix : prefix.end();
                    case LT: return prefix;
                    case LTE: return prefix.end();
                }
            } else {
                for (int i=0; i < baseColumnName.size(); i++) {
                    prefixBuilder.add(baseColumnName.get(i));
                }
                return prefixBuilder.build().withEOC(baseColumnName.eoc());
            }
        }
        return Composites.EMPTY;
    }

    private ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final OpOrder.Group writeOp, final ExtendedFilter filter, final MagnetoDBLocalSecondaryIndex index, Map<String, Object> query_options)
    {
        MagnetoDBLocalSecondaryIndex.QueryOptions.OrderType order =
                (MagnetoDBLocalSecondaryIndex.QueryOptions.OrderType)query_options.get(
                        MagnetoDBLocalSecondaryIndex.QueryOptions.ORDER);
        final boolean reversed = (order == MagnetoDBLocalSecondaryIndex.QueryOptions.OrderType.DESC);

        final CellNameType indexComparator = index.getIndexCfs().getComparator();

        final DecoratedKey basicCFPartitionKey = (DecoratedKey) filter.dataRange.keyRange().left;
        final DecoratedKey indexCFPartitionKey = index.getIndexCfs().partitioner.decorateKey(basicCFPartitionKey.getKey());

        final Composite startPrefix = makePrefix(index, basicCFPartitionKey.getKey(), filter, !reversed);
        final Composite endPrefix = makePrefix(index, basicCFPartitionKey.getKey(), filter, reversed);

        return new ColumnFamilyStore.AbstractScanIterator()
        {
            private Composite lastSeenPrefix = startPrefix;
            private Deque<Cell> indexCells;
            private int columnsRead = Integer.MAX_VALUE;
            private int limit = filter.currentLimit();
            private int columnsCount = 0;

            private int meanColumns = Math.max(index.getIndexCfs().getMeanColumns(), 1);
            // We shouldn't fetch only 1 row as this provides buggy paging in case the first row doesn't satisfy all clauses
            private int rowsPerQuery = Math.max(Math.min(filter.maxRows(), filter.maxColumns() / meanColumns), 2);

            public boolean needsFiltering()
            {
                return false;
            }

            private Row makeReturn(DecoratedKey key, ColumnFamily data)
            {
                if (data == null)
                    return endOfData();

                assert key != null;
                return new Row(key, data);
            }

            protected Row computeNext()
            {
                ColumnFamily data = null;

                while (true)
                {
                    // Did we get more columns that needed to respect the user limit?
                    // (but we still need to return what has been fetched already)
                    if (columnsCount >= limit)
                        return makeReturn(basicCFPartitionKey, data);

                    if (indexCells == null || indexCells.isEmpty())
                    {
                        if (columnsRead < rowsPerQuery)
                        {
                            logger.trace("Read only {} (< {}) last page through, must be done", columnsRead, rowsPerQuery);
                            return makeReturn(basicCFPartitionKey, data);
                        }

                        QueryFilter indexFilter = QueryFilter.getSliceFilter(indexCFPartitionKey,
                                index.getIndexCfs().name,
                                lastSeenPrefix,
                                endPrefix,
                                reversed,
                                rowsPerQuery,
                                filter.timestamp);
                        ColumnFamily indexRow = index.getIndexCfs().getColumnFamily(indexFilter);
                        if (indexRow == null || !indexRow.hasColumns())
                            return makeReturn(basicCFPartitionKey, data);

                        Collection<Cell> sortedCells =
                                reversed ? indexRow.getReverseSortedColumns() : indexRow.getSortedColumns();

                        columnsRead = sortedCells.size();
                        indexCells = new ArrayDeque<>(sortedCells);
                    }

                    while (!indexCells.isEmpty() && columnsCount <= limit)
                    {
                        Cell cell = indexCells.poll();
                        lastSeenPrefix = cell.name();
                        if (!cell.isLive(filter.timestamp))
                        {
                            logger.trace("skipping {}", cell.name());
                            continue;
                        }

                        MagnetoDBLocalSecondaryIndex.IndexedEntry entry = index.decodeEntry(cell);
                        Composite originalPrefix = entry.originalColumnNameBuilder.build();

                        logger.trace("Adding index hit to current row for {}", indexComparator.getString(cell.name()));

                        // We always query the whole CQL3 row. In the case where the original filter was a name filter this might be
                        // slightly wasteful, but this probably doesn't matter in practice and it simplify things.
                        ColumnSlice dataSlice = new ColumnSlice(originalPrefix, originalPrefix.end());
                        // If the table has static columns, we must fetch them too as they may need to be returned too.
                        // Note that this is potentially wasteful for 2 reasons:
                        //  1) we will retrieve the static parts for each indexed row, even if we have more than one row in
                        //     the same partition. If we were to group data queries to rows on the same slice, which would
                        //     speed up things in general, we would also optimize here since we would fetch static columns only
                        //     once for each group.
                        //  2) at this point we don't know if the user asked for static columns or not, so we might be fetching
                        //     them for nothing. We would however need to ship the list of "CQL3 columns selected" with getRangeSlice
                        //     to be able to know that.
                        // TODO: we should improve both point above
                        ColumnSlice[] slices = baseCfs.metadata.hasStaticColumns()
                                ? new ColumnSlice[]{ baseCfs.metadata.comparator.staticPrefix().slice(), dataSlice }
                                : new ColumnSlice[]{ dataSlice };
                        SliceQueryFilter dataFilter = new SliceQueryFilter(slices, false, Integer.MAX_VALUE, baseCfs.metadata.clusteringColumns().size());
                        ColumnFamily newData = baseCfs.getColumnFamily(new QueryFilter(basicCFPartitionKey, baseCfs.name, dataFilter, filter.timestamp));
                        if (newData == null || index.isStale(entry, newData, filter.timestamp))
                        {
                            index.delete(indexCFPartitionKey.getKey(), cell, writeOp);
                            continue;
                        }

                        assert newData != null : "An entry with no data should have been considered stale";

                        // We know the entry is not stale and so the entry satisfy the primary clause. So whether
                        // or not the data satisfies the other clauses, there will be no point to re-check the
                        // same CQL3 row if we run into another collection value entry for this row.
                        if (!filter.isSatisfiedBy(basicCFPartitionKey, newData, originalPrefix, null))
                            continue;

                        if (data == null)
                            data = ArrayBackedSortedColumns.factory.create(baseCfs.metadata);
                        data.addAll(newData);
                        columnsCount += dataFilter.lastCounted();
                    }
                    lastSeenPrefix = reversed ? lastSeenPrefix.end() : lastSeenPrefix.start();
                }
            }

            public void close() throws IOException {}
        };
    }
}
