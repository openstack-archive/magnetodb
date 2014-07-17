package com.mirantis.magnetodb.cassandra.db.index;

import org.apache.cassandra.cql.SelectExpression;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

public class MagnetoDBIndexSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MagnetoDBIndexSearcher.class);

    private static Set<IndexOperator> leftSideOperators = new HashSet<IndexOperator>(Arrays.asList(new IndexOperator[]{
        IndexOperator.EQ, IndexOperator.GT, IndexOperator.GTE
    }));

    private static Set<IndexOperator> rightSideOperators = new HashSet<IndexOperator>(Arrays.asList(new IndexOperator[]{
            IndexOperator.EQ, IndexOperator.LT, IndexOperator.LTE
    }));

    private static Set<IndexOperator> strictOperators = new HashSet<IndexOperator>(Arrays.asList(new IndexOperator[]{
            IndexOperator.GT, IndexOperator.LT
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
            if (!columns.contains(expression.column_name))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
            if (index != null)
                return  expression;

        }

        return null;
    }

    @Override
    public List<Row> search(ExtendedFilter filter)
    {
        assert filter.getClause() != null && !filter.getClause().isEmpty();
        return baseCfs.filter(getIndexedIterator(filter), filter);
    }

    private ByteBuffer makePrefix(MagnetoDBLocalSecondaryIndex index, DecoratedKey partition_key,
                                  List<IndexExpression> indexRestrictionList, ExtendedFilter filter, boolean isStart)
    {
        SliceQueryFilter columnFilter = (SliceQueryFilter) filter.columnFilter(partition_key.key);
        ByteBuffer base_column_name = isStart ? columnFilter.start() : columnFilter.finish();

        for (IndexExpression indexRestriction : indexRestrictionList) {
            if (isStart && !leftSideOperators.contains(indexRestriction.op)) {
                continue;
            }
            if (!isStart && !rightSideOperators.contains(indexRestriction.op)) {
                continue;
            }

            ColumnNameBuilder builder = ((CompositeType)index.indexCfs.getComparator()).builder().
                    add(indexRestriction.value);

            if (strictOperators.contains(indexRestriction.op) ||
                    base_column_name.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)) {
                switch (indexRestriction.op){
                    case GT:    return builder.buildForRelation(Relation.Type.GT);
                    case GTE:   return builder.buildForRelation(Relation.Type.GTE);
                    case EQ:    return builder.buildForRelation(isStart ? Relation.Type.GTE : Relation.Type.LTE);
                    case LT:    return builder.buildForRelation(Relation.Type.LT);
                    case LTE:   return builder.buildForRelation(Relation.Type.LTE);
                }
            } else {
                ByteBuffer prefix = builder.build();
                DataOutputBuffer out = new DataOutputBuffer();
                try {
                    ByteBufferUtil.write(prefix, out);
                    ByteBufferUtil.write(base_column_name, out);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                return ByteBuffer.wrap(out.getData(), 0, out.getLength());
            }
        }

        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    private ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final ExtendedFilter filter) {
        Map<String, String> query_options = null;
        List<IndexExpression> indexValueRestrictionList = new ArrayList<IndexExpression>();
        MagnetoDBLocalSecondaryIndex indexToSearch = null;

        ByteBuffer indexedColumnName = null;

        Iterator<IndexExpression> iter = filter.getClause().iterator();
        while (iter.hasNext()) {
            IndexExpression expr = iter.next();
            MagnetoDBLocalSecondaryIndex index =
                    (MagnetoDBLocalSecondaryIndex) indexManager.getIndexForColumn(expr.column_name);
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
                assert (indexedColumnName == null) || (indexedColumnName == expr.column_name);
                indexedColumnName = expr.column_name;
                indexToSearch = index;
                indexValueRestrictionList.add(expr);
            }
        }

        final MagnetoDBLocalSecondaryIndex index = indexToSearch;
        final boolean reversed = Boolean.parseBoolean(
                query_options.get(MagnetoDBLocalSecondaryIndex.QueryOptions.REVERSED));
        final CompositeType indexComparator = (CompositeType) index.getIndexCfs().getComparator();

        final DecoratedKey basicCFPartitionKey = (DecoratedKey) filter.dataRange.keyRange().left;
        final DecoratedKey indexCFPartitionKey = index.getIndexCfs().partitioner.decorateKey(basicCFPartitionKey.key);

        final ByteBuffer startPrefix = makePrefix(index, basicCFPartitionKey,
                indexValueRestrictionList, filter, !reversed);
        final ByteBuffer endPrefix = makePrefix(index, basicCFPartitionKey,
                indexValueRestrictionList, filter, reversed);

        final int limit = Math.min(filter.currentLimit(), SelectExpression.MAX_COLUMNS_DEFAULT);

        return new ColumnFamilyStore.AbstractScanIterator() {
            private int columnCount = 0;
            int columnToFetchCount = 0;
            int fetchedColumnCount = 0;
            private ByteBuffer lastPrefixSeen = startPrefix;

            public boolean needsFiltering() {
                return false;
            }

            private Row makeReturn(DecoratedKey key, ColumnFamily data) {
                if (data == null)
                    return endOfData();

                assert key != null;
                return new Row(key, data);
            }

            protected Row computeNext() {
            /*
             * Our internal index code is wired toward internal rows. So we need to accumulate all results for a given
             * row before returning from this method. Which unfortunately means that this method has to do what
             * CFS.filter does for KeysIndex.
             */
                ColumnFamily data = null;

MAIN_LOOP:      while (fetchedColumnCount >= columnToFetchCount) {
                    columnToFetchCount = (limit - columnCount);
                    columnToFetchCount += Math.max(columnToFetchCount / 10, 2);

                    QueryFilter indexFilter = QueryFilter.getSliceFilter(indexCFPartitionKey,
                            index.getIndexCfs().name,
                            lastPrefixSeen,
                            endPrefix,
                            reversed,
                            columnToFetchCount,
                            filter.timestamp);

                    if (indexFilter == null)
                        break MAIN_LOOP;

                    ColumnFamily indexRow = index.getIndexCfs().getColumnFamily(indexFilter);
                    if (indexRow == null || indexRow.getColumnCount() == 0)
                        break MAIN_LOOP;

                    fetchedColumnCount = indexRow.getColumnCount();

                    Collection<Column> sortedColumns =
                            reversed ? indexRow.getReverseSortedColumns() : indexRow.getSortedColumns();

                    for (Column column : sortedColumns) {
                        lastPrefixSeen = column.name();
                        if (column.isMarkedForDelete(filter.timestamp)) {
                            logger.trace("skipping {}", column.name());
                            continue;
                        }

                        MagnetoDBLocalSecondaryIndex.IndexedEntry entry = index.decodeEntry(basicCFPartitionKey, column);

                        ByteBuffer start = entry.originalColumnNameStart();

                        logger.trace("Adding index hit to current row for {}", indexComparator.getString(column.name()));

                        // We always query the whole CQL3 row. In the case where the original filter was a name filter this might be
                        // slightly wasteful, but this probably doesn't matter in practice and it simplify things.
                        ColumnSlice dataSlice = new ColumnSlice(start, entry.originalColumnNameEnd());
                        ColumnSlice[] slices;
                        if (baseCfs.metadata.hasStaticColumns()) {
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
                            ColumnSlice staticSlice = new ColumnSlice(ByteBufferUtil.EMPTY_BYTE_BUFFER, baseCfs.metadata.getStaticColumnNameBuilder().buildAsEndOfRange());
                            slices = new ColumnSlice[]{staticSlice, dataSlice};
                        } else {
                            slices = new ColumnSlice[]{dataSlice};
                        }
                        SliceQueryFilter dataFilter = new SliceQueryFilter(slices, false, Integer.MAX_VALUE, baseCfs.metadata.clusteringKeyColumns().size());
                        ColumnFamily newData = baseCfs.getColumnFamily(new QueryFilter(basicCFPartitionKey, baseCfs.name, dataFilter, filter.timestamp));
                        if (newData == null || index.isStale(entry, newData, filter.timestamp)) {
                            index.delete(indexCFPartitionKey.key, column);
                            continue;
                        }

                        assert newData != null : "An entry with not data should have been considered stale";

                        if (!filter.isSatisfiedBy(basicCFPartitionKey, newData, entry.originalColumnNameBuilder))
                            continue;

                        if (data == null) {
                            data = UnsortedColumns.factory.create(baseCfs.metadata);
                        }
                        data.resolve(newData);
                        columnCount++;
                        if (columnCount >= limit) {
                            break MAIN_LOOP;
                        }
                    }
                    lastPrefixSeen = ByteBufferUtil.clone(lastPrefixSeen);
                    lastPrefixSeen.put(lastPrefixSeen.remaining() - 1, (byte)(reversed ? -1 : 1));
                }

                return makeReturn(basicCFPartitionKey, data);
            }

            public void close() throws IOException {
            }
        };
    }
}
