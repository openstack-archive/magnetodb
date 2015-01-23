package com.mirantis.magnetodb.cassandra.db.index;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;


public class MagnetoDBLocalSecondaryIndex extends PerColumnSecondaryIndex
{
    public static final String QUERY_PROPERTIES_FIELD = "query_properties_field";


    protected ColumnFamilyStore indexCfs;

    // SecondaryIndex "forces" a set of ColumnDefinition. However this class (and thus it's subclass)
    // only support one def per index. So inline it in a field for 1) convenience and 2) avoid creating
    // an iterator each time we need to access it.
    // TODO: we should fix SecondaryIndex API
    protected ColumnDefinition columnDef;
    protected CellNameType indexComparator;
    protected boolean isQueryPropertiesField;

    public CellNameType getIndexComparator() {
        return indexComparator;
    }

    public void init()
    {
        assert baseCfs != null && columnDefs != null && columnDefs.size() == 1;

        columnDef = columnDefs.iterator().next();

        isQueryPropertiesField = Boolean.parseBoolean(columnDef.getIndexOptions().get(QUERY_PROPERTIES_FIELD));

        if (!isQueryPropertiesField) {
            int prefixSize = columnDef.position();
            List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(prefixSize + 1);
            types.add(columnDef.type);
            for (int i = 0; i < prefixSize; i++)
                types.add(baseCfs.metadata.comparator.subtype(i));
            indexComparator = new CompoundDenseCellNameType(types);

            AbstractType<?> keyType = baseCfs.metadata.getKeyValidator();
            CFMetaData indexedCfMetadata = CFMetaData.newIndexMetadata(baseCfs.metadata, columnDef, indexComparator)
                    .keyValidator(keyType)
                    .rebuild();
            indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                    indexedCfMetadata.cfName, new LocalPartitioner(keyType),
                    indexedCfMetadata);
        }
    }

    protected CellName makeIndexColumnName(ByteBuffer rowKey, Cell cell) {
        CBuilder builder = getIndexComparator().prefixBuilder();
        builder.add(cell.value());
        CellName cellName = cell.name();
        for (int i = 0; i < Math.min(columnDef.position(), cellName.size()); i++)
            builder.add(cellName.get(i));

        return getIndexComparator().create(builder.build(), columnDef);
    }

    public void delete(ByteBuffer rowKey, Cell cell, OpOrder.Group opGroup)
    {
        deleteForCleanup(rowKey, cell, opGroup);
    }

    public void deleteForCleanup(ByteBuffer rowKey, Cell cell, OpOrder.Group opGroup)
    {
        if (isQueryPropertiesField)
            throw new UnsupportedOperationException();

        if (!cell.isLive())
            return;

        DecoratedKey valueKey = new BufferDecoratedKey(indexCfs.partitioner.getToken(rowKey), rowKey);;
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        ColumnFamily cfi = ArrayBackedSortedColumns.factory.create(indexCfs.metadata, false, 1);
        cfi.addTombstone(makeIndexColumnName(rowKey, cell), localDeletionTime, cell.timestamp());
        indexCfs.apply(valueKey, cfi, SecondaryIndexManager.nullUpdater, opGroup, null);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", valueKey, cfi);
    }

    public void insert(ByteBuffer rowKey, Cell cell, OpOrder.Group opGroup)
    {
        if (isQueryPropertiesField)
            return;

        DecoratedKey valueKey = new BufferDecoratedKey(indexCfs.partitioner.getToken(rowKey), rowKey);
        ColumnFamily cfi = ArrayBackedSortedColumns.factory.create(indexCfs.metadata, false, 1);
        CellName name = makeIndexColumnName(rowKey, cell);
        if (cell instanceof ExpiringCell)
        {
            ExpiringCell ec = (ExpiringCell) cell;
            cfi.addColumn(new BufferExpiringCell(name, ByteBufferUtil.EMPTY_BYTE_BUFFER, ec.timestamp(), ec.getTimeToLive(), ec.getLocalDeletionTime()));
        }
        else
        {
            cfi.addColumn(new BufferCell(name, ByteBufferUtil.EMPTY_BYTE_BUFFER, cell.timestamp()));
        }
        if (logger.isDebugEnabled())
            logger.debug("applying index row {} in {}", indexCfs.metadata.getKeyValidator().getString(valueKey.getKey()), cfi);

        indexCfs.apply(valueKey, cfi, SecondaryIndexManager.nullUpdater, opGroup, null);
    }

    static boolean shouldCleanupOldValue(Cell oldCell, Cell newCell) {
        // If any one of name/value/timestamp are different, then we
        // should delete from the index. If not, then we can infer that
        // at least one of the cells is an ExpiringColumn and that the
        // difference is in the expiry time. In this case, we don't want to
        // delete the old value from the index as the tombstone we insert
        // will just hide the inserted value.
        // Completely identical cells (including expiring columns with
        // identical ttl & localExpirationTime) will not get this far due
        // to the oldCell.equals(newColumn) in StandardUpdater.update
        return !oldCell.name().equals(newCell.name())
                || !oldCell.value().equals(newCell.value())
                || oldCell.timestamp() != newCell.timestamp();
    }

    public void update(ByteBuffer rowKey, Cell oldCol, Cell col, OpOrder.Group opGroup)
    {
        if (isQueryPropertiesField)
            return;

        // insert the new value before removing the old one, so we never have a period
        // where the row is invisible to both queries (the opposite seems preferable); see CASSANDRA-5540
        insert(rowKey, col, opGroup);
        if (shouldCleanupOldValue(oldCol, col))
            delete(rowKey, oldCol, opGroup);
    }

    public void removeIndex(ByteBuffer columnName)
    {
        if (isQueryPropertiesField)
            return;
        indexCfs.invalidate();
    }

    public void forceBlockingFlush()
    {
        if (isQueryPropertiesField)
            return;

        Future<?> wait;
        // we synchronise on the baseCfs to make sure we are ordered correctly with other flushes to the base CFS
        synchronized (baseCfs.getDataTracker())
        {
            wait = indexCfs.forceFlush();
        }
        FBUtilities.waitOnFuture(wait);
    }

    public void invalidate()
    {
        if (isQueryPropertiesField)
            return;
        indexCfs.invalidate();
    }

    public void truncateBlocking(long truncatedAt)
    {
        if (isQueryPropertiesField)
            return;
        indexCfs.discardSSTables(truncatedAt);
    }

    public ColumnFamilyStore getIndexCfs()
    {
        if (isQueryPropertiesField)
            return null;
        return indexCfs;
    }

    public String getIndexName()
    {
        return baseCfs.metadata.indexColumnFamilyName(columnDef);
    }

    public long estimateResultRows()
    {
        if (isQueryPropertiesField)
            return 0;
        return getIndexCfs().getMeanColumns();
    }

    public void reload()
    {
        if (isQueryPropertiesField)
            return;
        indexCfs.metadata.reloadSecondaryIndexMetadata(baseCfs.metadata);
        indexCfs.reload();
    }

    @Override
    public void validateOptions() throws ConfigurationException {
        return;
    }

    @Override
    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
        return new MagnetoDBIndexSearcher(baseCfs.indexManager, columns);
    }

    @Override
    public boolean indexes(CellName name)
    {
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return name.size() > columnDef.position()
                && comp.compare(name.get(columnDef.position()), columnDef.name.bytes) == 0;
    }
    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        if (isQueryPropertiesField)
            throw new UnsupportedOperationException();

        CellName name = data.getComparator().create(entry.originalColumnNameBuilder.build(), columnDef);
        Cell cell = data.getColumn(name);
        return cell == null || !cell.isLive(now) || columnDef.type.compare(entry.indexValue, cell.value()) != 0;
    }

    public IndexedEntry decodeEntry(Cell indexEntry)
    {
        if (isQueryPropertiesField)
            throw new UnsupportedOperationException();

        CBuilder builder = baseCfs.getComparator().builder();
        for (int i = 0; i < columnDef.position(); i++)
            builder.add(indexEntry.name().get(i + 1));
        return new IndexedEntry(builder, indexEntry.name().get(0));
    }

    public static class IndexedEntry {
        public final ByteBuffer indexValue;
        public final CBuilder originalColumnNameBuilder;

        public IndexedEntry(CBuilder originalColumnNameBuilder, ByteBuffer indexValue)
        {
            this.indexValue = indexValue;
            this.originalColumnNameBuilder = originalColumnNameBuilder;
        }
    }

    public static class QueryOptions {
        public static enum OrderType {
            ASC, DESC
        }

        public static final String ORDER = "ORDER";

        public static Map<String, Object> parse(String query_parameters) {
            Map<String, Object> res = new HashMap<String, Object>();
            query_parameters = query_parameters.trim();
            if (query_parameters.isEmpty()) {
                return res;
            }
            String params[] = query_parameters.split(";");
            for (String param : params) {
                param = param.trim();
                String keyValue[] = param.split(":");
                assert keyValue.length == 2;

                String key = keyValue[0];
                Object value;

                switch (key) {
                    case ORDER: value = OrderType.valueOf(keyValue[1]); break;
                    default: throw new RuntimeException("Unknown query property: " + key);
                }

                res.put(key, value);
            }

            return res;
        }
    }
}
