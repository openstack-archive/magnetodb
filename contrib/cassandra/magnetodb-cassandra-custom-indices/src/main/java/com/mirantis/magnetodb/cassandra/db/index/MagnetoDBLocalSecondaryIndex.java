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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;


public class MagnetoDBLocalSecondaryIndex extends PerColumnSecondaryIndex
{
    public static final String QUERY_PROPERTIES_FIELD = "query_properties_field";


    protected ColumnFamilyStore indexCfs;

    // SecondaryIndex "forces" a set of ColumnDefinition. However this class (and thus it's subclass)
    // only support one def per index. So inline it in a field for 1) convenience and 2) avoid creating
    // an iterator each time we need to access it.
    // TODO: we should fix SecondaryIndex API
    protected ColumnDefinition columnDef;
    protected boolean isQueryPropertiesField;


    private static CompositeType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(((CompositeType)baseMetadata.comparator).types);
        types.add(0, columnDef.getValidator());
        return CompositeType.getInstance(types);
    }

    private static CFMetaData buildIndexMetadata(CFMetaData parent, ColumnDefinition info)
    {
        // Depends on parent's cache setting, turn on its index CF's cache.
        // Row caching is never enabled; see CASSANDRA-5732
        CFMetaData.Caching indexCaching =
                parent.getCaching() == CFMetaData.Caching.ALL || parent.getCaching() == CFMetaData.Caching.KEYS_ONLY
                        ? CFMetaData.Caching.KEYS_ONLY
                        : CFMetaData.Caching.NONE;

        AbstractType<?> columnComparator = buildIndexComparator(parent, info);

        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, columnComparator, (AbstractType)null)
                .keyValidator(parent.getKeyValidator())
                .readRepairChance(0.0)
                .dcLocalReadRepairChance(0.0)
                .gcGraceSeconds(0)
                .caching(indexCaching)
                .speculativeRetry(parent.getSpeculativeRetry())
                .compactionStrategyClass(parent.compactionStrategyClass)
                .compactionStrategyOptions(parent.compactionStrategyOptions)
                .reloadSecondaryIndexMetadata(parent)
                .rebuild();
    }

    public void init()
    {
        assert baseCfs != null && columnDefs != null && columnDefs.size() == 1;

        columnDef = columnDefs.iterator().next();

        isQueryPropertiesField = Boolean.parseBoolean(columnDef.getIndexOptions().get(QUERY_PROPERTIES_FIELD));

        if (!isQueryPropertiesField) {
            CFMetaData indexedCfMetadata = buildIndexMetadata(baseCfs.metadata, columnDef);
            indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                    indexedCfMetadata.cfName, new LocalPartitioner(keyComparator),
                    indexedCfMetadata);
        }
    }

    protected ByteBuffer makeIndexColumnName(ByteBuffer rowKey, Column column) {
        ColumnNameBuilder builder = makeIndexColumnNameBuilder(column.value(), column.name());
        return builder.build();
    }

    protected ColumnNameBuilder makeIndexColumnNameBuilder(ByteBuffer column_value, ByteBuffer column_name) {
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        CompositeType indexComparator = (CompositeType)indexCfs.getComparator();

        ByteBuffer[] components = baseComparator.split(column_name);

        CompositeType.Builder builder = indexComparator.builder();
        builder.add(column_value);

        for (int i = 0; i < Math.min(columnDef.componentIndex, components.length); i++)
            builder.add(components[i]);
        return builder;
    }

    public void delete(ByteBuffer rowKey, Column column)
    {
        if (isQueryPropertiesField)
            throw new UnsupportedOperationException();

        if (column.isMarkedForDelete(System.currentTimeMillis()))
            return;

        DecoratedKey valueKey = new DecoratedKey(indexCfs.partitioner.getToken(rowKey), rowKey);
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        ColumnFamily cfi = ArrayBackedSortedColumns.factory.create(indexCfs.metadata);
        ByteBuffer name = makeIndexColumnName(rowKey, column);
        assert name.remaining() > 0 && name.remaining() <= Column.MAX_NAME_LENGTH : name.remaining();
        cfi.addTombstone(name, localDeletionTime, column.timestamp());
        indexCfs.apply(valueKey, cfi, SecondaryIndexManager.nullUpdater);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", valueKey, cfi);
    }

    public void insert(ByteBuffer rowKey, Column column)
    {
        if (isQueryPropertiesField)
            return;

        DecoratedKey valueKey = new DecoratedKey(indexCfs.partitioner.getToken(rowKey), rowKey);
        ColumnFamily cfi = ArrayBackedSortedColumns.factory.create(indexCfs.metadata);
        ByteBuffer name = makeIndexColumnName(rowKey, column);
        assert name.remaining() > 0 && name.remaining() <= Column.MAX_NAME_LENGTH : name.remaining();
        if (column instanceof ExpiringColumn)
        {
            ExpiringColumn ec = (ExpiringColumn)column;
            cfi.addColumn(new ExpiringColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER, ec.timestamp(), ec.getTimeToLive(), ec.getLocalDeletionTime()));
        }
        else
        {
            cfi.addColumn(new Column(name, ByteBufferUtil.EMPTY_BYTE_BUFFER, column.timestamp()));
        }
        if (logger.isDebugEnabled())
            logger.debug("applying index row {} in {}", indexCfs.metadata.getKeyValidator().getString(valueKey.key), cfi);

        indexCfs.apply(valueKey, cfi, SecondaryIndexManager.nullUpdater);
    }

    public void update(ByteBuffer rowKey, Column col)
    {
        if (isQueryPropertiesField)
            return;
        insert(rowKey, col);
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
        indexCfs.forceBlockingFlush();
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

    public long getLiveSize()
    {
        if (isQueryPropertiesField)
            return 0;
        return indexCfs.getMemtableDataSize();
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
    public boolean indexes(ByteBuffer name)
    {
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(name);
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return components.length > columnDef.componentIndex
                && comp.compare(components[columnDef.componentIndex], columnDef.name) == 0;
    }

    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        if (isQueryPropertiesField)
            throw new UnsupportedOperationException();

        ByteBuffer bb = entry.originalColumnNameBuilder.copy().add(columnDef.name).build();
        Column liveColumn = data.getColumn(bb);
        if (liveColumn == null || liveColumn.isMarkedForDelete(now))
            return true;

        ByteBuffer liveValue = liveColumn.value();
        return columnDef.getValidator().compare(entry.indexValue, liveValue) != 0;
    }

    public IndexedEntry decodeEntry(DecoratedKey partition_key, Column indexEntry)
    {
        if (isQueryPropertiesField)
            throw new UnsupportedOperationException();

        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        CompositeType indexComparator = (CompositeType)indexCfs.getComparator();
        ByteBuffer[] components = indexComparator.split(indexEntry.name());

        CompositeType.Builder builder = baseComparator.builder();
        for (int i = 0; i < columnDef.componentIndex; i++)
            builder.add(components[i + 1]);
        return new IndexedEntry(builder, components[0]);
    }

    public void deleteIndexColumn(DecoratedKey partition_key, Column indexColumn)
    {
        if (isQueryPropertiesField)
            throw new UnsupportedOperationException();

        logger.error("!!!!!!!!!!!!!!!!!! deleteIndexColumn called");
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        ColumnFamily cfi = ArrayBackedSortedColumns.factory.create(indexCfs.metadata);
        cfi.addTombstone(indexColumn.name(), localDeletionTime, indexColumn.timestamp());
        indexCfs.apply(partition_key, cfi, SecondaryIndexManager.nullUpdater);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}", cfi);

    }

    public static class IndexedEntry {
        public final ByteBuffer indexValue;
        public final ColumnNameBuilder originalColumnNameBuilder;

        public IndexedEntry(ColumnNameBuilder originalColumnNameBuilder, ByteBuffer indexValue)
        {
            this.indexValue = indexValue;
            this.originalColumnNameBuilder = originalColumnNameBuilder;
        }

        public ByteBuffer originalColumnNameStart()
        {
            return originalColumnNameBuilder.build();
        }

        public ByteBuffer originalColumnNameEnd()
        {
            return originalColumnNameBuilder.buildAsEndOfRange();
        }
    }

    public static class QueryOptions {
        public static final String REVERSED = "reversed";

        public static Map<String, String> parse(String query_parameters) {
            Map<String, String> res = new HashMap<String, String>();
            query_parameters = query_parameters.trim();
            if (query_parameters.isEmpty()) {
                return res;
            }
            String params[] = query_parameters.split(";");
            for (String param : params) {
                param = param.trim();
                String keyValue[] = param.split(":");
                assert keyValue.length == 2;

                res.put(keyValue[0].toLowerCase(), keyValue[1]);
            }

            return res;
        }
    }
}
