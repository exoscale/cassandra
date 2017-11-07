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

package org.apache.cassandra.db.compaction;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactionStrategyManagerTest
{

    private static final String KS_PREFIX = "Keyspace1";
    private static final String TABLE_PREFIX = "CF_STANDARD";

    private static final int MAX_TEMP_DIRS = 10;
    private static File[] tempDirs = new File[MAX_TEMP_DIRS];
    private static Directories.DataDirectory[] originalDataDirs;
    private static IPartitioner originalPartitioner;

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        originalDataDirs = Directories.dataDirectories;
        originalPartitioner = StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        for (int i = 0; i < MAX_TEMP_DIRS; i++)
        {
            tempDirs[i] = Files.createTempDir();
        }
    }

    @AfterClass
    public static void afterClass() throws IOException
    {
        Directories.dataDirectories = originalDataDirs;
        DatabaseDescriptor.setPartitionerUnsafe(originalPartitioner);
        for (int i = 0; i < MAX_TEMP_DIRS; i++)
        {
            FileUtils.cleanDirectory(tempDirs[i]);
        }
        for (int i = 0; i < MAX_TEMP_DIRS; i++)
        {
            tempDirs[i].delete();
        }
    }

    @Test
    public void testSSTablesAssignedToCorrectCompactionStrategy()
    {
        for (int numDisks = 2; numDisks < 10; numDisks++)
        {
            testSSTablesAssignedToCorrectCompactionStrategy(numDisks);
        }
    }

    public void testSSTablesAssignedToCorrectCompactionStrategy(int disks)
    {
        // Update data directories to simulate JBOD
        Directories.dataDirectories = new Directories.DataDirectory[disks];
        for (int i = 0; i < disks; ++i)
            Directories.dataDirectories[i] = new Directories.DataDirectory(tempDirs[i]);

        String keyspace = KS_PREFIX + disks;
        String table = TABLE_PREFIX + disks;
        SchemaLoader.createKeyspace(keyspace,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(keyspace, table)
                                                .compaction(CompactionParams.scts(Collections.emptyMap())));
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        final Integer[] boundaries = getBoundaries(disks);
        System.out.println("Boundaries for " + disks + " disks is " + Arrays.toString(boundaries));

        CompactionStrategyManager csm = new CompactionStrategyManager(cfs, cf -> convert(boundaries), true);

        // Create 100 SSTables each with 1 key numbered from 1 to 100
        for (int i = 0; i < 100; i++)
        {
            createSSTableWithKey(keyspace, table, i);
        }
        assertEquals(100, cfs.getLiveSSTables().size());

        // Check that SSTables are assigned to the correct Compaction Strategy
        for (SSTableReader reader : cfs.getLiveSSTables())
        {
            verifySSTableIsAssignedToCorrectStrategy(cfs, boundaries, csm, reader);
        }

        for (int delta = 1; delta <= 3; delta++)
        {
            // Update disk boundaries
            Integer[] previousBoundaries = Arrays.copyOf(boundaries, boundaries.length);
            updateBoundaries(boundaries, delta);

            // Check that SSTables are still assigned to the previous boundary layout
            System.out.println("Old boundaries: " + Arrays.toString(previousBoundaries) + " New boundaries: " + Arrays.toString(boundaries));
            for (SSTableReader reader : cfs.getLiveSSTables())
            {
                verifySSTableIsAssignedToCorrectStrategy(cfs, previousBoundaries, csm, reader);
            }

            // Reload CompactionStrategyManager so new disk boundaries will be loaded
            csm.maybeReload(cfs.metadata);

            for (SSTableReader reader : cfs.getLiveSSTables())
            {
                // Check that SSTables are assigned to the new boundary layout
                verifySSTableIsAssignedToCorrectStrategy(cfs, boundaries, csm, reader);

                // Remove SSTable and check that it will be removed from the correct compaction strategy
                csm.handleNotification(new SSTableDeletingNotification(reader), this);
                assertFalse(((SizeTieredCompactionStrategy)csm.getCompactionStrategyFor(reader)).sstables.contains(reader));

                // Add SSTable again and check that is correctly assigned
                csm.handleNotification(new SSTableAddedNotification(Collections.singleton(reader)), this);
                verifySSTableIsAssignedToCorrectStrategy(cfs, boundaries, csm, reader);
            }
        }
    }

    /**
     * Updates the boundaries with a delta
     */
    private void updateBoundaries(Integer[] boundaries, int delta)
    {
        for (int j = 0; j < boundaries.length - 1; j++)
        {
            if ((j + delta) % 2 == 0)
                boundaries[j] -= delta;
            else
                boundaries[j] += delta;
        }
    }

    private void verifySSTableIsAssignedToCorrectStrategy(ColumnFamilyStore cfs, Integer[] boundaries, CompactionStrategyManager csm, SSTableReader reader)
    {
        // Check that sstable is assigned to correct disk
        int index = getSSTableIndex(boundaries, reader);
        assertEquals(index, csm.getCompactionStrategyIndex(reader));
        // Check that compaction strategy contains SSTable
        assertTrue(((SizeTieredCompactionStrategy)csm.getCompactionStrategyFor(reader)).sstables.contains(reader));
    }

    private Integer[] getBoundaries(int disks)
    {
        Integer[] result = new Integer[disks];
        int keysPerRange = 100 / disks;
        result[0] = keysPerRange;
        for (int i = 1; i < disks; i++)
        {
            result[i] = result[i - 1] + keysPerRange;
        }
        result[disks - 1] = 100; // make last boundary alwyays be 100 to prevent rounding errors
        return result;
    }

    private int getSSTableIndex(Integer[] boundaries, SSTableReader reader)
    {
        int index = 0;
        while (boundaries[index] < reader.descriptor.generation)
            index++;
        System.out.println("Index for SSTable " + reader.descriptor.generation + " on boundary " + Arrays.toString(boundaries) + " is " + index);
        return index;
    }

    private List<PartitionPosition> convert(Integer[] boundaries)
    {
        return Arrays.stream(boundaries).map(b -> Util.token(String.format(String.format("%04d", b))).minKeyBound()).collect(Collectors.toList());
    }

    private static void createSSTableWithKey(String keyspace, String table, int key)
    {
        long timestamp = System.currentTimeMillis();
        DecoratedKey dk = Util.dk(String.format("%04d", key));
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        new RowUpdateBuilder(cfs.metadata, timestamp, dk.getKey())
        .clustering(Integer.toString(key))
        .add("val", "val")
        .build()
        .applyUnsafe();
        cfs.forceBlockingFlush();
    }

}
