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
package org.apache.cassandra.metrics;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics for compaction.
 */
public class CompactionMetrics implements CompactionManager.CompactionExecutorStatsCollector
{

    private static final Logger logger = LoggerFactory.getLogger(CompactionMetrics.class);

    public static final MetricNameFactory factory = new DefaultNameFactory("Compaction");

    // a synchronized identity set of running tasks to their compaction info
    private static final Set<CompactionInfo.Holder> compactions = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<CompactionInfo.Holder, Boolean>()));

    /** Estimated number of compactions remaining to perform */
    public final Gauge<Integer> pendingTasks;
    /** Number of completed compactions since server [re]start */
    public final Gauge<Long> completedTasks;
    /** Total number of compactions since server [re]start */
    public final Meter totalCompactionsCompleted;
    /** Total number of bytes compacted since server [re]start */
    public final Counter bytesCompacted;

    public CompactionMetrics(final ThreadPoolExecutor... collectors)
    {
        pendingTasks = Metrics.newGauge(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
        {
            public Integer value()
            {
                // The collector thread is likely to be blocked by compactions
                // This is a quick fix to avoid losing metrics
                ExecutorService executor = Executors.newSingleThreadExecutor();

                final Future<Integer> future = executor.submit(new Callable() {
                    @Override
                    public Integer call() throws Exception {
                        int n = 0;
                        // add estimate number of compactions need to be done
                        for (String keyspaceName : Schema.instance.getKeyspaces())
                        {
                            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                                n += cfs.getCompactionStrategy().getEstimatedRemainingTasks();
                        }
                        // add number of currently running compactions
                        return n + compactions.size();
                    }
                });

                try {
                    return future.get(20, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    future.cancel(true);
                    logger.error("Skipping PendingTasks because some cfs is busy");
                } catch (Exception othere) {
                    logger.error("Skipping PendingTasks because of an unexpected exception", othere);
                }

                executor.shutdownNow();
                return 21;
            }
        });
        completedTasks = Metrics.newGauge(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long value()
            {
                long completedTasks = 0;
                for (ThreadPoolExecutor collector : collectors)
                    completedTasks += collector.getCompletedTaskCount();
                return completedTasks;
            }
        });
        totalCompactionsCompleted = Metrics.newMeter(factory.createMetricName("TotalCompactionsCompleted"), "compaction completed", TimeUnit.SECONDS);
        bytesCompacted = Metrics.newCounter(factory.createMetricName("BytesCompacted"));
    }

    public void beginCompaction(CompactionInfo.Holder ci)
    {
        // notify
        ci.started();
        compactions.add(ci);
    }

    public void finishCompaction(CompactionInfo.Holder ci)
    {
        // notify
        ci.finished();
        compactions.remove(ci);
        bytesCompacted.inc(ci.getCompactionInfo().getTotal());
        totalCompactionsCompleted.mark();
    }

    public static List<CompactionInfo.Holder> getCompactions()
    {
        return new ArrayList<CompactionInfo.Holder>(compactions);
    }
}
