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

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);

    private static final Request REQUEST_OF_DEATH = Request.requestOfDeath;

    /** The number of log entries to log before starting a snapshot */
    private static int snapCount = ZooKeeperServer.getSnapCount();

    /**
     * The total size of log entries before starting a snapshot
     */
    private static long snapSizeInBytes = ZooKeeperServer.getSnapSizeInBytes();

    /**
     * Random numbers used to vary snapshot timing
     */
    private int randRoll;
    private long randSize;

    private final BlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    private final Semaphore snapThreadMutex = new Semaphore(1);

    private final ZooKeeperServer zks;

    private final RequestProcessor nextProcessor;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    private final Queue<Request> toFlush;
    private long lastFlushTime;

    public SyncRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        this.toFlush = new ArrayDeque<>(zks.getMaxBatchSize());
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    private long getRemainingDelay() {
        long flushDelay = zks.getFlushDelay();
        long duration = Time.currentElapsedTime() - lastFlushTime;
        if (duration < flushDelay) {
            return flushDelay - duration;
        }
        return 0;
    }

    /** If both flushDelay and maxMaxBatchSize are set (bigger than 0), flush
     * whenever either condition is hit. If only one or the other is
     * set, flush only when the relevant condition is hit.
     */
    private boolean shouldFlush() {
        long flushDelay = zks.getFlushDelay();
        long maxBatchSize = zks.getMaxBatchSize();

        //每隔一段时间进行flush flush的时候会更新上一次flush的时间
        if ((flushDelay > 0) && (getRemainingDelay() == 0)) {
            return true;
        }

        // 积累maxBatchSize个请求就flush
        return (maxBatchSize > 0) && (toFlush.size() >= maxBatchSize);
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param size
     */
    public static void setSnapSizeInBytes(long size) {
        snapSizeInBytes = size;
    }

    private boolean shouldSnapshot() {
        //txnCount:当前这个日志文件flush的写请求数量
        //在调用zks.getZKDatabase().append(si)方法时会进行自增
        int logCount = zks.getZKDatabase().getTxnCount();  // log1.output txncount
        //logSize:整个zkServer的日志数量
        long logSize = zks.getZKDatabase().getTxnSize();  // log
        return (logCount > (snapCount / 2 + randRoll))
               || (snapSizeInBytes > 0 && logSize > (snapSizeInBytes / 2 + randSize));
    }

    private void resetSnapshotStats() {
        randRoll = ThreadLocalRandom.current().nextInt(snapCount / 2);
        randSize = Math.abs(ThreadLocalRandom.current().nextLong() % (snapSizeInBytes / 2));
    }

    /**
     * 1、（尽量）批量持久化request ==> 每个log日志文件大小不固定 == 每次打快照之后会生成一个新的log日志文件
     *     == 控制生成新的日志文件的方式是在zks.getZKDatabase().rollLog()（打快照 ）中将LogStream赋值为null
     *     ==每次zks.getZKDatabase().append(si)的时候会判断logStream是否为空
     *     == append操作只是将数据写到缓冲区了 只有调用了flush才真正完成刷盘（持久化）
     *
     * 2、持久化DataTree（打快照）  == 创建线程去打快照
     */
    @Override
    public void run() {
        try {
            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            resetSnapshotStats();
            lastFlushTime = Time.currentElapsedTime();

            // 不断的从queuedRequests获取Request进行持久化
            // 先尝试把Request对象添加到

            while (true) {
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_SIZE.add(queuedRequests.size());

                long pollTime = Math.min(zks.getMaxWriteQueuePollTime(), getRemainingDelay());

                // prepProcessor会向queuedRequests队列中添加任务
                // 移除并返回队列头部的元素，如果队列为空，则等待polltime时间 等待polltime后如果队列还为空 返回null
                // 批量持久请求
                Request si = queuedRequests.poll(pollTime, TimeUnit.MILLISECONDS);

                if (si == null) {
                    // 如果从queuedRequests队列中没有获取到请求了，那么就把接收到的请求flush
                    /* We timed out looking for more writes to batch, go ahead and flush immediately */
                    flush(); //这里考虑的请求量较小的情况
                    // 移除并返回队列头部的元素，如果队列为空，则阻塞
                    si = queuedRequests.take();
                }

                if (si == REQUEST_OF_DEATH) {
                    break;
                }

                long startProcessTime = Time.currentElapsedTime();
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_TIME.add(startProcessTime - si.syncQueueStartTime);

                // track the number of records written to the log
                // si表示Request，此时的Request中已经有日志头和日志体中了，可以持久化了
                // append只是把request添加到stream中
                // 只有当Request中的hdr为null时才返回false   读请求
                //如果是读请求 这里返回false
                //只有写请求才会apeend到logStream中
                if (zks.getZKDatabase().append(si)) {

                    // 是否应该打快照了
                    if (shouldSnapshot()) {
                        resetSnapshotStats();
                        // roll the log
                        // 把之前没有持久化的Request先持久化了
                        //this.logStream = null;  下一次append会生成新的log文件
                        zks.getZKDatabase().rollLog();  // logstrea=null
                        // take a snapshot
                        if (!snapThreadMutex.tryAcquire()) {
                            LOG.warn("Too busy to snap, skipping");
                        } else {
                            new ZooKeeperThread("Snapshot Thread") {
                                public void run() {
                                    try {
                                        // 持久  内存数据完成
                                        /**
                                         * //这里进行持久化操作会有一个问题（快照数据和request日志数据不一致）：
                                         * （对DataTree）打快照是在修改Datatree之前进行
                                         * 一个请求已经flush到request日志中了
                                         *  但还没有被finalProcessorr处理（没有同步到(修改)DataTree） 此时将内存中的DataTree持久化到磁盘
                                         *  但在下一次打快照时会
                                         *  zk会在重启的时候加载DataTree和request日志
                                         *  request日志命名规则：log. + 当前日志文件对应的第一个zxid对应的16进制
                                         *  快照日志命名规则：snapshot. + 当前日志文件对应的最后一个zxid对应的16进制
                                         *  加载逻辑：
                                         *
                                         *  优先加载zxid最大的快照文件（最新的快照文件）
                                         *  再加载request日志中zxid比最新快照文件大的记录
                                         */

                                        zks.takeSnapshot();
                                    } catch (Exception e) {
                                        LOG.warn("Unexpected exception", e);
                                    } finally {
                                        snapThreadMutex.release();
                                    }
                                }
                            }.start();
                        }
                    }
                } else if (toFlush.isEmpty()) {

                    /**  toFlush中会有读请求和写请求
                     * 如果是读请求 并且toFlush为空 那么可以直接调用finalProcessor处理读请求
                     *  如果toFlush不为空，有可能有读请求 此时就不能直接调用finalProcessor，因为DataTree中的数据不是最新的
                     *  toFlush相当于缓存
                     *
                     */

                    // toFlush是一个队列，表示需要进行持久化的Request，当Request被持久化了之后，就会把Request从toFlush中移除，直接调用nextProcessor
                    // 所以如果一个Request的hdr为null，表示不用进行持久化，
                    // 并且当toFlush队列中有值时不能先执行这个Request请求，得等toFlush中请求都执行完了之后才能执行

                    // optimization for read heavy workloads
                    // iff this is a read, and there are no pending
                    // flushes (writes), then just pass this to the next
                    // processor
                    if (nextProcessor != null) {
                        /**
                         * 集群模式下nextProcessor为AckRequestProcessor
                         * 单机模式下 nextProcessor为FinalRequestProcesor
                         */
                        nextProcessor.processRequest(si);
                        if (nextProcessor instanceof Flushable) {
                            ((Flushable) nextProcessor).flush();
                        }
                    }
                    continue;
                }

                // 把当前请求添加到待flush队列中
                toFlush.add(si);  // 写 读

                // 判断是否可以Flush了
                // 当累积了maxBatchSize个请求，或者达到某个定时点了就进行持久化
                if (shouldFlush()) {
                    //刷新到磁盘
                    flush();
                }

                ServerMetrics.getMetrics().SYNC_PROCESS_TIME.add(Time.currentElapsedTime() - startProcessTime);
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush() throws IOException, RequestProcessorException {
        if (this.toFlush.isEmpty()) {
            return;
        }


        ServerMetrics.getMetrics().BATCH_SIZE.add(toFlush.size());

        long flushStartTime = Time.currentElapsedTime();
        // flush logStream，真正进行持久化
        zks.getZKDatabase().commit();

        ServerMetrics.getMetrics().SYNC_PROCESSOR_FLUSH_TIME.add(Time.currentElapsedTime() - flushStartTime);

        if (this.nextProcessor == null) {
            this.toFlush.clear();
        } else {
            // 把toFlush队列中的Reqeust获取出来，并调用nnextProcessor
            while (!this.toFlush.isEmpty()) {    // log.1  output
                // 把toFlush中的请求交给nextProcessor
                final Request i = this.toFlush.remove();
                long latency = Time.currentElapsedTime() - i.syncQueueStartTime;
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_AND_FLUSH_TIME.add(latency);
                //调用下一个processor处理请求
                this.nextProcessor.processRequest(i);
            }
            if (this.nextProcessor instanceof Flushable) {
                ((Flushable) this.nextProcessor).flush();
            }
            //更新上次flush的时间
            lastFlushTime = Time.currentElapsedTime();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(REQUEST_OF_DEATH);
        try {
            this.join();
            this.flush();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for {} to finish", this);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(final Request request) {
        Objects.requireNonNull(request, "Request cannot be null");

        request.syncQueueStartTime = Time.currentElapsedTime();
        queuedRequests.add(request);
        ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUED.add(1);
    }

}
