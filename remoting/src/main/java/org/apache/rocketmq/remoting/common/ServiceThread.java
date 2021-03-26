/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.common;


import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * Base class for background thread
 * 抽象的服务线程类，当前类内部维护了一个thread和一个状态stopped。
 * 可以重复使用该线程(主要不中断内部已经有的线程就好), 只需要不断的修改其状态，就可以实现线程的复用
 */
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    // 默认等待90秒的事件
    private static final long JOIN_TIME = 90 * 1000;
    protected final Thread thread;
    protected volatile boolean hasNotified = false;
    protected volatile boolean stopped = false;

    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }

    public abstract String getServiceName();

    /**
     * 启动线程
     */
    public void start() {
        this.thread.start();
    }

    /**
     * 关闭线程
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 关闭线程
     * @param interrupt 是否需要中断内部实际的线程，如果不中断的话，可以连续使用
     */
    public void shutdown(final boolean interrupt) {
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        synchronized (this) {
            if (!this.hasNotified) {
                // 是否已经被通知到要关闭了
                this.hasNotified = true;
                this.notify();
            }
        }

        try {
            if (interrupt) {
                // 是否需要中断内部线程
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            // 等待默认90秒的时间
            this.thread.join(this.getJointime());
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    public boolean isStopped() {
        return stopped;
    }
}
