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

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 对Semaphore的一种包装， 只允许一次release，第二次release时，由于released已经为true, 所以无法再次执行release()。
 * 本身是通过Semaphore.tryAcquire()方法获取到锁之后，将该对象封装到SemaphoreReleaseOnlyOnce, 由SemaphoreReleaseOnlyOnce
 * 执行release()操作。
 *
 * 由于信号量的数量与线程并非一一对应，也就是说，当前的信号量可以由同2个线程分别加1，而可以由一个线程连续减去2.
 * 那么在SemaphoreReleaseOnlyOnce中，通过将released属性绑定到某个线程内部和semaphore配合使用，可以放置同一个线程多次执行relse()操作
 */
public class SemaphoreReleaseOnlyOnce {
    private final AtomicBoolean released = new AtomicBoolean(false);
    private final Semaphore semaphore;

    public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public void release() {
        if (this.semaphore != null) {
            if (this.released.compareAndSet(false, true)) {
                this.semaphore.release();
            }
        }
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public static void main(String[] args) throws InterruptedException {
        Semaphore semaphore = new Semaphore(2);
        for(int i= 1 ;i <=2; i++) {
            int finalI = i;
            new Thread(() -> {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(finalI * 5000);
                    System.out.println("当前线程睡眠结束: " + Thread.currentThread().getName());
                    // 睡眠结束的那个线程连续减了两次
                    semaphore.release();
                    semaphore.release();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        Thread.sleep(200);
        for(int i= 1 ;i <=2; i++) {
            new Thread(() -> {
                try {
                    semaphore.acquire();
                    System.out.println("当前线程拿到锁: " + Thread.currentThread().getName());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        // 让前两个线程先把信号量拿走2个，当前主线程应该是不可能再acquire成功，除非前面的线程release()
        try {
            Thread.sleep(200000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    // 输出
    /**
     * 当前线程睡眠结束: Thread-0
     * 当前线程拿到锁: Thread-2
     * 当前线程拿到锁: Thread-3
     * 当前线程睡眠结束: Thread-1
     * 当第二个线程还在睡眠的时候，第一个线程由于误操作多了一次realse(),导致第2个和第3个线程直接拿到执行权限，这个时候就打破了限流的目的。
     */
}
