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

import io.netty.channel.Channel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.IllegalFormatException;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 内部主要包含一个同步远程调用(invokeSync)的功能
 */
public class RemotingHelper {
    public static final String ROCKETMQ_REMOTING = "RocketmqRemoting";
    public static final String DEFAULT_CHARSET = "UTF-8";

    private static final InternalLogger log = InternalLoggerFactory.getLogger(ROCKETMQ_REMOTING);

    /**
     * 简单打印异常栈信息
     */
    public static String exceptionSimpleDesc(final Throwable e) {
        StringBuffer sb = new StringBuffer();
        if (e != null) {
            sb.append(e.toString());

            StackTraceElement[] stackTrace = e.getStackTrace();
            if (stackTrace != null && stackTrace.length > 0) {
                StackTraceElement elment = stackTrace[0];
                sb.append(", ");
                sb.append(elment.toString());
            }
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        String test = exceptionSimpleDesc(new IllegalArgumentException(new RuntimeException(new RuntimeException("test"))));
        System.out.println(test);

        SocketAddress socketAddress = string2SocketAddress("http://171.167.12.12:8080");
        System.out.println(socketAddress.toString());
    }


    /**
     * 将类似 http://171.167.12.12:8080 -> 转换成InetSocketAddress对象
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        // 从最后一个:开始，作为ip地址和端口的分割
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        InetSocketAddress isa = new InetSocketAddress(host, Integer.parseInt(port));
        return isa;
    }

    /**
     * 同步执行，首先将request进行编码，并通过NIO方式建立远程连接，拿到远程数据之后进行解码
     * @param addr 请求地址
     * @param request 远程请求指令
     * @param timeoutMillis 过期时间
     */
    public static RemotingCommand invokeSync(
            final String addr,
            final RemotingCommand request,
            final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginTime = System.currentTimeMillis();
        // 将连接转换成对象
        SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
        // 同步连接到远程
        SocketChannel socketChannel = RemotingUtil.connect(socketAddress);
        if (socketChannel != null) {
            boolean sendRequestOK = false;

            try {

                socketChannel.configureBlocking(true);

                //bugfix  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4614802
                socketChannel.socket().setSoTimeout((int) timeoutMillis);

                // 对请求信息进行编码
                ByteBuffer byteBufferRequest = request.encode();

                // 不断向通道写入请求数据
                while (byteBufferRequest.hasRemaining()) {
                    int length = socketChannel.write(byteBufferRequest);
                    if (length > 0) {
                        if (byteBufferRequest.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {
                                // 超时控制
                                throw new RemotingSendRequestException(addr);
                            }
                        }
                    } else {
                        // 通道写不进去数据了
                        throw new RemotingSendRequestException(addr);
                    }

                    Thread.sleep(1);
                }

                // 为什么对方在收到消息之后，知道最后一个字节代表接收数据结束了？
                sendRequestOK = true;

                // 开辟字节缓冲区 首先接收的是消息体的总长度数据，具体可见request.encode(); 前4个字节代表的是消息体总长度
                ByteBuffer byteBufferSize = ByteBuffer.allocate(4);
                while (byteBufferSize.hasRemaining()) {
                    // 将通道中读取到的数据写入到缓存区中
                    int length = socketChannel.read(byteBufferSize);
                    if (length > 0) {
                        if (byteBufferSize.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {
                                // 超时控制
                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    } else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    Thread.sleep(1);
                }

                // 计算出消息体的总长度
                int size = byteBufferSize.getInt(0);
                // 开辟指定长度的缓冲区
                ByteBuffer byteBufferBody = ByteBuffer.allocate(size);
                while (byteBufferBody.hasRemaining()) {
                    // 将数据不断读入到缓冲区中
                    int length = socketChannel.read(byteBufferBody);
                    if (length > 0) {
                        if (byteBufferBody.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {
                                // 超时控制
                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    } else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    Thread.sleep(1);
                }
                // 恢复读写指针
                byteBufferBody.flip();
                // 数据解码之后返回
                return RemotingCommand.decode(byteBufferBody);
            } catch (IOException e) {
                log.error("invokeSync failure", e);

                if (sendRequestOK) {
                    throw new RemotingTimeoutException(addr, timeoutMillis);
                } else {
                    throw new RemotingSendRequestException(addr);
                }
            } finally {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 解析通道远程连接地址
     */
    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }
        // 获取远程连接地址
        SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            // 返回地址
            return addr;
        }

        return "";
    }

    /**
     * 解析SocketAddress中的地址
     */
    public static String parseSocketAddressAddr(SocketAddress socketAddress) {
        if (socketAddress != null) {
            final String addr = socketAddress.toString();

            if (addr.length() > 0) {
                return addr.substring(1);
            }
        }
        return "";
    }

}
