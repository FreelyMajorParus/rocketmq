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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class RemotingUtil {
    public static final String OS_NAME = System.getProperty("os.name");

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private static boolean isLinuxPlatform = false; // 是否是Linux平台
    private static boolean isWindowsPlatform = false; // 是否是Windows平台

    static {
        if (OS_NAME != null && OS_NAME.toLowerCase().contains("linux")) {
            isLinuxPlatform = true;
        }

        if (OS_NAME != null && OS_NAME.toLowerCase().contains("windows")) {
            isWindowsPlatform = true;
        }
    }

    public static boolean isWindowsPlatform() {
        return isWindowsPlatform;
    }

    public static Selector openSelector() throws IOException {
        Selector result = null;

        if (isLinuxPlatform()) { // 如果是linux平台，尝试使用Epoll模型开启Selector
            try {
                final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if (providerClazz != null) {
                    try {
                        final Method method = providerClazz.getMethod("provider");
                        if (method != null) {
                            final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                            if (selectorProvider != null) {
                                result = selectorProvider.openSelector();
                            }
                        }
                    } catch (final Exception e) {
                        log.warn("Open ePoll Selector for linux platform exception", e);
                    }
                }
            } catch (final Exception e) {
                // ignore
            }
        }

        if (result == null) {
            // 如果无法使用Epoll模型，则只好使用JDK底层自动选择Epoll或者Poll模型
            result = Selector.open();
        }

        return result;
    }

    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }

    /**
     * 获取本机地址
     * @return 优先获取ipv4地址，如果ipv4获取不到，则获取ipv6地址
     */
    public static String getLocalAddress() {
        try {
            // Traversal Network interface to get the first non-loopback and non-private address
            // 首先获取当前机器内部所有的网络接口
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            ArrayList<String> ipv4Result = new ArrayList<String>();
            ArrayList<String> ipv6Result = new ArrayList<String>();
            while (enumeration.hasMoreElements()) { // 遍历机器内部的所有网络接口
                final NetworkInterface networkInterface = enumeration.nextElement();
                final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
                while (en.hasMoreElements()) {
                    final InetAddress address = en.nextElement();
                    if (!address.isLoopbackAddress()) { // 非回环地址
                        if (address instanceof Inet6Address) {
                            ipv6Result.add(normalizeHostAddress(address));
                        } else {
                            ipv4Result.add(normalizeHostAddress(address));
                        }
                    }
                }
            }

            // prefer ipv4
            // 优先使用ipv4地址, 如果ipv4地址不为空，则跳过127和1192地址
            if (!ipv4Result.isEmpty()) {
                for (String ip : ipv4Result) {
                    if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
                        continue;
                    }

                    return ip;
                }
                // 实在找不到，就用最后一个
                return ipv4Result.get(ipv4Result.size() - 1);
            } else if (!ipv6Result.isEmpty()) {
                // 如果ipv4地址为空，而ipv6地址不为空，则直接返回ipv6的第一个地址
                return ipv6Result.get(0);
            }
            // 如果还是找不到地址，则只好用本机地址
            //If failed to find,fall back to localhost
            final InetAddress localHost = InetAddress.getLocalHost();
            // 127.0.0.1
            return normalizeHostAddress(localHost);
        } catch (Exception e) {
            log.error("Failed to obtain local address", e);
        }

        return null;
    }

    /**
     * 获取Host地址， 如果是Ipv6,则返回的数据前后加[/]
     */
    public static String normalizeHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        } else {
            return localHost.getHostAddress();
        }
    }

    /**
     * 根据字符串转换成IP地址对象
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        InetSocketAddress isa = new InetSocketAddress(host, Integer.parseInt(port));
        return isa;
    }

    /**
     * 将IP地址对象转换成字符串ip:port
     */
    public static String socketAddress2String(final SocketAddress addr) {
        StringBuilder sb = new StringBuilder();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        sb.append(inetSocketAddress.getAddress().getHostAddress());
        sb.append(":");
        sb.append(inetSocketAddress.getPort());
        return sb.toString();
    }

    /**
     * 连接到一个远程地址
     */
    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 1000 * 5);
    }

    /**
     * 连接到远程, 阻塞连接成功之后, 设置为非阻塞模式
     */
    public static SocketChannel connect(SocketAddress remote, final int timeoutMillis) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(true);
            sc.socket().setSoLinger(false, -1);
            sc.socket().setTcpNoDelay(true);
            sc.socket().setReceiveBufferSize(1024 * 64);
            sc.socket().setSendBufferSize(1024 * 64);
            sc.socket().connect(remote, timeoutMillis);
            sc.configureBlocking(false);
            return sc;
        } catch (Exception e) {
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        return null;
    }

    /**
     * 关闭通道(Netty通道 部分)
     */
    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                        future.isSuccess());
            }
        });
    }

    public static void main(String[] args) {
        String localAddress = getLocalAddress();
    }
}
