/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aok.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Broker {

    public static final int DEFAULT_BIND_PORT = 5674;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }
    
    public void startUp() {
        EventLoopGroup eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyNIOBoss_"));
        EventLoopGroup eventLoopGroupSelector = new NioEventLoopGroup(3, new ThreadFactoryImpl("NettyServerNIOSelector_"));
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroupBoss, eventLoopGroupSelector)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 1024)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.SO_KEEPALIVE, false)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress("localhost", DEFAULT_BIND_PORT))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) {
                    final ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast("encoder", new AmqpEncoder());
                    pipeline.addLast("handler", new AmqpConnection());
                }
            });
            try {
                serverBootstrap.bind(DEFAULT_BIND_PORT).sync();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            log.info("Broker start up success, listening on port {}", DEFAULT_BIND_PORT);
    }
    
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        broker.startUp();
        broker.awaitShutdown();
    }
}
