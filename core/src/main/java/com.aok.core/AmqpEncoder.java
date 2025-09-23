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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;

import java.nio.ByteBuffer;

@Slf4j
public class AmqpEncoder extends MessageToByteEncoder<AMQDataBlock> {

    @Override
    public void encode(ChannelHandlerContext ctx, AMQDataBlock frame, ByteBuf out) {
        try {
            SimpleEncodeBufferSender sender = new SimpleEncodeBufferSender((int) frame.getSize());
            frame.writePayload(sender);
            try (QpidByteBuffer buffer = sender.getBuffer().flip()) {
                out.writeBytes(buffer.array(), buffer.position(), buffer.remaining());
            }
        } catch (Exception e) {
            log.error("channel {} encode exception {}", ctx.channel().toString(), e);
            ctx.close();
        }
    }
}