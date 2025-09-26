/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.aok.core;

import com.aok.meta.Exchange;
import com.aok.meta.ExchangeType;
import com.aok.meta.Queue;
import com.aok.meta.service.BindingService;
import com.aok.meta.service.ExchangeService;
import com.aok.meta.service.QueueService;
import com.aok.meta.service.VhostService;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;

@Slf4j
public class AmqpChannel implements ServerChannelMethodProcessor {
    
    private final AmqpConnection connection;

    @Getter
    private final int channelId;

    private final VhostService vhostService;

    private final BindingService bindingService;

    private final ExchangeService exchangeService;

    private  final QueueService queueService;
    
    AmqpChannel(
        AmqpConnection connection,
        int channelId,
        VhostService vhostService,
        ExchangeService exchangeService,
        QueueService queueService,
        BindingService bindingService
    ) {
        this.connection = connection;
        this.channelId = channelId;
        this.vhostService = vhostService;
        this.exchangeService = exchangeService;
        this.queueService = queueService;
        this.bindingService = bindingService;
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active, boolean write, boolean read) {
        AccessRequestOkBody response = connection.getRegistry().createAccessRequestOkBody(0);
        connection.writeFrame(response.generateFrame(1));
    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable, boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        String exchangeName = exchange == null ? StringUtil.EMPTY_STRING : exchange.toString();
        String vhost = connection.getVhost();
        exchangeService.addExchange(vhost, exchangeName, ExchangeType.value(AMQShortString.toString(type)), autoDelete, durable, internal, FieldTable.convertToMap(arguments));
        connection.writeFrame(connection.getRegistry().createExchangeDeclareOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        String vhost = connection.getVhost();
        Exchange cur = exchangeService.getExchange(vhost, AMQShortString.toString(exchange));
        if (cur != null) {
            exchangeService.deleteExchange(cur);
            log.info("delete exchange {} success", exchange);
        }
        connection.writeFrame(connection.getRegistry().createExchangeDeleteOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {
        ExchangeBoundOkBody exchangeBoundOkBody = connection.getRegistry()
            .createExchangeBoundOkBody(ExchangeBoundOkBody.OK, AMQShortString.validValueOf(""));
        connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive, boolean autoDelete, boolean nowait, FieldTable arguments) {
        Queue q = queueService.addQueue(
            connection.getVhost(),
            AMQShortString.toString(queue),
            exclusive,
            autoDelete,
            durable,
            FieldTable.convertToMap(arguments)
        );
        log.info("queue declare success: {}", q);
        QueueDeclareOkBody responseBody = connection.getRegistry().createQueueDeclareOkBody(AMQShortString.createAMQShortString(q.getName()), 0,0);
        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey, boolean nowait, FieldTable arguments) {
        connection.writeFrame(connection.getRegistry().createQueueBindOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {

    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        connection.writeFrame(connection.getRegistry().createQueueDeleteOkBody(0).generateFrame(channelId));
    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey, FieldTable arguments) {
        final QueueUnbindOkBody responseBody = connection.getRegistry().createQueueUnbindOkBody();
        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {
        connection.writeFrame(connection.getRegistry().createBasicRecoverSyncOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        connection.writeFrame(connection.getRegistry().createBasicQosOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag, boolean noLocal, boolean noAck, boolean exclusive, boolean nowait, FieldTable arguments) {
        final AMQMethodBody responseBody = connection.getRegistry().createBasicConsumeOkBody(
            AMQShortString.createAMQShortString("ctag"));
        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        connection.writeFrame(connection.getRegistry().createBasicCancelOkBody(consumerTag).generateFrame(channelId));
    }

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory, boolean immediate) {

    }

    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {
        connection.writeFrame(connection.getRegistry().createBasicGetEmptyBody(null).generateFrame(channelId));
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        connection.writeFrame(connection.getRegistry().createChannelFlowOkBody(true).generateFrame(channelId));
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {

    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {

    }

    @Override
    public void receiveChannelCloseOk() {

    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {

    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {

    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {

    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {

    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {

    }

    @Override
    public void receiveTxSelect() {

    }

    @Override
    public void receiveTxCommit() {

    }

    @Override
    public void receiveTxRollback() {

    }

    @Override
    public void receiveConfirmSelect(boolean nowait) {

    }
}