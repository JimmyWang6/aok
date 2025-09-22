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

public class AmqpChannel implements ServerChannelMethodProcessor {
    
    protected AmqpConnection connection;
    
    protected int channelId;
    
    AmqpChannel(AmqpConnection connection, int channelId) {
        this.connection = connection;
        this.channelId = channelId;
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active, boolean write, boolean read) {
        AccessRequestOkBody response = connection.getRegistry().createAccessRequestOkBody(0);
        connection.writeFrame(response.generateFrame(1));
    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable, boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        connection.writeFrame(connection.getRegistry().createExchangeDeclareOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
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
        QueueDeclareOkBody responseBody = connection.getRegistry().createQueueDeclareOkBody(AMQShortString.createAMQShortString("name"), 1,1);
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
        System.out.println("test pr");
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