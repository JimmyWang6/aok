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
package com.aok.meta.service;

import com.aok.meta.Queue;
import com.aok.meta.container.MetaContainer;

import java.util.List;
import java.util.Map;

public class QueueService {
    
    private final MetaContainer<Queue> metaContainer;
    
    public QueueService(MetaContainer<Queue> metaContainer) {
        this.metaContainer = metaContainer;
    }
    
    public Queue addQueue(String vhost, String name, Boolean exclusive, Boolean autoDelete, Boolean durable, Map<String, Object> arguments) {
        Queue queue = new Queue(vhost, name, exclusive, autoDelete, durable, arguments);
        return (Queue) metaContainer.add(queue);
    }
    
    public Queue deleteQueue(Queue queue) {
        return (Queue) metaContainer.delete(queue);
    }
    
    public List<Queue> listQueue() {
        return metaContainer.list(Queue.class);
    }

    public int size() {
        return metaContainer.size(Queue.class);
    }
}
