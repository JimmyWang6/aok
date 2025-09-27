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
package com.aok.meta.container;

import com.aok.meta.Meta;
import com.aok.meta.MetaKey;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryMetaContainer implements MetaContainer<Meta> {

    private final Map<MetaKey, Meta> map = new ConcurrentHashMap<>();

    @Override
    public synchronized Meta add(Meta meta) {
        String type = meta.getMetaType();
        MetaKey key = new MetaKey(type, meta.getVhost(), meta.getName());
        return map.put(key, meta);
    }

    @Override
    public synchronized Meta delete(Meta meta) {
        String type = meta.getMetaType();
        MetaKey key = new MetaKey(type, meta.getVhost(), meta.getName());
        return map.remove(key);
    }

    public Meta get(Class<?> classType, String vhost, String name) {
        JsonTypeName type = classType.getAnnotation(JsonTypeName.class);
        if (type == null) {
            return null;
        }
        MetaKey key = new MetaKey(type.value(), vhost, name);
        return map.get(key);
    }

    @Override
    public List<Meta> list(Class<?> classType) {
        JsonTypeName type = classType.getAnnotation(JsonTypeName.class);
        if (type == null) {
            return List.of();
        }
        return map.values().stream().filter(meta -> meta.getMetaType().equals(type.value())).toList();
    }
    
    @Override
    public int size(Class<?> classType) {
        return list(classType).size();
    }

    @Override
    public synchronized void update(Meta meta) {
        String type = meta.getMetaType();
        MetaKey key = new MetaKey(type, meta.getVhost(), meta.getName());
        map.put(key, meta);
    }
}