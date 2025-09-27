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

import java.util.List;

/**
 * Container interface for managing Meta objects.
 * @param <Meta> the type of Meta
 */
public interface MetaContainer<Meta> {

    /**
     * Adds a Meta object to the container.
     * @param meta the Meta object to add
     */
    Meta add(Meta meta);

    /**
     * Removes a Meta object from the container.
     * @param meta the Meta object to remove
     */
    Meta delete(Meta meta);

    /**
     * Retrieves a Meta object by type, vhost, and name.
     * @param classType the type of Meta
     * @param vhost the virtual host
     * @param name the name of Meta
     * @return the Meta object if found, otherwise null
     */
    Meta get(Class<?> classType, String vhost, String name);
    
    /**
     * Lists all Meta objects in the container.
     * @return a list of Meta objects
     */
    List<Meta> list(Class<?> classType);
    
    /**
     * Gets the count of Meta objects of a specific type in the container.
     * @param classType the class type of Meta
     * @return the count of Meta objects
     */
    int size(Class<?> classType);

    /**
     * Updates a Meta object in the container.
     * @param meta the Meta object to update
     */
    void update(Meta meta);
}