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

import com.aok.meta.Binding;
import com.aok.meta.container.MetaContainer;

import java.util.List;
import java.util.stream.Collectors;

public class BindingService {

    private final MetaContainer<Binding> metaContainer;

    public BindingService(MetaContainer<Binding> metaContainer) {
        this.metaContainer = metaContainer;
    }

    public void bind(String source, String destination, String routingKey) {
        Binding binding = new Binding(source, destination, routingKey);
        metaContainer.add(binding);
    }

    public void unbind(Binding binding) {
        metaContainer.delete(binding);
    }

    public List<Binding> listBindings() {
        return metaContainer.list(Binding.class);
    }

    public List<Binding> listBindings(String vhost, String exchange) {
        List<Binding> bindings = listBindings().stream().filter(binding -> binding.getVhost().equals(vhost)).collect(Collectors.toList());
        return bindings.stream().filter(binding -> binding.getSource().equals(exchange)).collect(Collectors.toList());
    }
}
