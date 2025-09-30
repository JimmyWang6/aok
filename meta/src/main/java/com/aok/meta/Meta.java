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
package com.aok.meta;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Data;

import java.io.Serializable;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({@JsonSubTypes.Type(value = Exchange.class, name = "exchange"),
        @JsonSubTypes.Type(value = Queue.class, name = "queue"),
        @JsonSubTypes.Type(value = Binding.class, name = "binding"),
        @JsonSubTypes.Type(value = Vhost.class, name = "vhost"),
        @JsonSubTypes.Type(value = Dummy.class, name = "dummy")})
public abstract class Meta implements Serializable {

    private long offset;

    private String vhost;

    private String name;

    public String getMetaType() {
        JsonTypeName type = getClass().getAnnotation(JsonTypeName.class);
        if (type == null) {
            throw new IllegalArgumentException("Meta class must be annotated with @MetaType");
        }
        return type.value();
    }
}
