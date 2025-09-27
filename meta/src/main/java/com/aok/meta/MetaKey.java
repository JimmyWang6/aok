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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public class MetaKey implements Serializable {

    private final String type;

    private final String vhost;

    private final String name;

    public MetaKey(@JsonProperty("type") String type,
                   @JsonProperty("vhost") String vhost,
                   @JsonProperty("name") String name) {
        this.type = type;
        this.vhost = vhost;
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public String getVhost() {
        return vhost;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetaKey)) return false;
        MetaKey metaKey = (MetaKey) o;
        return Objects.equals(type, metaKey.type) &&
               Objects.equals(vhost, metaKey.vhost) &&
               Objects.equals(name, metaKey.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, vhost, name);
    }
}