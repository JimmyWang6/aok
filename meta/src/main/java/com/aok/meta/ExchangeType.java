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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ExchangeType {

    Direct("direct"),
    Fanout("fanout"),
    Topic("topic"),
    Headers("headers"),
    ConsistentHash("x-consistent-hash"),
    DelayedMessage("x-delayed-message");

    private String value;

    ExchangeType(String value) {
        this.value = value;
    }

    @JsonCreator
    public static ExchangeType value(String type) {
        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("Illegal Exchange Type");
        }
        type = type.toLowerCase();
        switch (type) {
            case "direct":
                return Direct;
            case "fanout":
                return Fanout;
            case "topic":
                return Topic;
            case "headers":
                return Headers;
            case "x-consistent-hash":
                return ConsistentHash;
            case "x-delayed-message":
                return DelayedMessage;
            default:
                throw new IllegalArgumentException("Unknown Exchange Type: " + type);
        }
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
