package com.aok.core.storage.message;

import lombok.Data;

@Data
public class Message {

    private String vhost;

    private String queue;
}
