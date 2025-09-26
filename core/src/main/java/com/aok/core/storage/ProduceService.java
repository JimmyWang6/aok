package com.aok.core.storage;

import com.aok.core.storage.message.Message;

public class ProduceService {

    private final IStorage storage;

    ProduceService(IStorage storage) {
        this.storage = storage;
    }

    public void produce(Message message) {
        storage.produce(message);
    }
}
