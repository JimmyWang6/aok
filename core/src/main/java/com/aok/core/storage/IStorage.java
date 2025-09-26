package com.aok.core.storage;

import com.aok.core.storage.message.Message;

public interface IStorage {

    public void produce(Message message);
}
