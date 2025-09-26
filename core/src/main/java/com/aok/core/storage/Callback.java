package com.aok.core.storage;

import com.aok.core.storage.message.Message;

public interface Callback {
    void onCompletion(Message message, Exception e);
}