package com.aok.core.storage;

public class CommonUtils {

    public static String generateKey(String vhost, String name) {
        return vhost + "_" + name;
    }
}
