package com.aok.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ConfigLoader {

    public static Properties load(String filePath) throws IOException {
        Properties properties = new Properties();
        if (filePath != null) {
            try (InputStream input = new FileInputStream(filePath)) {
                properties.load(input);
            }
        } else {
            System.out.println("Did not load any properties since the property file is not specified");
        }
        return properties;
    }
}
