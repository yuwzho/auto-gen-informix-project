package com.example.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Database configuration class for Informix connection settings
 */
public class DatabaseConfig {
    
    private static final Properties properties = new Properties();
    
    static {
        try (InputStream input = DatabaseConfig.class.getClassLoader()
                .getResourceAsStream("database.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static String getUrl() {
        return properties.getProperty("db.url", "jdbc:informix-sqli://localhost:9088/testdb:INFORMIXSERVER=informix");
    }
    
    public static String getUsername() {
        return properties.getProperty("db.username", "informix");
    }
    
    public static String getPassword() {
        return properties.getProperty("db.password", "informix");
    }
    
    public static String getDriver() {
        return properties.getProperty("db.driver", "com.informix.jdbc.IfxDriver");
    }
}
