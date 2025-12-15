package com.example.util;

import com.example.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Manages database connections to Informix
 */
public class ConnectionManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
    
    static {
        try {
            Class.forName(DatabaseConfig.getDriver());
        } catch (ClassNotFoundException e) {
            logger.error("Failed to load Informix JDBC driver", e);
            throw new RuntimeException("Informix JDBC driver not found", e);
        }
    }
    
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
            DatabaseConfig.getUrl(),
            DatabaseConfig.getUsername(),
            DatabaseConfig.getPassword()
        );
    }
    
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Error closing connection", e);
            }
        }
    }
}
