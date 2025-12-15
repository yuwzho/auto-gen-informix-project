package com.example.dao;

import com.example.model.Supplier;
import com.example.util.ConnectionManager;

import java.sql.*;

/**
 * Data Access Object for Supplier entity
 * SQL strings 3001-3250
 */
public class SupplierDAO {
    
    // Basic CRUD - SQL strings 3001-3010
    private static final String INSERT_SUPPLIER = "INSERT INTO supplier (supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT, CURRENT)";
    private static final String UPDATE_SUPPLIER = "UPDATE supplier SET supplier_name = ?, contact_person = ?, email = ?, phone = ?, address = ?, city = ?, state = ?, zip_code = ?, country = ?, rating = ?, status = ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String DELETE_SUPPLIER = "DELETE FROM supplier WHERE supplier_id = ?";
    private static final String SELECT_BY_ID = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE supplier_id = ?";
    private static final String SELECT_ALL = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier";
    
    // Search queries - SQL strings 3011-3050
    private static final String SELECT_BY_NAME = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE supplier_name = ?";
    private static final String SELECT_BY_EMAIL = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE email = ?";
    private static final String SELECT_BY_PHONE = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE phone = ?";
    private static final String SELECT_BY_CONTACT_PERSON = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE contact_person = ?";
    private static final String SELECT_BY_CITY = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE city = ?";
    private static final String SELECT_BY_STATE = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE state = ?";
    private static final String SELECT_BY_COUNTRY = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE country = ?";
    private static final String SELECT_BY_STATUS = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE status = ?";
    private static final String SELECT_ACTIVE_SUPPLIERS = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE status = 'ACTIVE'";
    private static final String SELECT_INACTIVE_SUPPLIERS = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE status = 'INACTIVE'";
    
    // Rating queries - SQL strings 3051-3080
    private static final String SELECT_BY_RATING = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE rating = ?";
    private static final String SELECT_BY_RATING_GT = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE rating > ?";
    private static final String SELECT_BY_RATING_LT = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE rating < ?";
    private static final String SELECT_BY_RATING_RANGE = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE rating BETWEEN ? AND ?";
    private static final String SELECT_TOP_RATED = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE rating >= 4.5 ORDER BY rating DESC";
    private static final String SELECT_LOW_RATED = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE rating < 3.0 ORDER BY rating ASC";
    
    // Update operations - SQL strings 3081-3110
    private static final String UPDATE_NAME = "UPDATE supplier SET supplier_name = ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String UPDATE_CONTACT_PERSON = "UPDATE supplier SET contact_person = ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String UPDATE_EMAIL = "UPDATE supplier SET email = ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String UPDATE_PHONE = "UPDATE supplier SET phone = ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String UPDATE_ADDRESS = "UPDATE supplier SET address = ?, city = ?, state = ?, zip_code = ?, country = ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String UPDATE_STATUS = "UPDATE supplier SET status = ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String UPDATE_RATING = "UPDATE supplier SET rating = ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String ACTIVATE_SUPPLIER = "UPDATE supplier SET status = 'ACTIVE', modified_date = CURRENT WHERE supplier_id = ?";
    private static final String DEACTIVATE_SUPPLIER = "UPDATE supplier SET status = 'INACTIVE', modified_date = CURRENT WHERE supplier_id = ?";
    private static final String INCREMENT_RATING = "UPDATE supplier SET rating = rating + ?, modified_date = CURRENT WHERE supplier_id = ?";
    private static final String DECREMENT_RATING = "UPDATE supplier SET rating = rating - ?, modified_date = CURRENT WHERE supplier_id = ?";
    
    // Aggregation queries - SQL strings 3111-3140
    private static final String COUNT_ALL = "SELECT COUNT(*) FROM supplier";
    private static final String COUNT_BY_STATUS = "SELECT COUNT(*) FROM supplier WHERE status = ?";
    private static final String COUNT_ACTIVE = "SELECT COUNT(*) FROM supplier WHERE status = 'ACTIVE'";
    private static final String COUNT_INACTIVE = "SELECT COUNT(*) FROM supplier WHERE status = 'INACTIVE'";
    private static final String COUNT_BY_COUNTRY = "SELECT COUNT(*) FROM supplier WHERE country = ?";
    private static final String AVG_RATING = "SELECT AVG(rating) FROM supplier WHERE status = 'ACTIVE'";
    private static final String MAX_RATING = "SELECT MAX(rating) FROM supplier";
    private static final String MIN_RATING = "SELECT MIN(rating) FROM supplier";
    
    // Group by queries - SQL strings 3141-3170
    private static final String GROUP_BY_COUNTRY = "SELECT country, COUNT(*) as supplier_count FROM supplier GROUP BY country ORDER BY supplier_count DESC";
    private static final String GROUP_BY_STATE = "SELECT state, COUNT(*) as supplier_count FROM supplier GROUP BY state ORDER BY supplier_count DESC";
    private static final String GROUP_BY_STATUS = "SELECT status, COUNT(*) as supplier_count FROM supplier GROUP BY status";
    private static final String GROUP_BY_RATING = "SELECT FLOOR(rating) as rating_level, COUNT(*) as count FROM supplier GROUP BY rating_level ORDER BY rating_level DESC";
    private static final String AVG_RATING_BY_COUNTRY = "SELECT country, AVG(rating) as avg_rating FROM supplier GROUP BY country ORDER BY avg_rating DESC";
    
    // LIKE queries - SQL strings 3171-3200
    private static final String SELECT_BY_NAME_LIKE = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE supplier_name LIKE ?";
    private static final String SELECT_BY_CONTACT_LIKE = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE contact_person LIKE ?";
    private static final String SELECT_BY_EMAIL_LIKE = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier WHERE email LIKE ?";
    
    // Ordering queries - SQL strings 3201-3220
    private static final String SELECT_ALL_ORDER_BY_NAME = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier ORDER BY supplier_name";
    private static final String SELECT_ALL_ORDER_BY_RATING = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier ORDER BY rating DESC";
    private static final String SELECT_ALL_ORDER_BY_COUNTRY = "SELECT supplier_id, supplier_name, contact_person, email, phone, address, city, state, zip_code, country, rating, status, created_date, modified_date FROM supplier ORDER BY country, supplier_name";
    
    // Exists queries - SQL strings 3221-3240
    private static final String EXISTS_BY_NAME = "SELECT COUNT(*) FROM supplier WHERE supplier_name = ?";
    private static final String EXISTS_BY_EMAIL = "SELECT COUNT(*) FROM supplier WHERE email = ?";
    private static final String CHECK_DUPLICATE_NAME = "SELECT COUNT(*) FROM supplier WHERE supplier_name = ? AND supplier_id != ?";
    
    // Distinct queries - SQL strings 3241-3250
    private static final String SELECT_DISTINCT_COUNTRIES = "SELECT DISTINCT country FROM supplier WHERE country IS NOT NULL ORDER BY country";
    private static final String SELECT_DISTINCT_STATES = "SELECT DISTINCT state FROM supplier WHERE state IS NOT NULL ORDER BY state";
    private static final String SELECT_DISTINCT_CITIES = "SELECT DISTINCT city FROM supplier WHERE city IS NOT NULL ORDER BY city";
    
    public SupplierDAO() {}
}
