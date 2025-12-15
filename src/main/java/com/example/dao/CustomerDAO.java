package com.example.dao;

import com.example.model.Customer;
import com.example.util.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object for Customer entity with extensive SQL operations
 */
public class CustomerDAO {
    
    private static final Logger logger = LoggerFactory.getLogger(CustomerDAO.class);
    
    // Basic CRUD SQL Strings
    private static final String INSERT_CUSTOMER = "INSERT INTO customer (customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT, CURRENT, ?, ?, ?)";
    
    private static final String UPDATE_CUSTOMER = "UPDATE customer SET first_name = ?, last_name = ?, email = ?, phone = ?, address = ?, city = ?, state = ?, zip_code = ?, country = ?, modified_date = CURRENT, status = ?, credit_limit = ?, customer_type = ? WHERE customer_id = ?";
    
    private static final String DELETE_CUSTOMER = "DELETE FROM customer WHERE customer_id = ?";
    
    private static final String SELECT_BY_ID = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE customer_id = ?";
    
    private static final String SELECT_ALL = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer";
    
    // Search queries
    private static final String SELECT_BY_EMAIL = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE email = ?";
    
    private static final String SELECT_BY_PHONE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE phone = ?";
    
    private static final String SELECT_BY_FIRST_NAME = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE first_name = ?";
    
    private static final String SELECT_BY_LAST_NAME = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE last_name = ?";
    
    private static final String SELECT_BY_FULL_NAME = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE first_name = ? AND last_name = ?";
    
    private static final String SELECT_BY_CITY = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE city = ?";
    
    private static final String SELECT_BY_STATE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE state = ?";
    
    private static final String SELECT_BY_ZIP_CODE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE zip_code = ?";
    
    private static final String SELECT_BY_COUNTRY = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE country = ?";
    
    private static final String SELECT_BY_STATUS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE status = ?";
    
    private static final String SELECT_BY_CUSTOMER_TYPE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE customer_type = ?";
    
    // LIKE queries
    private static final String SELECT_BY_FIRST_NAME_LIKE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE first_name LIKE ?";
    
    private static final String SELECT_BY_LAST_NAME_LIKE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE last_name LIKE ?";
    
    private static final String SELECT_BY_EMAIL_LIKE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE email LIKE ?";
    
    private static final String SELECT_BY_ADDRESS_LIKE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE address LIKE ?";
    
    private static final String SELECT_BY_CITY_LIKE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE city LIKE ?";
    
    // Range queries
    private static final String SELECT_BY_CREDIT_LIMIT_RANGE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE credit_limit BETWEEN ? AND ?";
    
    private static final String SELECT_BY_CREDIT_LIMIT_GT = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE credit_limit > ?";
    
    private static final String SELECT_BY_CREDIT_LIMIT_LT = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE credit_limit < ?";
    
    private static final String SELECT_BY_CREDIT_LIMIT_GTE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE credit_limit >= ?";
    
    private static final String SELECT_BY_CREDIT_LIMIT_LTE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE credit_limit <= ?";
    
    // Date range queries
    private static final String SELECT_BY_CREATED_DATE_RANGE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE created_date BETWEEN ? AND ?";
    
    private static final String SELECT_BY_CREATED_DATE_AFTER = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE created_date > ?";
    
    private static final String SELECT_BY_CREATED_DATE_BEFORE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE created_date < ?";
    
    private static final String SELECT_BY_MODIFIED_DATE_RANGE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE modified_date BETWEEN ? AND ?";
    
    private static final String SELECT_BY_MODIFIED_DATE_AFTER = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE modified_date > ?";
    
    private static final String SELECT_BY_MODIFIED_DATE_BEFORE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE modified_date < ?";
    
    // Combined search queries
    private static final String SELECT_BY_CITY_AND_STATE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE city = ? AND state = ?";
    
    private static final String SELECT_BY_STATE_AND_STATUS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE state = ? AND status = ?";
    
    private static final String SELECT_BY_COUNTRY_AND_STATUS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE country = ? AND status = ?";
    
    private static final String SELECT_BY_TYPE_AND_STATUS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE customer_type = ? AND status = ?";
    
    private static final String SELECT_BY_CITY_STATE_COUNTRY = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE city = ? AND state = ? AND country = ?";
    
    // Update specific fields
    private static final String UPDATE_FIRST_NAME = "UPDATE customer SET first_name = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_LAST_NAME = "UPDATE customer SET last_name = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_EMAIL = "UPDATE customer SET email = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_PHONE = "UPDATE customer SET phone = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_ADDRESS = "UPDATE customer SET address = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_CITY = "UPDATE customer SET city = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_STATE = "UPDATE customer SET state = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_ZIP_CODE = "UPDATE customer SET zip_code = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_COUNTRY = "UPDATE customer SET country = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_STATUS = "UPDATE customer SET status = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_CREDIT_LIMIT = "UPDATE customer SET credit_limit = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_CUSTOMER_TYPE = "UPDATE customer SET customer_type = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    // Batch update operations
    private static final String UPDATE_STATUS_BY_TYPE = "UPDATE customer SET status = ?, modified_date = CURRENT WHERE customer_type = ?";
    
    private static final String UPDATE_STATUS_BY_CITY = "UPDATE customer SET status = ?, modified_date = CURRENT WHERE city = ?";
    
    private static final String UPDATE_STATUS_BY_STATE = "UPDATE customer SET status = ?, modified_date = CURRENT WHERE state = ?";
    
    private static final String UPDATE_CREDIT_LIMIT_BY_TYPE = "UPDATE customer SET credit_limit = ?, modified_date = CURRENT WHERE customer_type = ?";
    
    private static final String UPDATE_TYPE_BY_CREDIT_LIMIT = "UPDATE customer SET customer_type = ?, modified_date = CURRENT WHERE credit_limit > ?";
    
    // Count queries
    private static final String COUNT_ALL = "SELECT COUNT(*) FROM customer";
    
    private static final String COUNT_BY_STATUS = "SELECT COUNT(*) FROM customer WHERE status = ?";
    
    private static final String COUNT_BY_TYPE = "SELECT COUNT(*) FROM customer WHERE customer_type = ?";
    
    private static final String COUNT_BY_CITY = "SELECT COUNT(*) FROM customer WHERE city = ?";
    
    private static final String COUNT_BY_STATE = "SELECT COUNT(*) FROM customer WHERE state = ?";
    
    private static final String COUNT_BY_COUNTRY = "SELECT COUNT(*) FROM customer WHERE country = ?";
    
    private static final String COUNT_BY_CITY_STATE = "SELECT COUNT(*) FROM customer WHERE city = ? AND state = ?";
    
    private static final String COUNT_BY_STATUS_TYPE = "SELECT COUNT(*) FROM customer WHERE status = ? AND customer_type = ?";
    
    private static final String COUNT_ACTIVE = "SELECT COUNT(*) FROM customer WHERE status = 'ACTIVE'";
    
    private static final String COUNT_INACTIVE = "SELECT COUNT(*) FROM customer WHERE status = 'INACTIVE'";
    
    // EXISTS queries
    private static final String EXISTS_BY_EMAIL = "SELECT COUNT(*) FROM customer WHERE email = ?";
    
    private static final String EXISTS_BY_PHONE = "SELECT COUNT(*) FROM customer WHERE phone = ?";
    
    private static final String EXISTS_BY_ID = "SELECT COUNT(*) FROM customer WHERE customer_id = ?";
    
    // Ordering queries
    private static final String SELECT_ALL_ORDER_BY_LAST_NAME = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY last_name";
    
    private static final String SELECT_ALL_ORDER_BY_FIRST_NAME = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY first_name";
    
    private static final String SELECT_ALL_ORDER_BY_EMAIL = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY email";
    
    private static final String SELECT_ALL_ORDER_BY_CREATED_DATE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY created_date";
    
    private static final String SELECT_ALL_ORDER_BY_CREATED_DATE_DESC = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY created_date DESC";
    
    private static final String SELECT_ALL_ORDER_BY_MODIFIED_DATE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY modified_date";
    
    private static final String SELECT_ALL_ORDER_BY_MODIFIED_DATE_DESC = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY modified_date DESC";
    
    private static final String SELECT_ALL_ORDER_BY_CREDIT_LIMIT = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY credit_limit";
    
    private static final String SELECT_ALL_ORDER_BY_CREDIT_LIMIT_DESC = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY credit_limit DESC";
    
    private static final String SELECT_ALL_ORDER_BY_FULL_NAME = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY last_name, first_name";
    
    // Limit queries
    private static final String SELECT_TOP_10 = "SELECT FIRST 10 customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer";
    
    private static final String SELECT_TOP_100 = "SELECT FIRST 100 customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer";
    
    private static final String SELECT_TOP_10_BY_CREDIT_LIMIT = "SELECT FIRST 10 customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY credit_limit DESC";
    
    private static final String SELECT_TOP_10_RECENT = "SELECT FIRST 10 customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY created_date DESC";
    
    private static final String SELECT_TOP_10_MODIFIED = "SELECT FIRST 10 customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer ORDER BY modified_date DESC";
    
    // Aggregation queries
    private static final String SUM_CREDIT_LIMIT = "SELECT SUM(credit_limit) FROM customer";
    
    private static final String SUM_CREDIT_LIMIT_BY_TYPE = "SELECT SUM(credit_limit) FROM customer WHERE customer_type = ?";
    
    private static final String SUM_CREDIT_LIMIT_BY_STATUS = "SELECT SUM(credit_limit) FROM customer WHERE status = ?";
    
    private static final String AVG_CREDIT_LIMIT = "SELECT AVG(credit_limit) FROM customer";
    
    private static final String AVG_CREDIT_LIMIT_BY_TYPE = "SELECT AVG(credit_limit) FROM customer WHERE customer_type = ?";
    
    private static final String AVG_CREDIT_LIMIT_BY_STATUS = "SELECT AVG(credit_limit) FROM customer WHERE status = ?";
    
    private static final String MAX_CREDIT_LIMIT = "SELECT MAX(credit_limit) FROM customer";
    
    private static final String MIN_CREDIT_LIMIT = "SELECT MIN(credit_limit) FROM customer";
    
    private static final String MAX_CREDIT_LIMIT_BY_TYPE = "SELECT MAX(credit_limit) FROM customer WHERE customer_type = ?";
    
    private static final String MIN_CREDIT_LIMIT_BY_TYPE = "SELECT MIN(credit_limit) FROM customer WHERE customer_type = ?";
    
    // Group by queries
    private static final String COUNT_BY_STATE_GROUP = "SELECT state, COUNT(*) as customer_count FROM customer GROUP BY state";
    
    private static final String COUNT_BY_CITY_GROUP = "SELECT city, COUNT(*) as customer_count FROM customer GROUP BY city";
    
    private static final String COUNT_BY_TYPE_GROUP = "SELECT customer_type, COUNT(*) as customer_count FROM customer GROUP BY customer_type";
    
    private static final String COUNT_BY_STATUS_GROUP = "SELECT status, COUNT(*) as customer_count FROM customer GROUP BY status";
    
    private static final String SUM_CREDIT_BY_STATE = "SELECT state, SUM(credit_limit) as total_credit FROM customer GROUP BY state";
    
    private static final String SUM_CREDIT_BY_CITY = "SELECT city, SUM(credit_limit) as total_credit FROM customer GROUP BY city";
    
    private static final String SUM_CREDIT_BY_TYPE = "SELECT customer_type, SUM(credit_limit) as total_credit FROM customer GROUP BY customer_type";
    
    private static final String AVG_CREDIT_BY_STATE = "SELECT state, AVG(credit_limit) as avg_credit FROM customer GROUP BY state";
    
    private static final String AVG_CREDIT_BY_TYPE = "SELECT customer_type, AVG(credit_limit) as avg_credit FROM customer GROUP BY customer_type";
    
    // Having queries
    private static final String STATES_WITH_MIN_CUSTOMERS = "SELECT state, COUNT(*) as customer_count FROM customer GROUP BY state HAVING COUNT(*) > ?";
    
    private static final String CITIES_WITH_HIGH_AVG_CREDIT = "SELECT city, AVG(credit_limit) as avg_credit FROM customer GROUP BY city HAVING AVG(credit_limit) > ?";
    
    private static final String TYPES_WITH_HIGH_TOTAL_CREDIT = "SELECT customer_type, SUM(credit_limit) as total_credit FROM customer GROUP BY customer_type HAVING SUM(credit_limit) > ?";
    
    // IN queries
    private static final String SELECT_BY_STATES_IN = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE state IN (?, ?, ?)";
    
    private static final String SELECT_BY_CITIES_IN = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE city IN (?, ?, ?)";
    
    private static final String SELECT_BY_STATUS_IN = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE status IN (?, ?)";
    
    private static final String SELECT_BY_TYPES_IN = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE customer_type IN (?, ?, ?)";
    
    // NOT IN queries
    private static final String SELECT_NOT_IN_STATES = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE state NOT IN (?, ?)";
    
    private static final String SELECT_NOT_IN_STATUS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE status NOT IN (?)";
    
    // NULL checks
    private static final String SELECT_NULL_PHONE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE phone IS NULL";
    
    private static final String SELECT_NULL_EMAIL = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE email IS NULL";
    
    private static final String SELECT_NULL_ADDRESS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE address IS NULL";
    
    private static final String SELECT_NOT_NULL_PHONE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE phone IS NOT NULL";
    
    private static final String SELECT_NOT_NULL_EMAIL = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE email IS NOT NULL";
    
    // Distinct queries
    private static final String SELECT_DISTINCT_CITIES = "SELECT DISTINCT city FROM customer ORDER BY city";
    
    private static final String SELECT_DISTINCT_STATES = "SELECT DISTINCT state FROM customer ORDER BY state";
    
    private static final String SELECT_DISTINCT_COUNTRIES = "SELECT DISTINCT country FROM customer ORDER BY country";
    
    private static final String SELECT_DISTINCT_TYPES = "SELECT DISTINCT customer_type FROM customer ORDER BY customer_type";
    
    private static final String SELECT_DISTINCT_STATUSES = "SELECT DISTINCT status FROM customer ORDER BY status";
    
    // Complex combined queries
    private static final String SELECT_PREMIUM_CUSTOMERS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE credit_limit > 10000 AND status = 'ACTIVE' AND customer_type = 'PREMIUM'";
    
    private static final String SELECT_VIP_CUSTOMERS_BY_STATE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE customer_type = 'VIP' AND state = ? ORDER BY credit_limit DESC";
    
    private static final String SELECT_ACTIVE_IN_CITY = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE status = 'ACTIVE' AND city = ?";
    
    private static final String SELECT_HIGH_CREDIT_CUSTOMERS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE credit_limit > ? AND status = 'ACTIVE' ORDER BY credit_limit DESC";
    
    private static final String SELECT_NEW_CUSTOMERS_LAST_30_DAYS = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE created_date > CURRENT - 30 UNITS DAY ORDER BY created_date DESC";
    
    private static final String SELECT_RECENTLY_MODIFIED = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE modified_date > CURRENT - 7 UNITS DAY ORDER BY modified_date DESC";
    
    // Delete queries with conditions
    private static final String DELETE_BY_STATUS = "DELETE FROM customer WHERE status = ?";
    
    private static final String DELETE_INACTIVE_CUSTOMERS = "DELETE FROM customer WHERE status = 'INACTIVE'";
    
    private static final String DELETE_OLD_CUSTOMERS = "DELETE FROM customer WHERE created_date < ?";
    
    private static final String DELETE_BY_CITY = "DELETE FROM customer WHERE city = ?";
    
    private static final String DELETE_BY_STATE = "DELETE FROM customer WHERE state = ?";
    
    // Additional search variations
    private static final String SELECT_BY_NAME_STARTS_WITH = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE last_name LIKE ? || '%'";
    
    private static final String SELECT_BY_EMAIL_DOMAIN = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE email LIKE '%' || ? || '%'";
    
    private static final String SELECT_BY_AREA_CODE = "SELECT customer_id, first_name, last_name, email, phone, address, city, state, zip_code, country, created_date, modified_date, status, credit_limit, customer_type FROM customer WHERE phone LIKE ? || '%'";
    
    // Union queries
    private static final String SELECT_PREMIUM_OR_VIP = "SELECT customer_id, first_name, last_name, email, customer_type FROM customer WHERE customer_type = 'PREMIUM' UNION SELECT customer_id, first_name, last_name, email, customer_type FROM customer WHERE customer_type = 'VIP'";
    
    // Subquery examples
    private static final String SELECT_ABOVE_AVG_CREDIT = "SELECT customer_id, first_name, last_name, email, credit_limit FROM customer WHERE credit_limit > (SELECT AVG(credit_limit) FROM customer)";
    
    private static final String SELECT_MAX_CREDIT_CUSTOMER = "SELECT customer_id, first_name, last_name, email, credit_limit FROM customer WHERE credit_limit = (SELECT MAX(credit_limit) FROM customer)";
    
    private static final String SELECT_MIN_CREDIT_CUSTOMER = "SELECT customer_id, first_name, last_name, email, credit_limit FROM customer WHERE credit_limit = (SELECT MIN(credit_limit) FROM customer)";
    
    // Update queries with subqueries
    private static final String UPDATE_CREDIT_TO_AVG = "UPDATE customer SET credit_limit = (SELECT AVG(credit_limit) FROM customer WHERE status = 'ACTIVE'), modified_date = CURRENT WHERE customer_id = ?";
    
    // Case-based queries
    private static final String SELECT_WITH_CREDIT_CATEGORY = "SELECT customer_id, first_name, last_name, credit_limit, CASE WHEN credit_limit < 5000 THEN 'LOW' WHEN credit_limit < 15000 THEN 'MEDIUM' ELSE 'HIGH' END as credit_category FROM customer";
    
    private static final String SELECT_WITH_ACCOUNT_AGE = "SELECT customer_id, first_name, last_name, created_date, CASE WHEN created_date > CURRENT - 30 UNITS DAY THEN 'NEW' WHEN created_date > CURRENT - 365 UNITS DAY THEN 'ACTIVE' ELSE 'ESTABLISHED' END as account_status FROM customer";
    
    // More count variations
    private static final String COUNT_BY_CREDIT_RANGE = "SELECT COUNT(*) FROM customer WHERE credit_limit BETWEEN ? AND ?";
    
    private static final String COUNT_NEW_THIS_MONTH = "SELECT COUNT(*) FROM customer WHERE MONTH(created_date) = MONTH(CURRENT) AND YEAR(created_date) = YEAR(CURRENT)";
    
    private static final String COUNT_NEW_THIS_YEAR = "SELECT COUNT(*) FROM customer WHERE YEAR(created_date) = YEAR(CURRENT)";
    
    private static final String COUNT_MODIFIED_TODAY = "SELECT COUNT(*) FROM customer WHERE DATE(modified_date) = DATE(CURRENT)";
    
    // Advanced date queries
    private static final String SELECT_CREATED_THIS_MONTH = "SELECT customer_id, first_name, last_name, email, created_date FROM customer WHERE MONTH(created_date) = MONTH(CURRENT) AND YEAR(created_date) = YEAR(CURRENT)";
    
    private static final String SELECT_CREATED_THIS_YEAR = "SELECT customer_id, first_name, last_name, email, created_date FROM customer WHERE YEAR(created_date) = YEAR(CURRENT)";
    
    private static final String SELECT_CREATED_LAST_MONTH = "SELECT customer_id, first_name, last_name, email, created_date FROM customer WHERE MONTH(created_date) = MONTH(CURRENT - 1 UNITS MONTH) AND YEAR(created_date) = YEAR(CURRENT - 1 UNITS MONTH)";
    
    private static final String SELECT_MODIFIED_TODAY = "SELECT customer_id, first_name, last_name, email, modified_date FROM customer WHERE DATE(modified_date) = DATE(CURRENT)";
    
    private static final String SELECT_MODIFIED_THIS_WEEK = "SELECT customer_id, first_name, last_name, email, modified_date FROM customer WHERE modified_date > CURRENT - 7 UNITS DAY";
    
    // Pagination queries
    private static final String SELECT_PAGE_1 = "SELECT SKIP 0 FIRST 20 customer_id, first_name, last_name, email FROM customer ORDER BY customer_id";
    
    private static final String SELECT_PAGE_2 = "SELECT SKIP 20 FIRST 20 customer_id, first_name, last_name, email FROM customer ORDER BY customer_id";
    
    private static final String SELECT_PAGE_GENERIC = "SELECT SKIP ? FIRST ? customer_id, first_name, last_name, email FROM customer ORDER BY customer_id";
    
    // Additional field combinations
    private static final String SELECT_BY_FIRST_AND_EMAIL = "SELECT customer_id, first_name, last_name, email FROM customer WHERE first_name = ? AND email = ?";
    
    private static final String SELECT_BY_LAST_AND_PHONE = "SELECT customer_id, first_name, last_name, email, phone FROM customer WHERE last_name = ? AND phone = ?";
    
    private static final String SELECT_BY_CITY_AND_ZIP = "SELECT customer_id, first_name, last_name, city, zip_code FROM customer WHERE city = ? AND zip_code = ?";
    
    // String function queries
    private static final String SELECT_UPPER_NAMES = "SELECT customer_id, UPPER(first_name) as first_name, UPPER(last_name) as last_name FROM customer";
    
    private static final String SELECT_LOWER_EMAILS = "SELECT customer_id, LOWER(email) as email FROM customer";
    
    private static final String SELECT_NAME_LENGTH = "SELECT customer_id, first_name, LENGTH(first_name) as name_length FROM customer";
    
    private static final String SELECT_TRIM_NAMES = "SELECT customer_id, TRIM(first_name) as first_name, TRIM(last_name) as last_name FROM customer";
    
    // Join preparatory queries (for later use with orders)
    private static final String SELECT_CUSTOMERS_WITH_ORDERS = "SELECT DISTINCT c.customer_id, c.first_name, c.last_name, c.email FROM customer c INNER JOIN orders o ON c.customer_id = o.customer_id";
    
    private static final String SELECT_CUSTOMERS_WITHOUT_ORDERS = "SELECT c.customer_id, c.first_name, c.last_name, c.email FROM customer c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)";
    
    private static final String COUNT_CUSTOMERS_WITH_ORDERS = "SELECT COUNT(DISTINCT c.customer_id) FROM customer c INNER JOIN orders o ON c.customer_id = o.customer_id";
    
    // More update variations
    private static final String UPDATE_ALL_CREDIT_INCREASE = "UPDATE customer SET credit_limit = credit_limit * 1.1, modified_date = CURRENT WHERE status = 'ACTIVE'";
    
    private static final String UPDATE_CREDIT_BY_PERCENTAGE = "UPDATE customer SET credit_limit = credit_limit * (1 + ? / 100), modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_STATUS_BULK = "UPDATE customer SET status = ?, modified_date = CURRENT WHERE customer_id IN (?, ?, ?)";
    
    // Conditional updates
    private static final String UPGRADE_CUSTOMER_TYPE = "UPDATE customer SET customer_type = 'PREMIUM', modified_date = CURRENT WHERE credit_limit > 50000 AND status = 'ACTIVE'";
    
    private static final String DOWNGRADE_INACTIVE = "UPDATE customer SET customer_type = 'STANDARD', modified_date = CURRENT WHERE status = 'INACTIVE'";
    
    // More complex where clauses
    private static final String SELECT_MULTI_CONDITION_1 = "SELECT customer_id, first_name, last_name, email FROM customer WHERE (status = 'ACTIVE' OR status = 'PENDING') AND credit_limit > 1000";
    
    private static final String SELECT_MULTI_CONDITION_2 = "SELECT customer_id, first_name, last_name, email FROM customer WHERE status = ? AND (city = ? OR state = ?)";
    
    private static final String SELECT_MULTI_CONDITION_3 = "SELECT customer_id, first_name, last_name, email FROM customer WHERE customer_type IN ('PREMIUM', 'VIP') AND status = 'ACTIVE' AND credit_limit > ?";
    
    // Statistical queries
    private static final String SELECT_CREDIT_STATS = "SELECT COUNT(*) as total, SUM(credit_limit) as total_credit, AVG(credit_limit) as avg_credit, MAX(credit_limit) as max_credit, MIN(credit_limit) as min_credit FROM customer";
    
    private static final String SELECT_CREDIT_STATS_BY_TYPE = "SELECT customer_type, COUNT(*) as total, SUM(credit_limit) as total_credit, AVG(credit_limit) as avg_credit FROM customer GROUP BY customer_type";
    
    private static final String SELECT_CUSTOMER_GROWTH = "SELECT YEAR(created_date) as year, MONTH(created_date) as month, COUNT(*) as new_customers FROM customer GROUP BY YEAR(created_date), MONTH(created_date) ORDER BY year DESC, month DESC";
    
    // Additional partial match queries
    private static final String SELECT_BY_PHONE_PARTIAL = "SELECT customer_id, first_name, last_name, phone FROM customer WHERE phone LIKE '%' || ? || '%'";
    
    private static final String SELECT_BY_ADDRESS_PARTIAL = "SELECT customer_id, first_name, last_name, address FROM customer WHERE address LIKE '%' || ? || '%'";
    
    private static final String SELECT_BY_ZIP_PARTIAL = "SELECT customer_id, first_name, last_name, zip_code FROM customer WHERE zip_code LIKE ? || '%'";
    
    // More sorting variations
    private static final String SELECT_BY_STATE_SORT_NAME = "SELECT customer_id, first_name, last_name, state FROM customer WHERE state = ? ORDER BY last_name, first_name";
    
    private static final String SELECT_BY_CITY_SORT_CREDIT = "SELECT customer_id, first_name, last_name, city, credit_limit FROM customer WHERE city = ? ORDER BY credit_limit DESC";
    
    private static final String SELECT_ALL_SORT_MULTIPLE = "SELECT customer_id, first_name, last_name, state, city, created_date FROM customer ORDER BY state, city, created_date DESC";
    
    // Batch operations preparation
    private static final String SELECT_IDS_BY_STATUS = "SELECT customer_id FROM customer WHERE status = ?";
    
    private static final String SELECT_IDS_BY_TYPE = "SELECT customer_id FROM customer WHERE customer_type = ?";
    
    private static final String SELECT_IDS_OLD_CUSTOMERS = "SELECT customer_id FROM customer WHERE created_date < ?";
    
    // More specific field updates
    private static final String UPDATE_NAME_CASE = "UPDATE customer SET first_name = ?, last_name = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_CONTACT_INFO = "UPDATE customer SET email = ?, phone = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPDATE_LOCATION = "UPDATE customer SET address = ?, city = ?, state = ?, zip_code = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    // Existence check variations
    private static final String CHECK_DUPLICATE_EMAIL = "SELECT COUNT(*) FROM customer WHERE email = ? AND customer_id != ?";
    
    private static final String CHECK_DUPLICATE_PHONE = "SELECT COUNT(*) FROM customer WHERE phone = ? AND customer_id != ?";
    
    // More complex aggregations
    private static final String SELECT_TOP_STATES_BY_CUSTOMERS = "SELECT FIRST 10 state, COUNT(*) as customer_count FROM customer GROUP BY state ORDER BY customer_count DESC";
    
    private static final String SELECT_TOP_CITIES_BY_CREDIT = "SELECT FIRST 10 city, SUM(credit_limit) as total_credit FROM customer GROUP BY city ORDER BY total_credit DESC";
    
    private static final String SELECT_TYPE_DISTRIBUTION = "SELECT customer_type, COUNT(*) as count, ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customer), 2) as percentage FROM customer GROUP BY customer_type";
    
    // Date part queries
    private static final String SELECT_BY_MONTH = "SELECT customer_id, first_name, last_name, created_date FROM customer WHERE MONTH(created_date) = ?";
    
    private static final String SELECT_BY_YEAR = "SELECT customer_id, first_name, last_name, created_date FROM customer WHERE YEAR(created_date) = ?";
    
    private static final String SELECT_BY_YEAR_MONTH = "SELECT customer_id, first_name, last_name, created_date FROM customer WHERE YEAR(created_date) = ? AND MONTH(created_date) = ?";
    
    private static final String SELECT_BY_DAY_OF_WEEK = "SELECT customer_id, first_name, last_name, created_date FROM customer WHERE WEEKDAY(created_date) = ?";
    
    // Coalesce queries
    private static final String SELECT_WITH_DEFAULT_PHONE = "SELECT customer_id, first_name, last_name, NVL(phone, 'N/A') as phone FROM customer";
    
    private static final String SELECT_WITH_DEFAULT_EMAIL = "SELECT customer_id, first_name, last_name, NVL(email, 'no-email@example.com') as email FROM customer";
    
    // Complex business logic queries
    private static final String SELECT_CUSTOMERS_NEED_REVIEW = "SELECT customer_id, first_name, last_name, email, status, modified_date FROM customer WHERE status = 'ACTIVE' AND modified_date < CURRENT - 90 UNITS DAY";
    
    private static final String SELECT_HIGH_VALUE_INACTIVE = "SELECT customer_id, first_name, last_name, email, credit_limit, status FROM customer WHERE status = 'INACTIVE' AND credit_limit > 10000";
    
    private static final String SELECT_DORMANT_ACCOUNTS = "SELECT customer_id, first_name, last_name, email, modified_date FROM customer WHERE modified_date < CURRENT - 180 UNITS DAY AND status = 'ACTIVE'";
    
    // More delete variations
    private static final String DELETE_BY_ID_LIST = "DELETE FROM customer WHERE customer_id IN (?, ?, ?)";
    
    private static final String DELETE_OLD_INACTIVE = "DELETE FROM customer WHERE status = 'INACTIVE' AND modified_date < CURRENT - 365 UNITS DAY";
    
    private static final String DELETE_ZERO_CREDIT = "DELETE FROM customer WHERE credit_limit = 0 AND status = 'INACTIVE'";
    
    // Bulk insert preparation
    private static final String INSERT_SIMPLE = "INSERT INTO customer (customer_id, first_name, last_name, email) VALUES (?, ?, ?, ?)";
    
    // Upsert-like operations
    private static final String UPDATE_OR_INSERT_CHECK = "SELECT COUNT(*) FROM customer WHERE customer_id = ?";
    
    // Additional analytical queries
    private static final String SELECT_CUSTOMER_RETENTION = "SELECT YEAR(created_date) as cohort_year, COUNT(*) as total_customers, SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) as active_customers FROM customer GROUP BY YEAR(created_date)";
    
    private static final String SELECT_CREDIT_DISTRIBUTION = "SELECT CASE WHEN credit_limit < 5000 THEN '0-5K' WHEN credit_limit < 10000 THEN '5K-10K' WHEN credit_limit < 25000 THEN '10K-25K' ELSE '25K+' END as credit_range, COUNT(*) as customer_count FROM customer GROUP BY credit_range";
    
    // Location-based analytics
    private static final String SELECT_CUSTOMERS_PER_STATE = "SELECT state, COUNT(*) as total, SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) as active FROM customer GROUP BY state ORDER BY total DESC";
    
    private static final String SELECT_STATE_CREDIT_SUMMARY = "SELECT state, COUNT(*) as customers, SUM(credit_limit) as total_credit, AVG(credit_limit) as avg_credit, MAX(credit_limit) as max_credit FROM customer GROUP BY state ORDER BY total_credit DESC";
    
    // More timestamp queries
    private static final String SELECT_CREATED_BETWEEN_DATES = "SELECT customer_id, first_name, last_name, created_date FROM customer WHERE created_date BETWEEN ? AND ?";
    
    private static final String SELECT_MODIFIED_BETWEEN_DATES = "SELECT customer_id, first_name, last_name, modified_date FROM customer WHERE modified_date BETWEEN ? AND ?";
    
    private static final String SELECT_NOT_MODIFIED_SINCE = "SELECT customer_id, first_name, last_name, modified_date FROM customer WHERE modified_date < ?";
    
    // Customer lifecycle queries
    private static final String SELECT_CUSTOMERS_BY_AGE_DAYS = "SELECT customer_id, first_name, last_name, created_date, (CURRENT - created_date) UNITS DAY as account_age_days FROM customer ORDER BY account_age_days DESC";
    
    private static final String SELECT_NEWEST_CUSTOMERS = "SELECT FIRST 20 customer_id, first_name, last_name, email, created_date FROM customer ORDER BY created_date DESC";
    
    private static final String SELECT_OLDEST_CUSTOMERS = "SELECT FIRST 20 customer_id, first_name, last_name, email, created_date FROM customer ORDER BY created_date ASC";
    
    // Multiple field search
    private static final String SEARCH_CUSTOMERS_ALL_FIELDS = "SELECT customer_id, first_name, last_name, email, phone, address FROM customer WHERE first_name LIKE '%' || ? || '%' OR last_name LIKE '%' || ? || '%' OR email LIKE '%' || ? || '%' OR phone LIKE '%' || ? || '%'";
    
    private static final String SEARCH_BY_ANY_NAME = "SELECT customer_id, first_name, last_name, email FROM customer WHERE first_name LIKE '%' || ? || '%' OR last_name LIKE '%' || ? || '%'";
    
    // Account status transitions
    private static final String ACTIVATE_CUSTOMER = "UPDATE customer SET status = 'ACTIVE', modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String DEACTIVATE_CUSTOMER = "UPDATE customer SET status = 'INACTIVE', modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String SUSPEND_CUSTOMER = "UPDATE customer SET status = 'SUSPENDED', modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String PENDING_CUSTOMER = "UPDATE customer SET status = 'PENDING', modified_date = CURRENT WHERE customer_id = ?";
    
    // Type transitions
    private static final String UPGRADE_TO_PREMIUM = "UPDATE customer SET customer_type = 'PREMIUM', modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String UPGRADE_TO_VIP = "UPDATE customer SET customer_type = 'VIP', modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String DOWNGRADE_TO_STANDARD = "UPDATE customer SET customer_type = 'STANDARD', modified_date = CURRENT WHERE customer_id = ?";
    
    // Credit limit operations
    private static final String INCREASE_CREDIT_LIMIT = "UPDATE customer SET credit_limit = credit_limit + ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String DECREASE_CREDIT_LIMIT = "UPDATE customer SET credit_limit = credit_limit - ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String SET_CREDIT_LIMIT = "UPDATE customer SET credit_limit = ?, modified_date = CURRENT WHERE customer_id = ?";
    
    private static final String RESET_CREDIT_LIMIT = "UPDATE customer SET credit_limit = 0, modified_date = CURRENT WHERE customer_id = ?";
    
    // Verification queries
    private static final String VERIFY_EMAIL_UNIQUE = "SELECT COUNT(*) FROM customer WHERE LOWER(email) = LOWER(?)";
    
    private static final String VERIFY_PHONE_UNIQUE = "SELECT COUNT(*) FROM customer WHERE phone = ?";
    
    private static final String VERIFY_CUSTOMER_ACTIVE = "SELECT COUNT(*) FROM customer WHERE customer_id = ? AND status = 'ACTIVE'";
    
    // Export/Report queries
    private static final String SELECT_CUSTOMER_SUMMARY_REPORT = "SELECT customer_id, first_name, last_name, email, phone, city, state, status, customer_type, credit_limit, created_date FROM customer ORDER BY created_date DESC";
    
    private static final String SELECT_ACTIVE_CUSTOMERS_REPORT = "SELECT customer_id, first_name, last_name, email, phone, city, state, customer_type, credit_limit FROM customer WHERE status = 'ACTIVE' ORDER BY last_name, first_name";
    
    private static final String SELECT_CREDIT_REPORT = "SELECT customer_id, first_name, last_name, email, customer_type, credit_limit, (SELECT SUM(total_amount) FROM orders o WHERE o.customer_id = c.customer_id) as total_spent FROM customer c WHERE status = 'ACTIVE' ORDER BY credit_limit DESC";
    
    // Contact list queries
    private static final String SELECT_EMAIL_LIST = "SELECT DISTINCT email FROM customer WHERE email IS NOT NULL AND status = 'ACTIVE' ORDER BY email";
    
    private static final String SELECT_PHONE_LIST = "SELECT DISTINCT phone FROM customer WHERE phone IS NOT NULL AND status = 'ACTIVE' ORDER BY phone";
    
    private static final String SELECT_CONTACT_LIST = "SELECT customer_id, first_name, last_name, email, phone FROM customer WHERE status = 'ACTIVE' AND (email IS NOT NULL OR phone IS NOT NULL)";
    
    // Duplicate detection
    private static final String FIND_DUPLICATE_EMAILS = "SELECT email, COUNT(*) as count FROM customer WHERE email IS NOT NULL GROUP BY email HAVING COUNT(*) > 1";
    
    private static final String FIND_DUPLICATE_PHONES = "SELECT phone, COUNT(*) as count FROM customer WHERE phone IS NOT NULL GROUP BY phone HAVING COUNT(*) > 1";
    
    private static final String FIND_DUPLICATE_NAMES = "SELECT first_name, last_name, COUNT(*) as count FROM customer GROUP BY first_name, last_name HAVING COUNT(*) > 1";
    
    // Data quality checks
    private static final String SELECT_INCOMPLETE_PROFILES = "SELECT customer_id, first_name, last_name FROM customer WHERE email IS NULL OR phone IS NULL OR address IS NULL";
    
    private static final String SELECT_MISSING_CONTACT = "SELECT customer_id, first_name, last_name FROM customer WHERE email IS NULL AND phone IS NULL";
    
    private static final String SELECT_INVALID_STATUS = "SELECT customer_id, first_name, last_name, status FROM customer WHERE status NOT IN ('ACTIVE', 'INACTIVE', 'PENDING', 'SUSPENDED')";
    
    public CustomerDAO() {
    }
    
    public void insert(Customer customer) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(INSERT_CUSTOMER);
            stmt.setLong(1, customer.getCustomerId());
            stmt.setString(2, customer.getFirstName());
            stmt.setString(3, customer.getLastName());
            stmt.setString(4, customer.getEmail());
            stmt.setString(5, customer.getPhone());
            stmt.setString(6, customer.getAddress());
            stmt.setString(7, customer.getCity());
            stmt.setString(8, customer.getState());
            stmt.setString(9, customer.getZipCode());
            stmt.setString(10, customer.getCountry());
            stmt.setString(11, customer.getStatus());
            stmt.setBigDecimal(12, customer.getCreditLimit());
            stmt.setString(13, customer.getCustomerType());
            stmt.executeUpdate();
        } finally {
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    public void update(Customer customer) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(UPDATE_CUSTOMER);
            stmt.setString(1, customer.getFirstName());
            stmt.setString(2, customer.getLastName());
            stmt.setString(3, customer.getEmail());
            stmt.setString(4, customer.getPhone());
            stmt.setString(5, customer.getAddress());
            stmt.setString(6, customer.getCity());
            stmt.setString(7, customer.getState());
            stmt.setString(8, customer.getZipCode());
            stmt.setString(9, customer.getCountry());
            stmt.setString(10, customer.getStatus());
            stmt.setBigDecimal(11, customer.getCreditLimit());
            stmt.setString(12, customer.getCustomerType());
            stmt.setLong(13, customer.getCustomerId());
            stmt.executeUpdate();
        } finally {
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    public void delete(Long customerId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(DELETE_CUSTOMER);
            stmt.setLong(1, customerId);
            stmt.executeUpdate();
        } finally {
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    public Customer findById(Long customerId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(SELECT_BY_ID);
            stmt.setLong(1, customerId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return extractCustomer(rs);
            }
            return null;
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    public List<Customer> findAll() throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        List<Customer> customers = new ArrayList<>();
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(SELECT_ALL);
            while (rs.next()) {
                customers.add(extractCustomer(rs));
            }
            return customers;
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    private Customer extractCustomer(ResultSet rs) throws SQLException {
        Customer customer = new Customer();
        customer.setCustomerId(rs.getLong("customer_id"));
        customer.setFirstName(rs.getString("first_name"));
        customer.setLastName(rs.getString("last_name"));
        customer.setEmail(rs.getString("email"));
        customer.setPhone(rs.getString("phone"));
        customer.setAddress(rs.getString("address"));
        customer.setCity(rs.getString("city"));
        customer.setState(rs.getString("state"));
        customer.setZipCode(rs.getString("zip_code"));
        customer.setCountry(rs.getString("country"));
        customer.setCreatedDate(rs.getDate("created_date"));
        customer.setModifiedDate(rs.getDate("modified_date"));
        customer.setStatus(rs.getString("status"));
        customer.setCreditLimit(rs.getBigDecimal("credit_limit"));
        customer.setCustomerType(rs.getString("customer_type"));
        return customer;
    }
}
