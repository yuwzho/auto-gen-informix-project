package com.example.dao;

import com.example.model.Order;
import com.example.util.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object for Order entity with extensive SQL operations
 * SQL strings 1101-1800
 */
public class OrderDAO {
    
    private static final Logger logger = LoggerFactory.getLogger(OrderDAO.class);
    
    // Basic CRUD - SQL strings 1101-1110
    private static final String INSERT_ORDER = "INSERT INTO orders (order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT, CURRENT)";
    
    private static final String UPDATE_ORDER = "UPDATE orders SET customer_id = ?, order_date = ?, shipped_date = ?, delivered_date = ?, order_status = ?, total_amount = ?, tax_amount = ?, shipping_amount = ?, shipping_address = ?, shipping_city = ?, shipping_state = ?, shipping_zip = ?, payment_method = ?, payment_status = ?, modified_date = CURRENT WHERE order_id = ?";
    
    private static final String DELETE_ORDER = "DELETE FROM orders WHERE order_id = ?";
    
    private static final String SELECT_BY_ID = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_id = ?";
    
    private static final String SELECT_ALL = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders";
    
    // Search by customer - SQL strings 1111-1130
    private static final String SELECT_BY_CUSTOMER_ID = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE customer_id = ?";
    
    private static final String SELECT_BY_CUSTOMER_AND_STATUS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE customer_id = ? AND order_status = ?";
    
    private static final String SELECT_BY_CUSTOMER_DATE_RANGE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE customer_id = ? AND order_date BETWEEN ? AND ?";
    
    private static final String COUNT_BY_CUSTOMER = "SELECT COUNT(*) FROM orders WHERE customer_id = ?";
    
    private static final String SUM_TOTAL_BY_CUSTOMER = "SELECT SUM(total_amount) FROM orders WHERE customer_id = ?";
    
    private static final String SELECT_CUSTOMER_RECENT_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE customer_id = ? ORDER BY order_date DESC";
    
    private static final String SELECT_CUSTOMER_LARGEST_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE customer_id = ? ORDER BY total_amount DESC";
    
    // Status queries - SQL strings 1131-1160
    private static final String SELECT_BY_STATUS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = ?";
    
    private static final String SELECT_PENDING_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = 'PENDING'";
    
    private static final String SELECT_PROCESSING_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = 'PROCESSING'";
    
    private static final String SELECT_SHIPPED_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = 'SHIPPED'";
    
    private static final String SELECT_DELIVERED_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = 'DELIVERED'";
    
    private static final String SELECT_CANCELLED_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = 'CANCELLED'";
    
    private static final String COUNT_BY_STATUS = "SELECT COUNT(*) FROM orders WHERE order_status = ?";
    
    private static final String COUNT_PENDING = "SELECT COUNT(*) FROM orders WHERE order_status = 'PENDING'";
    
    private static final String COUNT_PROCESSING = "SELECT COUNT(*) FROM orders WHERE order_status = 'PROCESSING'";
    
    private static final String COUNT_SHIPPED = "SELECT COUNT(*) FROM orders WHERE order_status = 'SHIPPED'";
    
    private static final String COUNT_DELIVERED = "SELECT COUNT(*) FROM orders WHERE order_status = 'DELIVERED'";
    
    private static final String COUNT_CANCELLED = "SELECT COUNT(*) FROM orders WHERE order_status = 'CANCELLED'";
    
    // Payment status queries - SQL strings 1161-1180
    private static final String SELECT_BY_PAYMENT_STATUS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_status = ?";
    
    private static final String SELECT_PAID_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_status = 'PAID'";
    
    private static final String SELECT_UNPAID_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_status = 'UNPAID'";
    
    private static final String SELECT_REFUNDED_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_status = 'REFUNDED'";
    
    private static final String COUNT_BY_PAYMENT_STATUS = "SELECT COUNT(*) FROM orders WHERE payment_status = ?";
    
    private static final String SUM_PAID_ORDERS = "SELECT SUM(total_amount) FROM orders WHERE payment_status = 'PAID'";
    
    private static final String SUM_UNPAID_ORDERS = "SELECT SUM(total_amount) FROM orders WHERE payment_status = 'UNPAID'";
    
    // Payment method queries - SQL strings 1181-1200
    private static final String SELECT_BY_PAYMENT_METHOD = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_method = ?";
    
    private static final String SELECT_CREDIT_CARD_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_method = 'CREDIT_CARD'";
    
    private static final String SELECT_DEBIT_CARD_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_method = 'DEBIT_CARD'";
    
    private static final String SELECT_PAYPAL_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_method = 'PAYPAL'";
    
    private static final String COUNT_BY_PAYMENT_METHOD = "SELECT COUNT(*) FROM orders WHERE payment_method = ?";
    
    private static final String SUM_BY_PAYMENT_METHOD = "SELECT SUM(total_amount) FROM orders WHERE payment_method = ?";
    
    // Date range queries - SQL strings 1201-1240
    private static final String SELECT_BY_ORDER_DATE_RANGE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_date BETWEEN ? AND ?";
    
    private static final String SELECT_BY_ORDER_DATE_AFTER = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_date > ?";
    
    private static final String SELECT_BY_ORDER_DATE_BEFORE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_date < ?";
    
    private static final String SELECT_BY_SHIPPED_DATE_RANGE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE shipped_date BETWEEN ? AND ?";
    
    private static final String SELECT_BY_DELIVERED_DATE_RANGE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE delivered_date BETWEEN ? AND ?";
    
    private static final String SELECT_ORDERS_TODAY = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE DATE(order_date) = DATE(CURRENT)";
    
    private static final String SELECT_ORDERS_THIS_WEEK = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_date > CURRENT - 7 UNITS DAY";
    
    private static final String SELECT_ORDERS_THIS_MONTH = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE MONTH(order_date) = MONTH(CURRENT) AND YEAR(order_date) = YEAR(CURRENT)";
    
    private static final String SELECT_ORDERS_THIS_YEAR = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE YEAR(order_date) = YEAR(CURRENT)";
    
    private static final String SELECT_ORDERS_LAST_30_DAYS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_date > CURRENT - 30 UNITS DAY";
    
    private static final String COUNT_ORDERS_TODAY = "SELECT COUNT(*) FROM orders WHERE DATE(order_date) = DATE(CURRENT)";
    
    private static final String COUNT_ORDERS_THIS_WEEK = "SELECT COUNT(*) FROM orders WHERE order_date > CURRENT - 7 UNITS DAY";
    
    private static final String COUNT_ORDERS_THIS_MONTH = "SELECT COUNT(*) FROM orders WHERE MONTH(order_date) = MONTH(CURRENT) AND YEAR(order_date) = YEAR(CURRENT)";
    
    private static final String COUNT_ORDERS_THIS_YEAR = "SELECT COUNT(*) FROM orders WHERE YEAR(order_date) = YEAR(CURRENT)";
    
    private static final String SUM_ORDERS_TODAY = "SELECT SUM(total_amount) FROM orders WHERE DATE(order_date) = DATE(CURRENT)";
    
    private static final String SUM_ORDERS_THIS_WEEK = "SELECT SUM(total_amount) FROM orders WHERE order_date > CURRENT - 7 UNITS DAY";
    
    private static final String SUM_ORDERS_THIS_MONTH = "SELECT SUM(total_amount) FROM orders WHERE MONTH(order_date) = MONTH(CURRENT) AND YEAR(order_date) = YEAR(CURRENT)";
    
    private static final String SUM_ORDERS_THIS_YEAR = "SELECT SUM(total_amount) FROM orders WHERE YEAR(order_date) = YEAR(CURRENT)";
    
    // Amount range queries - SQL strings 1241-1270
    private static final String SELECT_BY_TOTAL_AMOUNT_RANGE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE total_amount BETWEEN ? AND ?";
    
    private static final String SELECT_BY_TOTAL_AMOUNT_GT = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE total_amount > ?";
    
    private static final String SELECT_BY_TOTAL_AMOUNT_LT = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE total_amount < ?";
    
    private static final String SELECT_HIGH_VALUE_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE total_amount > 1000 ORDER BY total_amount DESC";
    
    private static final String SELECT_LOW_VALUE_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE total_amount < 50 ORDER BY total_amount ASC";
    
    // Location queries - SQL strings 1271-1300
    private static final String SELECT_BY_SHIPPING_CITY = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE shipping_city = ?";
    
    private static final String SELECT_BY_SHIPPING_STATE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE shipping_state = ?";
    
    private static final String SELECT_BY_SHIPPING_ZIP = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE shipping_zip = ?";
    
    private static final String SELECT_BY_CITY_AND_STATE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE shipping_city = ? AND shipping_state = ?";
    
    private static final String COUNT_BY_SHIPPING_STATE = "SELECT COUNT(*) FROM orders WHERE shipping_state = ?";
    
    private static final String COUNT_BY_SHIPPING_CITY = "SELECT COUNT(*) FROM orders WHERE shipping_city = ?";
    
    private static final String SUM_BY_SHIPPING_STATE = "SELECT SUM(total_amount) FROM orders WHERE shipping_state = ?";
    
    // Update operations - SQL strings 1301-1340
    private static final String UPDATE_ORDER_STATUS = "UPDATE orders SET order_status = ?, modified_date = CURRENT WHERE order_id = ?";
    
    private static final String UPDATE_PAYMENT_STATUS = "UPDATE orders SET payment_status = ?, modified_date = CURRENT WHERE order_id = ?";
    
    private static final String UPDATE_SHIPPED_DATE = "UPDATE orders SET shipped_date = ?, order_status = 'SHIPPED', modified_date = CURRENT WHERE order_id = ?";
    
    private static final String UPDATE_DELIVERED_DATE = "UPDATE orders SET delivered_date = ?, order_status = 'DELIVERED', modified_date = CURRENT WHERE order_id = ?";
    
    private static final String UPDATE_TOTAL_AMOUNT = "UPDATE orders SET total_amount = ?, modified_date = CURRENT WHERE order_id = ?";
    
    private static final String UPDATE_SHIPPING_ADDRESS = "UPDATE orders SET shipping_address = ?, shipping_city = ?, shipping_state = ?, shipping_zip = ?, modified_date = CURRENT WHERE order_id = ?";
    
    private static final String MARK_AS_PROCESSING = "UPDATE orders SET order_status = 'PROCESSING', modified_date = CURRENT WHERE order_id = ?";
    
    private static final String MARK_AS_SHIPPED = "UPDATE orders SET order_status = 'SHIPPED', shipped_date = CURRENT, modified_date = CURRENT WHERE order_id = ?";
    
    private static final String MARK_AS_DELIVERED = "UPDATE orders SET order_status = 'DELIVERED', delivered_date = CURRENT, modified_date = CURRENT WHERE order_id = ?";
    
    private static final String MARK_AS_CANCELLED = "UPDATE orders SET order_status = 'CANCELLED', modified_date = CURRENT WHERE order_id = ?";
    
    private static final String MARK_AS_PAID = "UPDATE orders SET payment_status = 'PAID', modified_date = CURRENT WHERE order_id = ?";
    
    private static final String MARK_AS_REFUNDED = "UPDATE orders SET payment_status = 'REFUNDED', modified_date = CURRENT WHERE order_id = ?";
    
    // Aggregation queries - SQL strings 1341-1380
    private static final String SUM_ALL_ORDERS = "SELECT SUM(total_amount) FROM orders";
    
    private static final String AVG_ORDER_VALUE = "SELECT AVG(total_amount) FROM orders";
    
    private static final String MAX_ORDER_VALUE = "SELECT MAX(total_amount) FROM orders";
    
    private static final String MIN_ORDER_VALUE = "SELECT MIN(total_amount) FROM orders";
    
    private static final String COUNT_ALL_ORDERS = "SELECT COUNT(*) FROM orders";
    
    private static final String SUM_TAX_COLLECTED = "SELECT SUM(tax_amount) FROM orders";
    
    private static final String SUM_SHIPPING_COLLECTED = "SELECT SUM(shipping_amount) FROM orders";
    
    private static final String AVG_TAX_AMOUNT = "SELECT AVG(tax_amount) FROM orders";
    
    private static final String AVG_SHIPPING_AMOUNT = "SELECT AVG(shipping_amount) FROM orders";
    
    private static final String SUM_BY_STATUS_AND_DATE = "SELECT SUM(total_amount) FROM orders WHERE order_status = ? AND order_date BETWEEN ? AND ?";
    
    // Group by queries - SQL strings 1381-1420
    private static final String COUNT_BY_STATUS_GROUP = "SELECT order_status, COUNT(*) as order_count FROM orders GROUP BY order_status";
    
    private static final String COUNT_BY_PAYMENT_STATUS_GROUP = "SELECT payment_status, COUNT(*) as order_count FROM orders GROUP BY payment_status";
    
    private static final String COUNT_BY_PAYMENT_METHOD_GROUP = "SELECT payment_method, COUNT(*) as order_count FROM orders GROUP BY payment_method";
    
    private static final String SUM_BY_STATUS_GROUP = "SELECT order_status, SUM(total_amount) as total_revenue FROM orders GROUP BY order_status";
    
    private static final String SUM_BY_PAYMENT_METHOD_GROUP = "SELECT payment_method, SUM(total_amount) as total_revenue FROM orders GROUP BY payment_method";
    
    private static final String COUNT_BY_STATE_GROUP = "SELECT shipping_state, COUNT(*) as order_count FROM orders GROUP BY shipping_state";
    
    private static final String SUM_BY_STATE_GROUP = "SELECT shipping_state, SUM(total_amount) as total_revenue FROM orders GROUP BY shipping_state";
    
    private static final String COUNT_BY_CUSTOMER_GROUP = "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id ORDER BY order_count DESC";
    
    private static final String SUM_BY_CUSTOMER_GROUP = "SELECT customer_id, SUM(total_amount) as total_spent FROM orders GROUP BY customer_id ORDER BY total_spent DESC";
    
    private static final String AVG_BY_CUSTOMER_GROUP = "SELECT customer_id, AVG(total_amount) as avg_order_value FROM orders GROUP BY customer_id";
    
    private static final String ORDERS_BY_DATE_GROUP = "SELECT DATE(order_date) as order_day, COUNT(*) as order_count FROM orders GROUP BY order_day ORDER BY order_day DESC";
    
    private static final String REVENUE_BY_DATE_GROUP = "SELECT DATE(order_date) as order_day, SUM(total_amount) as daily_revenue FROM orders GROUP BY order_day ORDER BY order_day DESC";
    
    private static final String ORDERS_BY_MONTH_GROUP = "SELECT YEAR(order_date) as year, MONTH(order_date) as month, COUNT(*) as order_count FROM orders GROUP BY year, month ORDER BY year DESC, month DESC";
    
    private static final String REVENUE_BY_MONTH_GROUP = "SELECT YEAR(order_date) as year, MONTH(order_date) as month, SUM(total_amount) as monthly_revenue FROM orders GROUP BY year, month ORDER BY year DESC, month DESC";
    
    // Having queries - SQL strings 1421-1450
    private static final String CUSTOMERS_WITH_MULTIPLE_ORDERS = "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING COUNT(*) > 1";
    
    private static final String CUSTOMERS_HIGH_SPEND = "SELECT customer_id, SUM(total_amount) as total_spent FROM orders GROUP BY customer_id HAVING SUM(total_amount) > ?";
    
    private static final String STATES_WITH_MIN_ORDERS = "SELECT shipping_state, COUNT(*) as order_count FROM orders GROUP BY shipping_state HAVING COUNT(*) > ?";
    
    private static final String CUSTOMERS_AVG_HIGH_VALUE = "SELECT customer_id, AVG(total_amount) as avg_order FROM orders GROUP BY customer_id HAVING AVG(total_amount) > ?";
    
    // Ordering queries - SQL strings 1451-1480
    private static final String SELECT_ALL_ORDER_BY_DATE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders ORDER BY order_date";
    
    private static final String SELECT_ALL_ORDER_BY_DATE_DESC = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders ORDER BY order_date DESC";
    
    private static final String SELECT_ALL_ORDER_BY_AMOUNT = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders ORDER BY total_amount";
    
    private static final String SELECT_ALL_ORDER_BY_AMOUNT_DESC = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders ORDER BY total_amount DESC";
    
    private static final String SELECT_ALL_ORDER_BY_CUSTOMER = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders ORDER BY customer_id, order_date DESC";
    
    // Limit queries - SQL strings 1481-1510
    private static final String SELECT_TOP_10_RECENT = "SELECT FIRST 10 order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders ORDER BY order_date DESC";
    
    private static final String SELECT_TOP_10_BY_AMOUNT = "SELECT FIRST 10 order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders ORDER BY total_amount DESC";
    
    private static final String SELECT_TOP_100_RECENT = "SELECT FIRST 100 order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders ORDER BY order_date DESC";
    
    private static final String SELECT_TOP_CUSTOMERS_BY_ORDERS = "SELECT FIRST 10 customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id ORDER BY order_count DESC";
    
    private static final String SELECT_TOP_CUSTOMERS_BY_REVENUE = "SELECT FIRST 10 customer_id, SUM(total_amount) as total_revenue FROM orders GROUP BY customer_id ORDER BY total_revenue DESC";
    
    // Complex business queries - SQL strings 1511-1560
    private static final String SELECT_PENDING_SHIPMENTS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status IN ('PENDING', 'PROCESSING') AND payment_status = 'PAID' ORDER BY order_date ASC";
    
    private static final String SELECT_OVERDUE_SHIPMENTS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = 'PROCESSING' AND order_date < CURRENT - 3 UNITS DAY";
    
    private static final String SELECT_SHIPPED_NOT_DELIVERED = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = 'SHIPPED' AND delivered_date IS NULL";
    
    private static final String SELECT_UNPAID_ORDERS_OLD = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_status = 'UNPAID' AND order_date < CURRENT - 7 UNITS DAY";
    
    private static final String SELECT_COMPLETED_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE order_status = 'DELIVERED' AND payment_status = 'PAID'";
    
    private static final String SELECT_PROBLEM_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE (order_status = 'CANCELLED' OR payment_status = 'REFUNDED') AND order_date > CURRENT - 30 UNITS DAY";
    
    private static final String SELECT_LARGE_UNPAID_ORDERS = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE payment_status = 'UNPAID' AND total_amount > ? ORDER BY total_amount DESC";
    
    private static final String SELECT_RECENT_HIGH_VALUE = "SELECT order_id, customer_id, order_date, shipped_date, delivered_date, order_status, total_amount, tax_amount, shipping_amount, shipping_address, shipping_city, shipping_state, shipping_zip, payment_method, payment_status, created_date, modified_date FROM orders WHERE total_amount > ? AND order_date > CURRENT - 30 UNITS DAY ORDER BY total_amount DESC";
    
    // Statistical queries - SQL strings 1561-1600
    private static final String SELECT_ORDER_STATS = "SELECT COUNT(*) as total_orders, SUM(total_amount) as total_revenue, AVG(total_amount) as avg_order_value, MAX(total_amount) as max_order, MIN(total_amount) as min_order FROM orders";
    
    private static final String SELECT_ORDER_STATS_BY_STATUS = "SELECT order_status, COUNT(*) as count, SUM(total_amount) as revenue, AVG(total_amount) as avg_value FROM orders GROUP BY order_status";
    
    private static final String SELECT_MONTHLY_PERFORMANCE = "SELECT YEAR(order_date) as year, MONTH(order_date) as month, COUNT(*) as orders, SUM(total_amount) as revenue, AVG(total_amount) as avg_value FROM orders GROUP BY year, month ORDER BY year DESC, month DESC";
    
    private static final String SELECT_DAILY_PERFORMANCE = "SELECT DATE(order_date) as day, COUNT(*) as orders, SUM(total_amount) as revenue FROM orders WHERE order_date > CURRENT - 30 UNITS DAY GROUP BY day ORDER BY day DESC";
    
    private static final String SELECT_CUSTOMER_LIFETIME_VALUE = "SELECT customer_id, COUNT(*) as order_count, SUM(total_amount) as lifetime_value, AVG(total_amount) as avg_order, MAX(total_amount) as max_order FROM orders GROUP BY customer_id ORDER BY lifetime_value DESC";
    
    private static final String SELECT_REPEAT_CUSTOMER_RATE = "SELECT COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) * 100.0 / COUNT(DISTINCT customer_id) as repeat_rate FROM (SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id) t";
    
    private static final String SELECT_AVERAGE_DAYS_TO_SHIP = "SELECT AVG((shipped_date - order_date) UNITS DAY) FROM orders WHERE shipped_date IS NOT NULL";
    
    private static final String SELECT_AVERAGE_DAYS_TO_DELIVER = "SELECT AVG((delivered_date - order_date) UNITS DAY) FROM orders WHERE delivered_date IS NOT NULL";
    
    private static final String SELECT_CANCELLATION_RATE = "SELECT COUNT(CASE WHEN order_status = 'CANCELLED' THEN 1 END) * 100.0 / COUNT(*) as cancellation_rate FROM orders";
    
    private static final String SELECT_FULFILLMENT_RATE = "SELECT COUNT(CASE WHEN order_status = 'DELIVERED' THEN 1 END) * 100.0 / COUNT(*) as fulfillment_rate FROM orders";
    
    // Join queries with customer - SQL strings 1601-1650
    private static final String SELECT_ORDERS_WITH_CUSTOMER_INFO = "SELECT o.order_id, o.order_date, o.total_amount, o.order_status, c.first_name, c.last_name, c.email FROM orders o INNER JOIN customer c ON o.customer_id = c.customer_id";
    
    private static final String SELECT_ORDERS_WITH_CUSTOMER_BY_STATUS = "SELECT o.order_id, o.order_date, o.total_amount, o.order_status, c.first_name, c.last_name, c.email FROM orders o INNER JOIN customer c ON o.customer_id = c.customer_id WHERE o.order_status = ?";
    
    private static final String SELECT_HIGH_VALUE_WITH_CUSTOMER = "SELECT o.order_id, o.order_date, o.total_amount, c.first_name, c.last_name, c.email, c.customer_type FROM orders o INNER JOIN customer c ON o.customer_id = c.customer_id WHERE o.total_amount > ? ORDER BY o.total_amount DESC";
    
    private static final String SELECT_VIP_CUSTOMER_ORDERS = "SELECT o.order_id, o.order_date, o.total_amount, c.first_name, c.last_name FROM orders o INNER JOIN customer c ON o.customer_id = c.customer_id WHERE c.customer_type = 'VIP'";
    
    private static final String SELECT_PREMIUM_CUSTOMER_ORDERS = "SELECT o.order_id, o.order_date, o.total_amount, c.first_name, c.last_name FROM orders o INNER JOIN customer c ON o.customer_id = c.customer_id WHERE c.customer_type = 'PREMIUM'";
    
    // Delete operations - SQL strings 1651-1680
    private static final String DELETE_CANCELLED_ORDERS = "DELETE FROM orders WHERE order_status = 'CANCELLED' AND order_date < ?";
    
    private static final String DELETE_OLD_DELIVERED_ORDERS = "DELETE FROM orders WHERE order_status = 'DELIVERED' AND delivered_date < ?";
    
    private static final String DELETE_BY_CUSTOMER = "DELETE FROM orders WHERE customer_id = ?";
    
    // Report queries - SQL strings 1681-1730
    private static final String SELECT_SALES_REPORT_BY_DATE = "SELECT DATE(order_date) as sale_date, COUNT(*) as order_count, SUM(total_amount) as total_sales, AVG(total_amount) as avg_order FROM orders WHERE order_date BETWEEN ? AND ? GROUP BY sale_date ORDER BY sale_date";
    
    private static final String SELECT_REVENUE_BY_STATE_REPORT = "SELECT shipping_state, COUNT(*) as orders, SUM(total_amount) as revenue, AVG(total_amount) as avg_order FROM orders GROUP BY shipping_state ORDER BY revenue DESC";
    
    private static final String SELECT_PAYMENT_METHOD_REPORT = "SELECT payment_method, COUNT(*) as orders, SUM(total_amount) as revenue, AVG(total_amount) as avg_order FROM orders GROUP BY payment_method ORDER BY revenue DESC";
    
    private static final String SELECT_ORDER_STATUS_REPORT = "SELECT order_status, COUNT(*) as count, SUM(total_amount) as total_value, AVG(total_amount) as avg_value FROM orders GROUP BY order_status";
    
    private static final String SELECT_SHIPPING_PERFORMANCE = "SELECT COUNT(*) as total_shipped, AVG((shipped_date - order_date) UNITS DAY) as avg_processing_days FROM orders WHERE shipped_date IS NOT NULL AND order_date > CURRENT - 30 UNITS DAY";
    
    private static final String SELECT_DELIVERY_PERFORMANCE = "SELECT COUNT(*) as total_delivered, AVG((delivered_date - shipped_date) UNITS DAY) as avg_delivery_days FROM orders WHERE delivered_date IS NOT NULL AND shipped_date IS NOT NULL AND order_date > CURRENT - 30 UNITS DAY";
    
    private static final String SELECT_TOP_REVENUE_DAYS = "SELECT FIRST 10 DATE(order_date) as day, SUM(total_amount) as daily_revenue FROM orders GROUP BY day ORDER BY daily_revenue DESC";
    
    private static final String SELECT_CUSTOMER_ORDER_FREQUENCY = "SELECT customer_id, COUNT(*) as order_count, MIN(order_date) as first_order, MAX(order_date) as last_order, (MAX(order_date) - MIN(order_date)) UNITS DAY as customer_tenure FROM orders GROUP BY customer_id HAVING COUNT(*) > 1";
    
    // Additional analytical queries - SQL strings 1731-1800
    private static final String SELECT_ORDERS_BY_HOUR = "SELECT HOUR(order_date) as hour, COUNT(*) as order_count FROM orders WHERE order_date > CURRENT - 7 UNITS DAY GROUP BY hour ORDER BY hour";
    
    private static final String SELECT_ORDERS_BY_DAY_OF_WEEK = "SELECT WEEKDAY(order_date) as day_of_week, COUNT(*) as order_count, SUM(total_amount) as revenue FROM orders GROUP BY day_of_week ORDER BY day_of_week";
    
    private static final String SELECT_AVERAGE_ORDER_BY_CUSTOMER_TYPE = "SELECT c.customer_type, AVG(o.total_amount) as avg_order_value FROM orders o INNER JOIN customer c ON o.customer_id = c.customer_id GROUP BY c.customer_type";
    
    private static final String SELECT_NEW_VS_REPEAT_CUSTOMERS = "SELECT CASE WHEN order_count = 1 THEN 'New' ELSE 'Repeat' END as customer_type, COUNT(*) as customers FROM (SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id) t GROUP BY customer_type";
    
    private static final String SELECT_COHORT_ANALYSIS = "SELECT YEAR(first_order) as cohort_year, MONTH(first_order) as cohort_month, COUNT(*) as customers FROM (SELECT customer_id, MIN(order_date) as first_order FROM orders GROUP BY customer_id) t GROUP BY cohort_year, cohort_month ORDER BY cohort_year DESC, cohort_month DESC";
    
    private static final String SELECT_ORDERS_NEEDING_ATTENTION = "SELECT order_id, customer_id, order_date, order_status, payment_status, total_amount FROM orders WHERE (order_status = 'PENDING' AND order_date < CURRENT - 2 UNITS DAY) OR (order_status = 'PROCESSING' AND order_date < CURRENT - 5 UNITS DAY) OR (payment_status = 'UNPAID' AND order_date < CURRENT - 7 UNITS DAY)";
    
    private static final String SELECT_REVENUE_TREND = "SELECT DATE(order_date) as day, SUM(total_amount) as revenue, SUM(SUM(total_amount)) OVER (ORDER BY DATE(order_date)) as cumulative_revenue FROM orders WHERE order_date > CURRENT - 30 UNITS DAY GROUP BY day ORDER BY day";
    
    private static final String SELECT_ORDER_VALUE_DISTRIBUTION = "SELECT CASE WHEN total_amount < 50 THEN 'Under $50' WHEN total_amount < 100 THEN '$50-$100' WHEN total_amount < 250 THEN '$100-$250' WHEN total_amount < 500 THEN '$250-$500' ELSE 'Over $500' END as value_range, COUNT(*) as order_count, SUM(total_amount) as total_revenue FROM orders GROUP BY value_range";
    
    private static final String SELECT_ORDERS_WITH_HIGH_SHIPPING_COST = "SELECT order_id, customer_id, total_amount, shipping_amount, (shipping_amount / NULLIF(total_amount, 0) * 100) as shipping_percentage FROM orders WHERE shipping_amount > total_amount * 0.2";
    
    private static final String SELECT_ORDERS_WITH_HIGH_TAX = "SELECT order_id, customer_id, total_amount, tax_amount, (tax_amount / NULLIF(total_amount, 0) * 100) as tax_percentage FROM orders WHERE tax_amount > 0 ORDER BY tax_percentage DESC";
    
    public OrderDAO() {
    }
    
    public void insert(Order order) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(INSERT_ORDER);
            // Set parameters (implementation details)
            stmt.executeUpdate();
        } finally {
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
}
