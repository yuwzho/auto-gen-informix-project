package com.example.dao;

import com.example.model.Product;
import com.example.util.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object for Product entity with extensive SQL operations
 * This file continues from CustomerDAO.java
 */
public class ProductDAO {
    
    private static final Logger logger = LoggerFactory.getLogger(ProductDAO.class);
    
    // Basic CRUD operations - SQL strings 301-320
    private static final String INSERT_PRODUCT = "INSERT INTO product (product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT, CURRENT, ?, ?)";
    
    private static final String UPDATE_PRODUCT = "UPDATE product SET product_code = ?, product_name = ?, description = ?, category = ?, sub_category = ?, price = ?, cost = ?, stock_quantity = ?, reorder_level = ?, supplier = ?, manufacturer = ?, modified_date = CURRENT, status = ?, barcode = ? WHERE product_id = ?";
    
    private static final String DELETE_PRODUCT = "DELETE FROM product WHERE product_id = ?";
    
    private static final String SELECT_BY_ID = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE product_id = ?";
    
    private static final String SELECT_ALL = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product";
    
    // Search queries - SQL strings 321-360
    private static final String SELECT_BY_CODE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE product_code = ?";
    
    private static final String SELECT_BY_NAME = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE product_name = ?";
    
    private static final String SELECT_BY_CATEGORY = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE category = ?";
    
    private static final String SELECT_BY_SUB_CATEGORY = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE sub_category = ?";
    
    private static final String SELECT_BY_SUPPLIER = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE supplier = ?";
    
    private static final String SELECT_BY_MANUFACTURER = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE manufacturer = ?";
    
    private static final String SELECT_BY_STATUS = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE status = ?";
    
    private static final String SELECT_BY_BARCODE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE barcode = ?";
    
    private static final String SELECT_ACTIVE_PRODUCTS = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE status = 'ACTIVE'";
    
    private static final String SELECT_DISCONTINUED_PRODUCTS = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE status = 'DISCONTINUED'";
    
    // LIKE queries - SQL strings 361-380
    private static final String SELECT_BY_NAME_LIKE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE product_name LIKE ?";
    
    private static final String SELECT_BY_CODE_LIKE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE product_code LIKE ?";
    
    private static final String SELECT_BY_DESCRIPTION_LIKE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE description LIKE ?";
    
    private static final String SELECT_BY_CATEGORY_LIKE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE category LIKE ?";
    
    private static final String SELECT_BY_SUPPLIER_LIKE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE supplier LIKE ?";
    
    // Price range queries - SQL strings 381-400
    private static final String SELECT_BY_PRICE_RANGE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE price BETWEEN ? AND ?";
    
    private static final String SELECT_BY_PRICE_GT = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE price > ?";
    
    private static final String SELECT_BY_PRICE_LT = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE price < ?";
    
    private static final String SELECT_BY_PRICE_GTE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE price >= ?";
    
    private static final String SELECT_BY_PRICE_LTE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE price <= ?";
    
    private static final String SELECT_BY_COST_RANGE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE cost BETWEEN ? AND ?";
    
    private static final String SELECT_BY_COST_GT = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE cost > ?";
    
    private static final String SELECT_BY_COST_LT = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE cost < ?";
    
    // Stock queries - SQL strings 401-420
    private static final String SELECT_BY_STOCK_RANGE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE stock_quantity BETWEEN ? AND ?";
    
    private static final String SELECT_LOW_STOCK = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE stock_quantity < reorder_level";
    
    private static final String SELECT_OUT_OF_STOCK = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE stock_quantity = 0";
    
    private static final String SELECT_IN_STOCK = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE stock_quantity > 0";
    
    private static final String SELECT_HIGH_STOCK = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE stock_quantity > reorder_level * 2";
    
    private static final String SELECT_STOCK_GT = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE stock_quantity > ?";
    
    private static final String SELECT_STOCK_LT = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE stock_quantity < ?";
    
    // Combined search queries - SQL strings 421-450
    private static final String SELECT_BY_CATEGORY_AND_STATUS = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE category = ? AND status = ?";
    
    private static final String SELECT_BY_CATEGORY_PRICE_RANGE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE category = ? AND price BETWEEN ? AND ?";
    
    private static final String SELECT_BY_SUPPLIER_CATEGORY = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE supplier = ? AND category = ?";
    
    private static final String SELECT_BY_MANUFACTURER_STATUS = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE manufacturer = ? AND status = ?";
    
    private static final String SELECT_BY_CATEGORY_SUBCATEGORY = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE category = ? AND sub_category = ?";
    
    // Update specific fields - SQL strings 451-480
    private static final String UPDATE_PRODUCT_NAME = "UPDATE product SET product_name = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_PRICE = "UPDATE product SET price = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_COST = "UPDATE product SET cost = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_STOCK_QUANTITY = "UPDATE product SET stock_quantity = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_REORDER_LEVEL = "UPDATE product SET reorder_level = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_STATUS = "UPDATE product SET status = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_CATEGORY = "UPDATE product SET category = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_SUB_CATEGORY = "UPDATE product SET sub_category = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_SUPPLIER = "UPDATE product SET supplier = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_MANUFACTURER = "UPDATE product SET manufacturer = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_DESCRIPTION = "UPDATE product SET description = ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_BARCODE = "UPDATE product SET barcode = ?, modified_date = CURRENT WHERE product_id = ?";
    
    // Stock operations - SQL strings 481-510
    private static final String INCREMENT_STOCK = "UPDATE product SET stock_quantity = stock_quantity + ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String DECREMENT_STOCK = "UPDATE product SET stock_quantity = stock_quantity - ?, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String RESET_STOCK = "UPDATE product SET stock_quantity = 0, modified_date = CURRENT WHERE product_id = ?";
    
    private static final String INCREASE_PRICE_PERCENT = "UPDATE product SET price = price * (1 + ? / 100), modified_date = CURRENT WHERE product_id = ?";
    
    private static final String DECREASE_PRICE_PERCENT = "UPDATE product SET price = price * (1 - ? / 100), modified_date = CURRENT WHERE product_id = ?";
    
    private static final String INCREASE_COST_PERCENT = "UPDATE product SET cost = cost * (1 + ? / 100), modified_date = CURRENT WHERE product_id = ?";
    
    private static final String UPDATE_PRICE_BY_CATEGORY = "UPDATE product SET price = price * (1 + ? / 100), modified_date = CURRENT WHERE category = ?";
    
    private static final String UPDATE_STATUS_BY_CATEGORY = "UPDATE product SET status = ?, modified_date = CURRENT WHERE category = ?";
    
    private static final String UPDATE_STATUS_BY_SUPPLIER = "UPDATE product SET status = ?, modified_date = CURRENT WHERE supplier = ?";
    
    // Count queries - SQL strings 511-540
    private static final String COUNT_ALL = "SELECT COUNT(*) FROM product";
    
    private static final String COUNT_BY_CATEGORY = "SELECT COUNT(*) FROM product WHERE category = ?";
    
    private static final String COUNT_BY_STATUS = "SELECT COUNT(*) FROM product WHERE status = ?";
    
    private static final String COUNT_BY_SUPPLIER = "SELECT COUNT(*) FROM product WHERE supplier = ?";
    
    private static final String COUNT_BY_MANUFACTURER = "SELECT COUNT(*) FROM product WHERE manufacturer = ?";
    
    private static final String COUNT_ACTIVE = "SELECT COUNT(*) FROM product WHERE status = 'ACTIVE'";
    
    private static final String COUNT_OUT_OF_STOCK = "SELECT COUNT(*) FROM product WHERE stock_quantity = 0";
    
    private static final String COUNT_LOW_STOCK = "SELECT COUNT(*) FROM product WHERE stock_quantity < reorder_level";
    
    private static final String COUNT_BY_PRICE_RANGE = "SELECT COUNT(*) FROM product WHERE price BETWEEN ? AND ?";
    
    private static final String COUNT_BY_CATEGORY_STATUS = "SELECT COUNT(*) FROM product WHERE category = ? AND status = ?";
    
    // EXISTS queries - SQL strings 541-560
    private static final String EXISTS_BY_CODE = "SELECT COUNT(*) FROM product WHERE product_code = ?";
    
    private static final String EXISTS_BY_BARCODE = "SELECT COUNT(*) FROM product WHERE barcode = ?";
    
    private static final String EXISTS_BY_ID = "SELECT COUNT(*) FROM product WHERE product_id = ?";
    
    private static final String CHECK_DUPLICATE_CODE = "SELECT COUNT(*) FROM product WHERE product_code = ? AND product_id != ?";
    
    private static final String CHECK_DUPLICATE_BARCODE = "SELECT COUNT(*) FROM product WHERE barcode = ? AND product_id != ?";
    
    // Ordering queries - SQL strings 561-590
    private static final String SELECT_ALL_ORDER_BY_NAME = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY product_name";
    
    private static final String SELECT_ALL_ORDER_BY_PRICE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY price";
    
    private static final String SELECT_ALL_ORDER_BY_PRICE_DESC = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY price DESC";
    
    private static final String SELECT_ALL_ORDER_BY_STOCK = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY stock_quantity";
    
    private static final String SELECT_ALL_ORDER_BY_STOCK_DESC = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY stock_quantity DESC";
    
    private static final String SELECT_ALL_ORDER_BY_CATEGORY = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY category, product_name";
    
    private static final String SELECT_ALL_ORDER_BY_CREATED_DATE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY created_date DESC";
    
    private static final String SELECT_ALL_ORDER_BY_MODIFIED_DATE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY modified_date DESC";
    
    // Limit queries - SQL strings 591-610
    private static final String SELECT_TOP_10 = "SELECT FIRST 10 product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product";
    
    private static final String SELECT_TOP_10_BY_PRICE = "SELECT FIRST 10 product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY price DESC";
    
    private static final String SELECT_TOP_10_CHEAPEST = "SELECT FIRST 10 product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY price ASC";
    
    private static final String SELECT_TOP_10_NEWEST = "SELECT FIRST 10 product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product ORDER BY created_date DESC";
    
    private static final String SELECT_TOP_10_LOW_STOCK = "SELECT FIRST 10 product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE stock_quantity < reorder_level ORDER BY stock_quantity ASC";
    
    private static final String SELECT_TOP_100 = "SELECT FIRST 100 product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product";
    
    // Aggregation queries - SQL strings 611-640
    private static final String SUM_STOCK_VALUE = "SELECT SUM(stock_quantity * price) FROM product";
    
    private static final String SUM_STOCK_COST = "SELECT SUM(stock_quantity * cost) FROM product";
    
    private static final String SUM_STOCK_QUANTITY = "SELECT SUM(stock_quantity) FROM product";
    
    private static final String AVG_PRICE = "SELECT AVG(price) FROM product";
    
    private static final String AVG_COST = "SELECT AVG(cost) FROM product";
    
    private static final String AVG_STOCK = "SELECT AVG(stock_quantity) FROM product";
    
    private static final String MAX_PRICE = "SELECT MAX(price) FROM product";
    
    private static final String MIN_PRICE = "SELECT MIN(price) FROM product";
    
    private static final String MAX_COST = "SELECT MAX(cost) FROM product";
    
    private static final String MIN_COST = "SELECT MIN(cost) FROM product";
    
    private static final String AVG_PRICE_BY_CATEGORY = "SELECT AVG(price) FROM product WHERE category = ?";
    
    private static final String SUM_STOCK_BY_CATEGORY = "SELECT SUM(stock_quantity) FROM product WHERE category = ?";
    
    // Group by queries - SQL strings 641-670
    private static final String COUNT_BY_CATEGORY_GROUP = "SELECT category, COUNT(*) as product_count FROM product GROUP BY category";
    
    private static final String COUNT_BY_SUPPLIER_GROUP = "SELECT supplier, COUNT(*) as product_count FROM product GROUP BY supplier";
    
    private static final String COUNT_BY_MANUFACTURER_GROUP = "SELECT manufacturer, COUNT(*) as product_count FROM product GROUP BY manufacturer";
    
    private static final String COUNT_BY_STATUS_GROUP = "SELECT status, COUNT(*) as product_count FROM product GROUP BY status";
    
    private static final String SUM_STOCK_BY_CATEGORY_GROUP = "SELECT category, SUM(stock_quantity) as total_stock FROM product GROUP BY category";
    
    private static final String AVG_PRICE_BY_CATEGORY_GROUP = "SELECT category, AVG(price) as avg_price FROM product GROUP BY category";
    
    private static final String SUM_VALUE_BY_CATEGORY = "SELECT category, SUM(stock_quantity * price) as total_value FROM product GROUP BY category";
    
    private static final String AVG_PRICE_BY_SUPPLIER = "SELECT supplier, AVG(price) as avg_price FROM product GROUP BY supplier";
    
    private static final String COUNT_BY_SUBCATEGORY = "SELECT sub_category, COUNT(*) as product_count FROM product GROUP BY sub_category";
    
    // Having queries - SQL strings 671-690
    private static final String CATEGORIES_WITH_MIN_PRODUCTS = "SELECT category, COUNT(*) as product_count FROM product GROUP BY category HAVING COUNT(*) > ?";
    
    private static final String CATEGORIES_WITH_HIGH_AVG_PRICE = "SELECT category, AVG(price) as avg_price FROM product GROUP BY category HAVING AVG(price) > ?";
    
    private static final String SUPPLIERS_WITH_HIGH_PRODUCT_COUNT = "SELECT supplier, COUNT(*) as product_count FROM product GROUP BY supplier HAVING COUNT(*) > ?";
    
    // IN queries - SQL strings 691-710
    private static final String SELECT_BY_CATEGORIES_IN = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE category IN (?, ?, ?)";
    
    private static final String SELECT_BY_STATUS_IN = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE status IN (?, ?)";
    
    private static final String SELECT_BY_SUPPLIERS_IN = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE supplier IN (?, ?, ?)";
    
    // NOT IN queries - SQL strings 711-720
    private static final String SELECT_NOT_IN_CATEGORIES = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE category NOT IN (?, ?)";
    
    private static final String SELECT_NOT_IN_STATUS = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE status NOT IN (?)";
    
    // NULL checks - SQL strings 721-740
    private static final String SELECT_NULL_SUPPLIER = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE supplier IS NULL";
    
    private static final String SELECT_NULL_MANUFACTURER = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE manufacturer IS NULL";
    
    private static final String SELECT_NULL_BARCODE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE barcode IS NULL";
    
    private static final String SELECT_NOT_NULL_BARCODE = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE barcode IS NOT NULL";
    
    // Distinct queries - SQL strings 741-760
    private static final String SELECT_DISTINCT_CATEGORIES = "SELECT DISTINCT category FROM product ORDER BY category";
    
    private static final String SELECT_DISTINCT_SUPPLIERS = "SELECT DISTINCT supplier FROM product WHERE supplier IS NOT NULL ORDER BY supplier";
    
    private static final String SELECT_DISTINCT_MANUFACTURERS = "SELECT DISTINCT manufacturer FROM product WHERE manufacturer IS NOT NULL ORDER BY manufacturer";
    
    private static final String SELECT_DISTINCT_STATUSES = "SELECT DISTINCT status FROM product ORDER BY status";
    
    private static final String SELECT_DISTINCT_SUBCATEGORIES = "SELECT DISTINCT sub_category FROM product WHERE sub_category IS NOT NULL ORDER BY sub_category";
    
    private static final String SELECT_DISTINCT_CATEGORIES_BY_SUPPLIER = "SELECT DISTINCT category FROM product WHERE supplier = ? ORDER BY category";
    
    // Complex queries - SQL strings 761-790
    private static final String SELECT_HIGH_MARGIN_PRODUCTS = "SELECT product_id, product_code, product_name, price, cost, (price - cost) as margin FROM product WHERE price > cost * 1.5 ORDER BY margin DESC";
    
    private static final String SELECT_PROFITABLE_PRODUCTS = "SELECT product_id, product_code, product_name, price, cost, ((price - cost) / cost * 100) as margin_percent FROM product WHERE cost > 0 ORDER BY margin_percent DESC";
    
    private static final String SELECT_PRODUCTS_NEED_REORDER = "SELECT product_id, product_code, product_name, stock_quantity, reorder_level, supplier FROM product WHERE stock_quantity <= reorder_level AND status = 'ACTIVE' ORDER BY stock_quantity ASC";
    
    private static final String SELECT_OVERSTOCK_PRODUCTS = "SELECT product_id, product_code, product_name, stock_quantity, reorder_level FROM product WHERE stock_quantity > reorder_level * 3 ORDER BY stock_quantity DESC";
    
    private static final String SELECT_NEW_PRODUCTS_LAST_30_DAYS = "SELECT product_id, product_code, product_name, created_date FROM product WHERE created_date > CURRENT - 30 UNITS DAY ORDER BY created_date DESC";
    
    private static final String SELECT_RECENTLY_MODIFIED = "SELECT product_id, product_code, product_name, modified_date FROM product WHERE modified_date > CURRENT - 7 UNITS DAY ORDER BY modified_date DESC";
    
    private static final String SELECT_EXPENSIVE_PRODUCTS = "SELECT product_id, product_code, product_name, price FROM product WHERE price > ? ORDER BY price DESC";
    
    private static final String SELECT_BUDGET_PRODUCTS = "SELECT product_id, product_code, product_name, price FROM product WHERE price < ? ORDER BY price ASC";
    
    // Delete queries - SQL strings 791-810
    private static final String DELETE_BY_STATUS = "DELETE FROM product WHERE status = ?";
    
    private static final String DELETE_DISCONTINUED = "DELETE FROM product WHERE status = 'DISCONTINUED'";
    
    private static final String DELETE_OLD_PRODUCTS = "DELETE FROM product WHERE created_date < ?";
    
    private static final String DELETE_BY_CATEGORY = "DELETE FROM product WHERE category = ?";
    
    private static final String DELETE_OUT_OF_STOCK_DISCONTINUED = "DELETE FROM product WHERE stock_quantity = 0 AND status = 'DISCONTINUED'";
    
    // Search variations - SQL strings 811-840
    private static final String SELECT_BY_NAME_STARTS_WITH = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE product_name LIKE ? || '%'";
    
    private static final String SELECT_BY_CODE_STARTS_WITH = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE product_code LIKE ? || '%'";
    
    private static final String SELECT_BY_NAME_ENDS_WITH = "SELECT product_id, product_code, product_name, description, category, sub_category, price, cost, stock_quantity, reorder_level, supplier, manufacturer, created_date, modified_date, status, barcode FROM product WHERE product_name LIKE '%' || ?";
    
    private static final String SEARCH_PRODUCTS_ALL_FIELDS = "SELECT product_id, product_code, product_name, description, category FROM product WHERE product_name LIKE '%' || ? || '%' OR product_code LIKE '%' || ? || '%' OR description LIKE '%' || ? || '%'";
    
    // Subquery examples - SQL strings 841-860
    private static final String SELECT_ABOVE_AVG_PRICE = "SELECT product_id, product_code, product_name, price FROM product WHERE price > (SELECT AVG(price) FROM product)";
    
    private static final String SELECT_MAX_PRICE_PRODUCT = "SELECT product_id, product_code, product_name, price FROM product WHERE price = (SELECT MAX(price) FROM product)";
    
    private static final String SELECT_MIN_PRICE_PRODUCT = "SELECT product_id, product_code, product_name, price FROM product WHERE price = (SELECT MIN(price) FROM product)";
    
    private static final String SELECT_ABOVE_AVG_STOCK = "SELECT product_id, product_code, product_name, stock_quantity FROM product WHERE stock_quantity > (SELECT AVG(stock_quantity) FROM product)";
    
    // Case-based queries - SQL strings 861-880
    private static final String SELECT_WITH_PRICE_CATEGORY = "SELECT product_id, product_name, price, CASE WHEN price < 10 THEN 'BUDGET' WHEN price < 50 THEN 'STANDARD' WHEN price < 200 THEN 'PREMIUM' ELSE 'LUXURY' END as price_category FROM product";
    
    private static final String SELECT_WITH_STOCK_STATUS = "SELECT product_id, product_name, stock_quantity, CASE WHEN stock_quantity = 0 THEN 'OUT_OF_STOCK' WHEN stock_quantity < reorder_level THEN 'LOW_STOCK' WHEN stock_quantity > reorder_level * 2 THEN 'OVERSTOCKED' ELSE 'NORMAL' END as stock_status FROM product";
    
    private static final String SELECT_WITH_PRODUCT_AGE = "SELECT product_id, product_name, created_date, CASE WHEN created_date > CURRENT - 30 UNITS DAY THEN 'NEW' WHEN created_date > CURRENT - 180 UNITS DAY THEN 'RECENT' ELSE 'ESTABLISHED' END as product_age FROM product";
    
    // Date queries - SQL strings 881-900
    private static final String SELECT_CREATED_THIS_MONTH = "SELECT product_id, product_code, product_name, created_date FROM product WHERE MONTH(created_date) = MONTH(CURRENT) AND YEAR(created_date) = YEAR(CURRENT)";
    
    private static final String SELECT_CREATED_THIS_YEAR = "SELECT product_id, product_code, product_name, created_date FROM product WHERE YEAR(created_date) = YEAR(CURRENT)";
    
    private static final String SELECT_MODIFIED_TODAY = "SELECT product_id, product_code, product_name, modified_date FROM product WHERE DATE(modified_date) = DATE(CURRENT)";
    
    private static final String SELECT_MODIFIED_THIS_WEEK = "SELECT product_id, product_code, product_name, modified_date FROM product WHERE modified_date > CURRENT - 7 UNITS DAY";
    
    private static final String COUNT_NEW_THIS_MONTH = "SELECT COUNT(*) FROM product WHERE MONTH(created_date) = MONTH(CURRENT) AND YEAR(created_date) = YEAR(CURRENT)";
    
    private static final String COUNT_NEW_THIS_YEAR = "SELECT COUNT(*) FROM product WHERE YEAR(created_date) = YEAR(CURRENT)";
    
    // Pagination - SQL strings 901-920
    private static final String SELECT_PAGE_GENERIC = "SELECT SKIP ? FIRST ? product_id, product_code, product_name, price FROM product ORDER BY product_id";
    
    private static final String SELECT_PAGE_BY_CATEGORY = "SELECT SKIP ? FIRST ? product_id, product_code, product_name, price FROM product WHERE category = ? ORDER BY product_id";
    
    // Additional combinations - SQL strings 921-950
    private static final String SELECT_BY_CATEGORY_PRICE_STOCK = "SELECT product_id, product_code, product_name, price, stock_quantity FROM product WHERE category = ? AND price BETWEEN ? AND ? AND stock_quantity > 0";
    
    private static final String SELECT_ACTIVE_IN_STOCK_BY_CATEGORY = "SELECT product_id, product_code, product_name, price, stock_quantity FROM product WHERE status = 'ACTIVE' AND stock_quantity > 0 AND category = ?";
    
    private static final String SELECT_DISCOUNTED_PRODUCTS = "SELECT product_id, product_code, product_name, price, cost FROM product WHERE price < cost * 1.2";
    
    // String functions - SQL strings 951-970
    private static final String SELECT_UPPER_NAMES = "SELECT product_id, UPPER(product_name) as product_name FROM product";
    
    private static final String SELECT_LOWER_CODES = "SELECT product_id, LOWER(product_code) as product_code FROM product";
    
    private static final String SELECT_NAME_LENGTH = "SELECT product_id, product_name, LENGTH(product_name) as name_length FROM product";
    
    // Statistical queries - SQL strings 971-1000
    private static final String SELECT_STOCK_STATS = "SELECT COUNT(*) as total_products, SUM(stock_quantity) as total_stock, AVG(stock_quantity) as avg_stock, MAX(stock_quantity) as max_stock, MIN(stock_quantity) as min_stock FROM product";
    
    private static final String SELECT_PRICE_STATS = "SELECT COUNT(*) as total_products, SUM(price) as sum_price, AVG(price) as avg_price, MAX(price) as max_price, MIN(price) as min_price FROM product";
    
    private static final String SELECT_CATEGORY_STATS = "SELECT category, COUNT(*) as total, SUM(stock_quantity) as total_stock, AVG(price) as avg_price, SUM(stock_quantity * price) as inventory_value FROM product GROUP BY category ORDER BY inventory_value DESC";
    
    private static final String SELECT_SUPPLIER_STATS = "SELECT supplier, COUNT(*) as product_count, AVG(price) as avg_price, SUM(stock_quantity) as total_stock FROM product WHERE supplier IS NOT NULL GROUP BY supplier ORDER BY product_count DESC";
    
    private static final String SELECT_INVENTORY_VALUE_BY_CATEGORY = "SELECT category, SUM(stock_quantity * price) as inventory_value, SUM(stock_quantity * cost) as inventory_cost FROM product GROUP BY category ORDER BY inventory_value DESC";
    
    private static final String SELECT_MARGIN_ANALYSIS = "SELECT category, AVG((price - cost) / NULLIF(cost, 0) * 100) as avg_margin_percent FROM product WHERE cost > 0 GROUP BY category ORDER BY avg_margin_percent DESC";
    
    private static final String SELECT_STOCK_TURNOVER = "SELECT category, COUNT(*) as products, SUM(CASE WHEN stock_quantity = 0 THEN 1 ELSE 0 END) as out_of_stock, SUM(CASE WHEN stock_quantity < reorder_level THEN 1 ELSE 0 END) as low_stock FROM product GROUP BY category";
    
    // Batch update operations - SQL strings 1001-1020
    private static final String UPDATE_PRICE_BY_CATEGORY_RANGE = "UPDATE product SET price = price * (1 + ? / 100), modified_date = CURRENT WHERE category = ? AND price BETWEEN ? AND ?";
    
    private static final String ACTIVATE_PRODUCTS_BY_CATEGORY = "UPDATE product SET status = 'ACTIVE', modified_date = CURRENT WHERE category = ?";
    
    private static final String DISCONTINUE_LOW_STOCK = "UPDATE product SET status = 'DISCONTINUED', modified_date = CURRENT WHERE stock_quantity = 0 AND modified_date < CURRENT - 90 UNITS DAY";
    
    private static final String UPDATE_REORDER_LEVELS_BY_CATEGORY = "UPDATE product SET reorder_level = ?, modified_date = CURRENT WHERE category = ?";
    
    // Report queries - SQL strings 1021-1050
    private static final String SELECT_INVENTORY_REPORT = "SELECT product_id, product_code, product_name, category, stock_quantity, reorder_level, price, (stock_quantity * price) as stock_value FROM product ORDER BY stock_value DESC";
    
    private static final String SELECT_LOW_STOCK_REPORT = "SELECT product_id, product_code, product_name, category, supplier, stock_quantity, reorder_level, (reorder_level - stock_quantity) as units_needed FROM product WHERE stock_quantity < reorder_level ORDER BY units_needed DESC";
    
    private static final String SELECT_PROFIT_MARGIN_REPORT = "SELECT product_id, product_code, product_name, price, cost, (price - cost) as profit, ((price - cost) / NULLIF(cost, 0) * 100) as margin_percent FROM product WHERE cost > 0 ORDER BY margin_percent DESC";
    
    private static final String SELECT_CATEGORY_PERFORMANCE = "SELECT category, COUNT(*) as total_products, SUM(stock_quantity) as total_stock, AVG(price) as avg_price, SUM(stock_quantity * price) as total_value FROM product GROUP BY category ORDER BY total_value DESC";
    
    private static final String SELECT_SUPPLIER_PERFORMANCE = "SELECT supplier, COUNT(*) as products_supplied, AVG(price) as avg_product_price, SUM(stock_quantity) as total_stock FROM product WHERE supplier IS NOT NULL GROUP BY supplier ORDER BY products_supplied DESC";
    
    // Additional business logic queries - SQL strings 1051-1100
    private static final String SELECT_PRODUCTS_FOR_PROMOTION = "SELECT product_id, product_code, product_name, price, stock_quantity FROM product WHERE stock_quantity > reorder_level * 2 AND status = 'ACTIVE' ORDER BY stock_quantity DESC";
    
    private static final String SELECT_CLEARANCE_CANDIDATES = "SELECT product_id, product_code, product_name, price, stock_quantity, modified_date FROM product WHERE status = 'ACTIVE' AND modified_date < CURRENT - 180 UNITS DAY AND stock_quantity > 0";
    
    private static final String SELECT_FAST_MOVERS = "SELECT p.product_id, p.product_code, p.product_name, p.stock_quantity FROM product p WHERE p.stock_quantity < p.reorder_level AND p.status = 'ACTIVE'";
    
    private static final String SELECT_SLOW_MOVERS = "SELECT p.product_id, p.product_code, p.product_name, p.stock_quantity FROM product p WHERE p.stock_quantity > p.reorder_level * 3 AND p.status = 'ACTIVE'";
    
    private static final String SELECT_ZERO_MARGIN_PRODUCTS = "SELECT product_id, product_code, product_name, price, cost FROM product WHERE price <= cost AND status = 'ACTIVE'";
    
    private static final String SELECT_HIGH_VALUE_INVENTORY = "SELECT product_id, product_code, product_name, stock_quantity, price, (stock_quantity * price) as total_value FROM product WHERE (stock_quantity * price) > ? ORDER BY total_value DESC";
    
    private static final String SELECT_PRODUCTS_BY_MARGIN_THRESHOLD = "SELECT product_id, product_code, product_name, price, cost, ((price - cost) / NULLIF(cost, 0) * 100) as margin FROM product WHERE cost > 0 AND ((price - cost) / cost * 100) > ? ORDER BY margin DESC";
    
    private static final String SELECT_PRODUCTS_BELOW_COST = "SELECT product_id, product_code, product_name, price, cost FROM product WHERE price < cost";
    
    private static final String UPDATE_STATUS_OLD_PRODUCTS = "UPDATE product SET status = 'DISCONTINUED', modified_date = CURRENT WHERE created_date < ? AND stock_quantity = 0";
    
    private static final String SELECT_PRODUCTS_BY_VALUE_RANGE = "SELECT product_id, product_code, product_name, stock_quantity, price, (stock_quantity * price) as inventory_value FROM product WHERE (stock_quantity * price) BETWEEN ? AND ? ORDER BY inventory_value DESC";
    
    public ProductDAO() {
    }
    
    public void insert(Product product) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(INSERT_PRODUCT);
            stmt.setLong(1, product.getProductId());
            stmt.setString(2, product.getProductCode());
            stmt.setString(3, product.getProductName());
            stmt.setString(4, product.getDescription());
            stmt.setString(5, product.getCategory());
            stmt.setString(6, product.getSubCategory());
            stmt.setBigDecimal(7, product.getPrice());
            stmt.setBigDecimal(8, product.getCost());
            stmt.setInt(9, product.getStockQuantity());
            stmt.setInt(10, product.getReorderLevel());
            stmt.setString(11, product.getSupplier());
            stmt.setString(12, product.getManufacturer());
            stmt.setString(13, product.getStatus());
            stmt.setString(14, product.getBarcode());
            stmt.executeUpdate();
        } finally {
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    public void update(Product product) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(UPDATE_PRODUCT);
            stmt.setString(1, product.getProductCode());
            stmt.setString(2, product.getProductName());
            stmt.setString(3, product.getDescription());
            stmt.setString(4, product.getCategory());
            stmt.setString(5, product.getSubCategory());
            stmt.setBigDecimal(6, product.getPrice());
            stmt.setBigDecimal(7, product.getCost());
            stmt.setInt(8, product.getStockQuantity());
            stmt.setInt(9, product.getReorderLevel());
            stmt.setString(10, product.getSupplier());
            stmt.setString(11, product.getManufacturer());
            stmt.setString(12, product.getStatus());
            stmt.setString(13, product.getBarcode());
            stmt.setLong(14, product.getProductId());
            stmt.executeUpdate();
        } finally {
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    public void delete(Long productId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(DELETE_PRODUCT);
            stmt.setLong(1, productId);
            stmt.executeUpdate();
        } finally {
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    public Product findById(Long productId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement(SELECT_BY_ID);
            stmt.setLong(1, productId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return extractProduct(rs);
            }
            return null;
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            ConnectionManager.closeConnection(conn);
        }
    }
    
    private Product extractProduct(ResultSet rs) throws SQLException {
        Product product = new Product();
        product.setProductId(rs.getLong("product_id"));
        product.setProductCode(rs.getString("product_code"));
        product.setProductName(rs.getString("product_name"));
        product.setDescription(rs.getString("description"));
        product.setCategory(rs.getString("category"));
        product.setSubCategory(rs.getString("sub_category"));
        product.setPrice(rs.getBigDecimal("price"));
        product.setCost(rs.getBigDecimal("cost"));
        product.setStockQuantity(rs.getInt("stock_quantity"));
        product.setReorderLevel(rs.getInt("reorder_level"));
        product.setSupplier(rs.getString("supplier"));
        product.setManufacturer(rs.getString("manufacturer"));
        product.setCreatedDate(rs.getDate("created_date"));
        product.setModifiedDate(rs.getDate("modified_date"));
        product.setStatus(rs.getString("status"));
        product.setBarcode(rs.getString("barcode"));
        return product;
    }
}
