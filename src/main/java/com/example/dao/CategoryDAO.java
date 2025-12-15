package com.example.dao;

import com.example.model.Category;
import com.example.util.ConnectionManager;

import java.sql.*;

/**
 * Data Access Object for Category entity
 * SQL strings 3251-3400
 */
public class CategoryDAO {
    
    // Basic CRUD - SQL strings 3251-3260
    private static final String INSERT_CATEGORY = "INSERT INTO category (category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date) VALUES (?, ?, ?, ?, ?, ?, CURRENT, CURRENT)";
    private static final String UPDATE_CATEGORY = "UPDATE category SET category_name = ?, parent_category_id = ?, description = ?, display_order = ?, status = ?, modified_date = CURRENT WHERE category_id = ?";
    private static final String DELETE_CATEGORY = "DELETE FROM category WHERE category_id = ?";
    private static final String SELECT_BY_ID = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE category_id = ?";
    private static final String SELECT_ALL = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category";
    
    // Search queries - SQL strings 3261-3290
    private static final String SELECT_BY_NAME = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE category_name = ?";
    private static final String SELECT_BY_PARENT_ID = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE parent_category_id = ?";
    private static final String SELECT_BY_STATUS = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE status = ?";
    private static final String SELECT_ROOT_CATEGORIES = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE parent_category_id IS NULL";
    private static final String SELECT_ACTIVE_CATEGORIES = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE status = 'ACTIVE'";
    private static final String SELECT_INACTIVE_CATEGORIES = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE status = 'INACTIVE'";
    
    // Hierarchy queries - SQL strings 3291-3320
    private static final String SELECT_CHILD_CATEGORIES = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE parent_category_id = ? ORDER BY display_order";
    private static final String SELECT_CATEGORY_PATH = "SELECT c1.category_id, c1.category_name, c2.category_name as parent_name FROM category c1 LEFT JOIN category c2 ON c1.parent_category_id = c2.category_id WHERE c1.category_id = ?";
    private static final String SELECT_SUBCATEGORY_COUNT = "SELECT COUNT(*) FROM category WHERE parent_category_id = ?";
    private static final String CHECK_HAS_CHILDREN = "SELECT COUNT(*) FROM category WHERE parent_category_id = ?";
    
    // Update operations - SQL strings 3321-3350
    private static final String UPDATE_NAME = "UPDATE category SET category_name = ?, modified_date = CURRENT WHERE category_id = ?";
    private static final String UPDATE_DESCRIPTION = "UPDATE category SET description = ?, modified_date = CURRENT WHERE category_id = ?";
    private static final String UPDATE_DISPLAY_ORDER = "UPDATE category SET display_order = ?, modified_date = CURRENT WHERE category_id = ?";
    private static final String UPDATE_STATUS = "UPDATE category SET status = ?, modified_date = CURRENT WHERE category_id = ?";
    private static final String UPDATE_PARENT = "UPDATE category SET parent_category_id = ?, modified_date = CURRENT WHERE category_id = ?";
    private static final String ACTIVATE_CATEGORY = "UPDATE category SET status = 'ACTIVE', modified_date = CURRENT WHERE category_id = ?";
    private static final String DEACTIVATE_CATEGORY = "UPDATE category SET status = 'INACTIVE', modified_date = CURRENT WHERE category_id = ?";
    private static final String MOVE_CATEGORY = "UPDATE category SET parent_category_id = ?, modified_date = CURRENT WHERE category_id = ?";
    private static final String REORDER_CATEGORY = "UPDATE category SET display_order = display_order + 1 WHERE parent_category_id = ? AND display_order >= ?";
    
    // Aggregation queries - SQL strings 3351-3370
    private static final String COUNT_ALL = "SELECT COUNT(*) FROM category";
    private static final String COUNT_BY_STATUS = "SELECT COUNT(*) FROM category WHERE status = ?";
    private static final String COUNT_ACTIVE = "SELECT COUNT(*) FROM category WHERE status = 'ACTIVE'";
    private static final String COUNT_ROOT_CATEGORIES = "SELECT COUNT(*) FROM category WHERE parent_category_id IS NULL";
    
    // LIKE queries - SQL strings 3371-3385
    private static final String SELECT_BY_NAME_LIKE = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE category_name LIKE ?";
    private static final String SELECT_BY_DESCRIPTION_LIKE = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category WHERE description LIKE ?";
    
    // Ordering queries - SQL strings 3386-3395
    private static final String SELECT_ALL_ORDER_BY_NAME = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category ORDER BY category_name";
    private static final String SELECT_ALL_ORDER_BY_DISPLAY = "SELECT category_id, category_name, parent_category_id, description, display_order, status, created_date, modified_date FROM category ORDER BY display_order";
    
    // Exists queries - SQL strings 3396-3400
    private static final String EXISTS_BY_NAME = "SELECT COUNT(*) FROM category WHERE category_name = ?";
    private static final String CHECK_DUPLICATE_NAME = "SELECT COUNT(*) FROM category WHERE category_name = ? AND category_id != ?";
    private static final String CHECK_CIRCULAR_REFERENCE = "SELECT COUNT(*) FROM category WHERE category_id = ? AND parent_category_id = ?";
    
    public CategoryDAO() {}
}
