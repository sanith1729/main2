/**
 * backendcompanies.js
 * Backend controller and routes for the companies module
 */

const express = require('express');
const router = express.Router();

// Helper function for pagination
const getPaginationParams = (req) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 25;
  const offset = (page - 1) * limit;
  
  return { page, limit, offset };
};

/**
 * Get company statistics
 * @route GET /api/companies/stats
 */
router.get('/stats', async (req, res) => {
  try {
    // Base query parameters for RLS
    let whereClause = '';
    let params = [];
    
    if (req.applyRLS) {
      whereClause = 'WHERE client_id = ? AND app_id = ?';
      params.push(req.clientId, req.appId);
      
      if (req.authenticatedUser) {
        whereClause += ' AND (created_by = ? OR is_public = 1)';
        params.push(req.userId);
      }
    }
    
    // Get total companies count
    const [totalCompaniesResult] = await req.db.query(
      `SELECT COUNT(*) as totalCompanies 
       FROM companies 
       ${whereClause}`,
      params
    );
    
    // Top industries breakdown
    const [topIndustries] = await req.db.query(
      `SELECT industry, COUNT(*) as count 
       FROM companies 
       ${whereClause}
       GROUP BY industry 
       ORDER BY count DESC 
       LIMIT 5`,
      params
    );
    
    // Build deal stages filter for active deals
    let dealParams = [...params]; // Clone params array
    
    // Get active deals count - FIXED QUERY
    const [activeDealsResult] = await req.db.query(
      `SELECT COUNT(*) as activeDeals 
       FROM deals d
       JOIN deal_stages ds ON d.stage_id = ds.id
       ${whereClause ? whereClause.replace(/\bclient_id\b/g, 'd.client_id').replace(/\bapp_id\b/g, 'd.app_id').replace(/\bcreated_by\b/g, 'd.created_by').replace(/\bis_public\b/g, 'd.is_public') : 'WHERE 1=1'} 
       AND ds.type = 'active'
       ${req.applyRLS ? ' AND ds.client_id = ? AND ds.app_id = ?' : ''}`,
      req.applyRLS ? [...dealParams, req.clientId, req.appId] : dealParams
    );
    
    // Get total revenue from won deals - FIXED QUERY
    const [totalRevenueResult] = await req.db.query(
      `SELECT IFNULL(SUM(d.value), 0) as totalRevenue 
       FROM deals d
       JOIN deal_stages ds ON d.stage_id = ds.id
       ${whereClause ? whereClause.replace(/\bclient_id\b/g, 'd.client_id').replace(/\bapp_id\b/g, 'd.app_id').replace(/\bcreated_by\b/g, 'd.created_by').replace(/\bis_public\b/g, 'd.is_public') : 'WHERE 1=1'} 
       AND ds.type = 'won'
       ${req.applyRLS ? ' AND ds.client_id = ? AND ds.app_id = ?' : ''}`,
      req.applyRLS ? [...dealParams, req.clientId, req.appId] : dealParams
    );
    
    // Get average deal size - FIXED QUERY
    const [avgDealSizeResult] = await req.db.query(
      `SELECT IFNULL(AVG(d.value), 0) as averageDealSize 
       FROM deals d
       JOIN deal_stages ds ON d.stage_id = ds.id
       ${whereClause ? whereClause.replace(/\bclient_id\b/g, 'd.client_id').replace(/\bapp_id\b/g, 'd.app_id').replace(/\bcreated_by\b/g, 'd.created_by').replace(/\bis_public\b/g, 'd.is_public') : 'WHERE 1=1'} 
       AND ds.type = 'won'
       ${req.applyRLS ? ' AND ds.client_id = ? AND ds.app_id = ?' : ''}`,
      req.applyRLS ? [...dealParams, req.clientId, req.appId] : dealParams
    );
    
    // Return the statistics
    res.json({
      success: true,
      data: {
        totalCompanies: totalCompaniesResult[0].totalCompanies || 0,
        activeDeals: activeDealsResult[0].activeDeals || 0,
        totalRevenue: totalRevenueResult[0].totalRevenue || 0,
        averageDealSize: avgDealSizeResult[0].averageDealSize || 0,
        topIndustries: topIndustries || []
      }
    });
    
  } catch (err) {
    console.error('Error fetching company stats:', err);
    res.status(500).json({ 
      success: false, 
      message: 'Server error',
      error: err.message
    });
  }
});

/**
 * Get all companies with filtering, sorting and pagination
 * @route GET /api/companies
 */
router.get('/', async (req, res) => {
  try {
    const { page, limit, offset } = getPaginationParams(req);
    
    // Get sorting parameters
    let sortField = 'name'; // Default sort by name
    let sortDirection = 'ASC';
    
    if (req.query.sort) {
      if (req.query.sort.startsWith('-')) {
        sortDirection = 'DESC';
        sortField = req.query.sort.substring(1);
      } else {
        sortField = req.query.sort;
      }
    }
    
    // Map frontend sort fields to database columns
    const sortFieldMap = {
      'name': 'c.name',
      'industry': 'c.industry',
      'size': 'c.employees_count',
      'revenue': 'c.annual_revenue',
      'location': 'c.city', // Using city as location sort
      'status': 'c.status',
      'deals': 'open_deals',
      'created_at': 'c.created_at',
      'updated_at': 'c.updated_at'
    };
    
    const dbSortField = sortFieldMap[sortField] || 'c.name';
    
    // Build base query with open deals count
    let baseQuery = `
      SELECT c.*, 
             (SELECT COUNT(*) FROM deals d 
              JOIN deal_stages ds ON d.stage_id = ds.id 
              WHERE d.company_id = c.id AND ds.type = 'active'
    `;
    
    // Add RLS to subquery if needed
    if (req.applyRLS) {
      baseQuery += ' AND d.client_id = ? AND d.app_id = ?';
    }
    
    // Complete subquery
    baseQuery += `) as open_deals FROM companies c`;
    
    // Start building the count query
    let countQuery = 'SELECT COUNT(*) as total FROM companies c';
    
    // Apply RLS filtering
    let whereAdded = false;
    let params = [];
    let countParams = [];
    
    if (req.applyRLS) {
      whereAdded = true;
      baseQuery += ' WHERE c.client_id = ? AND c.app_id = ?';
      countQuery += ' WHERE c.client_id = ? AND c.app_id = ?';
      
      // Add parameters for both main query and subquery
      params.push(req.clientId, req.appId, req.clientId, req.appId);
      countParams.push(req.clientId, req.appId);
      
      // Add user-level permissions if authenticated
      if (req.authenticatedUser) {
        baseQuery += ' AND (c.created_by = ? OR c.is_public = 1)';
        countQuery += ' AND (c.created_by = ? OR c.is_public = 1)';
        params.push(req.userId);
        countParams.push(req.userId);
      }
    }
    
    // Apply industry filter
    if (req.query.industry) {
      if (whereAdded) {
        baseQuery += ' AND c.industry = ?';
        countQuery += ' AND c.industry = ?';
      } else {
        baseQuery += ' WHERE c.industry = ?';
        countQuery += ' WHERE c.industry = ?';
        whereAdded = true;
      }
      params.push(req.query.industry);
      countParams.push(req.query.industry);
    }
    
    // Apply status filter
    if (req.query.status) {
      if (whereAdded) {
        baseQuery += ' AND c.status = ?';
        countQuery += ' AND c.status = ?';
      } else {
        baseQuery += ' WHERE c.status = ?';
        countQuery += ' WHERE c.status = ?';
        whereAdded = true;
      }
      params.push(req.query.status);
      countParams.push(req.query.status);
    }
    
    // Apply company size filter
    if (req.query.size) {
      const sizeFilter = req.query.size;
      let sizeClause = '';
      
      if (whereAdded) {
        sizeClause = ' AND c.employees_count';
      } else {
        sizeClause = ' WHERE c.employees_count';
        whereAdded = true;
      }
      
      // Handle different size ranges
      switch (sizeFilter) {
        case '1-10':
          baseQuery += `${sizeClause} BETWEEN 1 AND 10`;
          countQuery += `${sizeClause} BETWEEN 1 AND 10`;
          break;
        case '11-50':
          baseQuery += `${sizeClause} BETWEEN 11 AND 50`;
          countQuery += `${sizeClause} BETWEEN 11 AND 50`;
          break;
        case '51-200':
          baseQuery += `${sizeClause} BETWEEN 51 AND 200`;
          countQuery += `${sizeClause} BETWEEN 51 AND 200`;
          break;
        case '201-500':
          baseQuery += `${sizeClause} BETWEEN 201 AND 500`;
          countQuery += `${sizeClause} BETWEEN 201 AND 500`;
          break;
        case '501-1000':
          baseQuery += `${sizeClause} BETWEEN 501 AND 1000`;
          countQuery += `${sizeClause} BETWEEN 501 AND 1000`;
          break;
        case '1001+':
          baseQuery += `${sizeClause} > 1000`;
          countQuery += `${sizeClause} > 1000`;
          break;
        default:
          if (!isNaN(parseInt(sizeFilter))) {
            baseQuery += `${sizeClause} = ?`;
            countQuery += `${sizeClause} = ?`;
            params.push(parseInt(sizeFilter));
            countParams.push(parseInt(sizeFilter));
          }
      }
    }
    
    // Apply revenue filter
    if (req.query.revenue) {
      const revenueFilter = req.query.revenue;
      let revenueClause = '';
      
      if (whereAdded) {
        revenueClause = ' AND c.annual_revenue';
      } else {
        revenueClause = ' WHERE c.annual_revenue';
        whereAdded = true;
      }
      
      // Handle different revenue ranges
      switch (revenueFilter) {
        case '<1M':
          baseQuery += `${revenueClause} < 1000000`;
          countQuery += `${revenueClause} < 1000000`;
          break;
        case '1-10M':
          baseQuery += `${revenueClause} BETWEEN 1000000 AND 10000000`;
          countQuery += `${revenueClause} BETWEEN 1000000 AND 10000000`;
          break;
        case '10-50M':
          baseQuery += `${revenueClause} BETWEEN 10000000 AND 50000000`;
          countQuery += `${revenueClause} BETWEEN 10000000 AND 50000000`;
          break;
        case '50-100M':
          baseQuery += `${revenueClause} BETWEEN 50000000 AND 100000000`;
          countQuery += `${revenueClause} BETWEEN 50000000 AND 100000000`;
          break;
        case '100M+':
          baseQuery += `${revenueClause} > 100000000`;
          countQuery += `${revenueClause} > 100000000`;
          break;
        default:
          if (!isNaN(parseFloat(revenueFilter))) {
            baseQuery += `${revenueClause} = ?`;
            countQuery += `${revenueClause} = ?`;
            params.push(parseFloat(revenueFilter));
            countParams.push(parseFloat(revenueFilter));
          }
      }
    }
    
    // Apply search filter
    if (req.query.search) {
      const searchTerm = `%${req.query.search}%`;
      
      if (whereAdded) {
        baseQuery += ' AND (c.name LIKE ? OR c.industry LIKE ? OR c.city LIKE ? OR c.country LIKE ? OR c.phone LIKE ?)';
        countQuery += ' AND (c.name LIKE ? OR c.industry LIKE ? OR c.city LIKE ? OR c.country LIKE ? OR c.phone LIKE ?)';
      } else {
        baseQuery += ' WHERE (c.name LIKE ? OR c.industry LIKE ? OR c.city LIKE ? OR c.country LIKE ? OR c.phone LIKE ?)';
        countQuery += ' WHERE (c.name LIKE ? OR c.industry LIKE ? OR c.city LIKE ? OR c.country LIKE ? OR c.phone LIKE ?)';
        whereAdded = true;
      }
      params.push(searchTerm, searchTerm, searchTerm, searchTerm, searchTerm);
      countParams.push(searchTerm, searchTerm, searchTerm, searchTerm, searchTerm);
    }
    
    // Add sorting to the base query
    baseQuery += ` ORDER BY ${dbSortField} ${sortDirection}`;
    
    // Add pagination
    baseQuery += ' LIMIT ? OFFSET ?';
    params.push(limit, offset);
    
    // Execute queries
    console.log('Companies Query:', baseQuery);
    console.log('Params:', params);
    
    const [companies] = await req.db.query(baseQuery, params);
    const [countResult] = await req.db.query(countQuery, countParams);
    const total = countResult[0].total;
    
    res.json({
      success: true,
      data: companies,
      pagination: {
        page,
        limit,
        total,
        pages: Math.ceil(total / limit)
      }
    });
  } catch (err) {
    console.error('Error fetching companies:', err);
    res.status(500).json({ 
      success: false, 
      message: 'Server error',
      error: err.message
    });
  }
});

/**
 * Get a single company by ID
 * @route GET /api/companies/:id
 */
router.get('/:id', async (req, res) => {
  try {
    // Build query with RLS support
    let query = 'SELECT * FROM companies WHERE id = ?';
    let params = [req.params.id];
    
    if (req.applyRLS) {
      query += ' AND client_id = ? AND app_id = ?';
      params.push(req.clientId, req.appId);
      
      if (req.authenticatedUser) {
        query += ' AND (created_by = ? OR is_public = 1)';
        params.push(req.userId);
      }
    }
    
    const [companies] = await req.db.query(query, params);
    
    if (companies.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Company not found or you do not have permission to access it'
      });
    }
    
    res.json({
      success: true,
      data: companies[0]
    });
  } catch (err) {
    console.error('Error fetching company:', err);
    res.status(500).json({
      success: false,
      message: 'Server error',
      error: err.message
    });
  }
});

/**
 * Create a new company
 * @route POST /api/companies
 */
router.post('/', async (req, res) => {
  try {
    // Validate required fields
    if (!req.body.name) {
      return res.status(400).json({
        success: false,
        message: 'Company name is required'
      });
    }
    
    // Extract fields from request body
    const {
      name,
      industry,
      employees_count,
      website,
      phone,
      address,
      city,
      state,
      postal_code,
      country,
      annual_revenue,
      status = 'active', // Default status
      notes,
      is_public = 1 // Default to public
    } = req.body;
    
    // Apply multi-tenancy
    let client_id = null;
    let app_id = null;
    let created_by = null;
    
    if (req.applyRLS) {
      client_id = req.clientId;
      app_id = req.appId;
      
      if (req.authenticatedUser) {
        created_by = req.userId;
      }
    }
    
    // Set timestamps
    const now = new Date();
    
    // Build query and params
    const query = `
      INSERT INTO companies (
        name, industry, employees_count, website, phone, 
        address, city, state, postal_code, country,
        annual_revenue, status, notes, is_public,
        client_id, app_id, created_by, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    const params = [
      name, industry, employees_count, website, phone,
      address, city, state, postal_code, country,
      annual_revenue, status, notes, is_public,
      client_id, app_id, created_by, now, now
    ];
    
    // Execute query
    const [result] = await req.db.query(query, params);
    const companyId = result.insertId;
    
    // Fetch the created company
    const [companies] = await req.db.query(
      'SELECT * FROM companies WHERE id = ?',
      [companyId]
    );
    
    res.status(201).json({
      success: true,
      message: 'Company created successfully',
      data: companies[0]
    });
  } catch (err) {
    console.error('Error creating company:', err);
    res.status(500).json({
      success: false,
      message: 'Server error',
      error: err.message
    });
  }
});

/**
 * Update a company
 * @route PUT /api/companies/:id
 */
router.put('/:id', async (req, res) => {
  try {
    const companyId = req.params.id;
    
    // Validate required fields
    if (!req.body.name) {
      return res.status(400).json({
        success: false,
        message: 'Company name is required'
      });
    }
    
    // Check if company exists and user has permission
    let query = 'SELECT * FROM companies WHERE id = ?';
    let params = [companyId];
    
    if (req.applyRLS) {
      query += ' AND client_id = ? AND app_id = ?';
      params.push(req.clientId, req.appId);
      
      if (req.authenticatedUser) {
        query += ' AND (created_by = ? OR is_public = 1)';
        params.push(req.userId);
      }
    }
    
    const [companies] = await req.db.query(query, params);
    
    if (companies.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Company not found or you do not have permission to update it'
      });
    }
    
    // Extract fields from request body
    const {
      name,
      industry,
      employees_count,
      website,
      phone,
      address,
      city,
      state,
      postal_code,
      country,
      annual_revenue,
      status,
      notes
    } = req.body;
    
    // Set updated timestamp
    const now = new Date();
    
    // Build update query
    let updateQuery = `
      UPDATE companies
      SET name = ?, industry = ?, employees_count = ?, website = ?, phone = ?,
          address = ?, city = ?, state = ?, postal_code = ?, country = ?,
          annual_revenue = ?, status = ?, notes = ?, updated_at = ?
      WHERE id = ?
    `;
    
    let updateParams = [
      name, industry, employees_count, website, phone,
      address, city, state, postal_code, country,
      annual_revenue, status, notes, now, companyId
    ];
    
    // If RLS is applied, add tenant constraints
    if (req.applyRLS) {
      updateQuery += ' AND client_id = ? AND app_id = ?';
      updateParams.push(req.clientId, req.appId);
    }
    
    // Execute update query
    await req.db.query(updateQuery, updateParams);
    
    // Fetch updated company
    const [updatedCompanies] = await req.db.query(
      'SELECT * FROM companies WHERE id = ?',
      [companyId]
    );
    
    res.json({
      success: true,
      message: 'Company updated successfully',
      data: updatedCompanies[0]
    });
  } catch (err) {
    console.error('Error updating company:', err);
    res.status(500).json({
      success: false,
      message: 'Server error',
      error: err.message
    });
  }
});

/**
 * Delete a company
 * @route DELETE /api/companies/:id
 */
router.delete('/:id', async (req, res) => {
  try {
    const companyId = req.params.id;
    
    // Check if company exists and user has permission
    let query = 'SELECT * FROM companies WHERE id = ?';
    let params = [companyId];
    
    if (req.applyRLS) {
      query += ' AND client_id = ? AND app_id = ?';
      params.push(req.clientId, req.appId);
      
      if (req.authenticatedUser) {
        query += ' AND (created_by = ? OR is_public = 1)';
        params.push(req.userId);
      }
    }
    
    const [companies] = await req.db.query(query, params);
    
    if (companies.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Company not found or you do not have permission to delete it'
      });
    }
    
    // Build delete query
    let deleteQuery = 'DELETE FROM companies WHERE id = ?';
    let deleteParams = [companyId];
    
    if (req.applyRLS) {
      deleteQuery += ' AND client_id = ? AND app_id = ?';
      deleteParams.push(req.clientId, req.appId);
    }
    
    // Execute delete query
    await req.db.query(deleteQuery, deleteParams);
    
    res.json({
      success: true,
      message: 'Company deleted successfully'
    });
  } catch (err) {
    console.error('Error deleting company:', err);
    res.status(500).json({
      success: false,
      message: 'Server error',
      error: err.message
    });
  }
});

/**
 * Get company contacts
 * @route GET /api/companies/:id/contacts
 */
router.get('/:id/contacts', async (req, res) => {
  try {
    const companyId = req.params.id;
    
    // Check if company exists and user has permission
    let companyQuery = 'SELECT * FROM companies WHERE id = ?';
    let companyParams = [companyId];
    
    if (req.applyRLS) {
      companyQuery += ' AND client_id = ? AND app_id = ?';
      companyParams.push(req.clientId, req.appId);
      
      if (req.authenticatedUser) {
        companyQuery += ' AND (created_by = ? OR is_public = 1)';
        companyParams.push(req.userId);
      }
    }
    
    const [companies] = await req.db.query(companyQuery, companyParams);
    
    if (companies.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Company not found or you do not have permission to access it'
      });
    }
    
    // Get contacts with RLS support
    let contactsQuery = `
      SELECT c.*, 
             CONCAT(c.first_name, ' ', c.last_name) AS full_name 
      FROM contacts c 
      WHERE c.company_id = ?
    `;
    
    let contactsParams = [companyId];
    
    if (req.applyRLS) {
      contactsQuery += ' AND c.client_id = ? AND c.app_id = ?';
      contactsParams.push(req.clientId, req.appId);
      
      if (req.authenticatedUser) {
        contactsQuery += ' AND (c.created_by = ? OR c.is_public = 1)';
        contactsParams.push(req.userId);
      }
    }
    
    contactsQuery += ' ORDER BY c.first_name, c.last_name';
    
    const [contacts] = await req.db.query(contactsQuery, contactsParams);
    
    res.json({
      success: true,
      data: contacts
    });
  } catch (err) {
    console.error('Error fetching company contacts:', err);
    res.status(500).json({
      success: false,
      message: 'Server error',
      error: err.message
    });
  }
});

/**
 * Get company deals
 * @route GET /api/companies/:id/deals
 */
router.get('/:id/deals', async (req, res) => {
  try {
    const companyId = req.params.id;
    
    // Check if company exists and user has permission
    let companyQuery = 'SELECT * FROM companies WHERE id = ?';
    let companyParams = [companyId];
    
    if (req.applyRLS) {
      companyQuery += ' AND client_id = ? AND app_id = ?';
      companyParams.push(req.clientId, req.appId);
      
      if (req.authenticatedUser) {
        companyQuery += ' AND (created_by = ? OR is_public = 1)';
        companyParams.push(req.userId);
      }
    }
    
    const [companies] = await req.db.query(companyQuery, companyParams);
    
    if (companies.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Company not found or you do not have permission to access it'
      });
    }
    
    // Get deals with RLS support
    let dealsQuery = `
      SELECT d.*, 
             ds.name AS stage_name, 
             ds.type AS stage_type,
             ds.color AS stage_color,
             c.first_name AS contact_first_name,
             c.last_name AS contact_last_name,
             CONCAT(c.first_name, ' ', c.last_name) AS contact_name
      FROM deals d
      JOIN deal_stages ds ON d.stage_id = ds.id
      LEFT JOIN contacts c ON d.contact_id = c.id
      WHERE d.company_id = ?
    `;
    
    let dealsParams = [companyId];
    
    if (req.applyRLS) {
      dealsQuery += ' AND d.client_id = ? AND d.app_id = ?';
      dealsParams.push(req.clientId, req.appId);
      
      if (req.authenticatedUser) {
        dealsQuery += ' AND (d.created_by = ? OR d.is_public = 1)';
        dealsParams.push(req.userId);
      }
    }
    
    dealsQuery += ' ORDER BY d.expected_close_date ASC';
    
    const [deals] = await req.db.query(dealsQuery, dealsParams);
    
    res.json({
      success: true,
      data: deals
    });
  } catch (err) {
    console.error('Error fetching company deals:', err);
    res.status(500).json({
      success: false,
      message: 'Server error',
      error: err.message
    });
  }
});

// App-specific routes router - for routes in the format /api/apps/:appId/companies
const appRouter = express.Router({ mergeParams: true });

// Create the same routes for the app-specific pattern
appRouter.get('/stats', router.stack.find(layer => layer.route?.path === '/stats').handle);
appRouter.get('/', router.stack.find(layer => layer.route?.path === '/').handle);
appRouter.get('/:id', router.stack.find(layer => layer.route?.path === '/:id').handle);
appRouter.post('/', router.stack.find(layer => layer.route?.path === '/' && layer.route.methods.post).handle);
appRouter.put('/:id', router.stack.find(layer => layer.route?.path === '/:id' && layer.route.methods.put).handle);
appRouter.delete('/:id', router.stack.find(layer => layer.route?.path === '/:id' && layer.route.methods.delete).handle);
appRouter.get('/:id/contacts', router.stack.find(layer => layer.route?.path === '/:id/contacts').handle);
appRouter.get('/:id/deals', router.stack.find(layer => layer.route?.path === '/:id/deals').handle);

module.exports = {
  companiesRouter: router,
  companiesAppRouter: appRouter
};
