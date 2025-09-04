/**
 * backendcalendars.js
 * Comprehensive Calendar API implementation with multi-tenant support
 */

const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const auth = require('./auth');

// Calendar constants
const EVENT_TYPES = {
  MEETING: 'meeting',
  CALL: 'call',
  DEADLINE: 'deadline',
  REMINDER: 'reminder', 
  PERSONAL: 'personal'
};

const DEFAULT_CALENDARS = [
  { id: 'work', name: 'Work', color: '#4361ee' },
  { id: 'sales', name: 'Sales', color: '#10b981' },
  { id: 'marketing', name: 'Marketing', color: '#f59e0b' },
  { id: 'personal', name: 'Personal', color: '#8b5cf6' }
];

/**
 * Ensure calendar tables exist
 * This function creates necessary tables if they don't exist yet
 */
async function ensureCalendarTables(db) {
  try {
    // Check and create calendars table
    const [calendarCheck] = await db.query(
      `SELECT COUNT(*) as count 
       FROM information_schema.tables 
       WHERE table_schema = DATABASE() 
       AND table_name = 'calendars'`
    );
    
    if (calendarCheck[0].count === 0) {
      console.log('Creating calendars table');
      await db.query(`
        CREATE TABLE calendars (
          id VARCHAR(50) PRIMARY KEY,
          name VARCHAR(100) NOT NULL,
          color VARCHAR(20) NOT NULL,
          owner_id INT,
          is_default BOOLEAN DEFAULT 0,
          client_id INT NOT NULL,
          app_id VARCHAR(50) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX idx_client_app (client_id, app_id)
        )
      `);
    }
    
    // Check and create calendar_events table
    const [eventsCheck] = await db.query(
      `SELECT COUNT(*) as count 
       FROM information_schema.tables 
       WHERE table_schema = DATABASE() 
       AND table_name = 'calendar_events'`
    );
    
    if (eventsCheck[0].count === 0) {
      console.log('Creating calendar_events table');
      await db.query(`
        CREATE TABLE calendar_events (
          id INT AUTO_INCREMENT PRIMARY KEY,
          title VARCHAR(255) NOT NULL,
          description TEXT,
          type VARCHAR(50) NOT NULL,
          start_date DATETIME NOT NULL,
          end_date DATETIME,
          is_all_day BOOLEAN DEFAULT 0,
          location VARCHAR(255),
          reminder_minutes INT,
          calendar_id VARCHAR(50),
          created_by INT,
          client_id INT NOT NULL,
          app_id VARCHAR(50) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX idx_client_app (client_id, app_id),
          INDEX idx_dates (start_date, end_date),
          FOREIGN KEY (calendar_id) REFERENCES calendars(id)
        )
      `);
    }
    
    // Check and create event_attendees table
    const [attendeesCheck] = await db.query(
      `SELECT COUNT(*) as count 
       FROM information_schema.tables 
       WHERE table_schema = DATABASE() 
       AND table_name = 'event_attendees'`
    );
    
    if (attendeesCheck[0].count === 0) {
      console.log('Creating event_attendees table');
      await db.query(`
        CREATE TABLE event_attendees (
          id INT AUTO_INCREMENT PRIMARY KEY,
          event_id INT NOT NULL,
          user_id INT NOT NULL,
          status VARCHAR(20) DEFAULT 'pending',
          client_id INT NOT NULL,
          app_id VARCHAR(50) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          INDEX idx_client_app (client_id, app_id),
          FOREIGN KEY (event_id) REFERENCES calendar_events(id) ON DELETE CASCADE
        )
      `);
    }
    
    // Check and create user_calendar_preferences table
    const [prefsCheck] = await db.query(
      `SELECT COUNT(*) as count 
       FROM information_schema.tables 
       WHERE table_schema = DATABASE() 
       AND table_name = 'user_calendar_preferences'`
    );
    
    if (prefsCheck[0].count === 0) {
      console.log('Creating user_calendar_preferences table');
      await db.query(`
        CREATE TABLE user_calendar_preferences (
          id INT AUTO_INCREMENT PRIMARY KEY,
          user_id INT NOT NULL,
          calendar_id VARCHAR(50) NOT NULL,
          active BOOLEAN DEFAULT 1,
          client_id INT NOT NULL,
          app_id VARCHAR(50) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY user_calendar_idx (user_id, calendar_id, client_id, app_id),
          INDEX idx_client_app (client_id, app_id)
        )
      `);
    }
    
    return true;
  } catch (error) {
    console.error('Error ensuring calendar tables:', error);
    return false;
  }
}

/**
 * Initialize default calendars for a client/app
 */
async function initDefaultCalendars(db, clientId, appId) {
  try {
    // Check if client already has calendars
    const [existingCalendars] = await db.query(
      'SELECT COUNT(*) as count FROM calendars WHERE client_id = ? AND app_id = ?',
      [clientId, appId]
    );
    
    if (existingCalendars[0].count > 0) {
      return true; // Already initialized
    }
    
    // Clean the app ID by removing special characters
    const appIdClean = appId.replace(/[^a-zA-Z0-9]/g, '');
    
    // Insert default calendars
    for (const calendar of DEFAULT_CALENDARS) {
      const calendarId = `cal_${clientId}_${appIdClean}_${calendar.id}`;
      await db.query(
        'INSERT INTO calendars (id, name, color, is_default, client_id, app_id) VALUES (?, ?, ?, 1, ?, ?)',
        [calendarId, calendar.name, calendar.color, clientId, appId]
      );
    }
    
    return true;
  } catch (error) {
    console.error('Error initializing default calendars:', error);
    return false;
  }
}

/**
 * Get all events with filtering
 */
async function getEvents(req, res) {
  try {
    const db = req.db;
    
    // Ensure tables exist
    await ensureCalendarTables(db);
    
    // Initialize default calendars if needed
    if (req.applyRLS) {
      await initDefaultCalendars(db, req.clientId, req.appId);
    }
    
    // Parse query parameters
    const start = req.query.start ? new Date(req.query.start) : null;
    const end = req.query.end ? new Date(req.query.end) : null;
    const calendars = req.query.calendars ? req.query.calendars.split(',') : null;
    const type = req.query.type || null;
    const search = req.query.search || null;
    
    // Build the WHERE clause based on filters
    let whereClause = '1=1';
    let params = [];
    
    // Apply tenant isolation
    if (req.applyRLS) {
      whereClause += ` AND e.client_id = ? AND e.app_id = ?`;
      params.push(req.clientId, req.appId);
    }
    
    // Date filtering
    if (start) {
      whereClause += ` AND (
        (e.end_date >= ? OR e.is_all_day = 1)
      )`;
      params.push(start.toISOString());
    }
    
    if (end) {
      whereClause += ` AND (
        (e.start_date <= ? OR e.is_all_day = 1)
      )`;
      params.push(end.toISOString());
    }
    
    // Calendar filtering
    if (calendars && calendars.length > 0) {
      whereClause += ` AND e.calendar_id IN (${calendars.map(() => '?').join(',')})`;
      params = [...params, ...calendars];
    }
    
    // Type filtering
    if (type) {
      whereClause += ' AND e.type = ?';
      params.push(type);
    }
    
    // Search term
    if (search) {
      whereClause += ' AND (e.title LIKE ? OR e.description LIKE ? OR e.location LIKE ?)';
      const searchParam = `%${search}%`;
      params.push(searchParam, searchParam, searchParam);
    }
    
    // Query for events with filtering and proper joins
    const [events] = await db.query(
      `SELECT 
         e.id,
         e.title,
         e.description,
         e.type,
         e.start_date,
         e.end_date,
         e.is_all_day,
         e.location,
         e.reminder_minutes,
         e.calendar_id,
         c.name as calendar_name,
         c.color as calendar_color,
         COUNT(DISTINCT ea.id) as attendee_count
       FROM calendar_events e
       LEFT JOIN calendars c ON e.calendar_id = c.id 
         AND c.client_id = e.client_id 
         AND c.app_id = e.app_id
       LEFT JOIN event_attendees ea ON e.id = ea.event_id 
         AND ea.client_id = e.client_id 
         AND ea.app_id = e.app_id
       WHERE ${whereClause}
       GROUP BY e.id
       ORDER BY e.start_date ASC`,
      params
    );
    
    // Format the event data for the frontend
    const formattedEvents = events.map(event => ({
      id: event.id,
      title: event.title,
      description: event.description || '',
      type: event.type,
      start: event.start_date,
      end: event.end_date,
      isAllDay: Boolean(event.is_all_day),
      location: event.location || '',
      reminderMinutes: event.reminder_minutes,
      calendar: {
        id: event.calendar_id,
        name: event.calendar_name,
        color: event.calendar_color
      },
      attendeeCount: event.attendee_count
    }));
    
    return res.json({
      success: true,
      events: formattedEvents
    });
    
  } catch (error) {
    console.error('Error fetching calendar events:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to fetch events',
      error: error.message
    });
  }
}

/**
 * Get upcoming events
 */
async function getUpcomingEvents(req, res) {
  try {
    const db = req.db;
    const limit = parseInt(req.query.limit) || 5;
    
    // Ensure tables exist
    await ensureCalendarTables(db);
    
    // Initialize default calendars if needed
    if (req.applyRLS) {
      await initDefaultCalendars(db, req.clientId, req.appId);
    }
    
    // Current date in ISO format
    const now = new Date().toISOString();
    
    // Apply tenant isolation in WHERE clause
    let whereClause = 'e.start_date >= ?';
    let params = [now];
    
    if (req.applyRLS) {
      whereClause += ` AND e.client_id = ? AND e.app_id = ?`;
      params.push(req.clientId, req.appId);
    }
    
    // Query for upcoming events
    const [events] = await db.query(
      `SELECT 
         e.id,
         e.title,
         e.description,
         e.type,
         e.start_date,
         e.end_date,
         e.is_all_day,
         e.location,
         e.reminder_minutes,
         e.calendar_id,
         c.name as calendar_name,
         c.color as calendar_color,
         COUNT(DISTINCT ea.id) as attendee_count
       FROM calendar_events e
       LEFT JOIN calendars c ON e.calendar_id = c.id 
         AND c.client_id = e.client_id 
         AND c.app_id = e.app_id
       LEFT JOIN event_attendees ea ON e.id = ea.event_id 
         AND ea.client_id = e.client_id 
         AND ea.app_id = e.app_id
       WHERE ${whereClause}
       GROUP BY e.id
       ORDER BY e.start_date ASC
       LIMIT ?`,
      [...params, limit]
    );
    
    // Format the event data
    const formattedEvents = events.map(event => ({
      id: event.id,
      title: event.title,
      start: event.start_date,
      end: event.end_date,
      isAllDay: Boolean(event.is_all_day),
      type: event.type,
      location: event.location || '',
      attendeeCount: event.attendee_count,
      calendar: {
        id: event.calendar_id,
        name: event.calendar_name,
        color: event.calendar_color
      }
    }));
    
    return res.json({
      success: true,
      events: formattedEvents
    });
    
  } catch (error) {
    console.error('Error fetching upcoming events:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to fetch upcoming events',
      error: error.message
    });
  }
}

/**
 * Get a single event by ID
 */
async function getEventById(req, res) {
  try {
    const db = req.db;
    const eventId = req.params.id;
    
    // Apply tenant isolation in WHERE clause
    let whereClause = 'e.id = ?';
    let params = [eventId];
    
    if (req.applyRLS) {
      whereClause += ` AND e.client_id = ? AND e.app_id = ?`;
      params.push(req.clientId, req.appId);
    }
    
    const [events] = await db.query(
      `SELECT 
         e.id,
         e.title,
         e.description,
         e.type,
         e.start_date,
         e.end_date,
         e.is_all_day,
         e.location,
         e.reminder_minutes,
         e.calendar_id,
         c.name as calendar_name,
         c.color as calendar_color
       FROM calendar_events e
       LEFT JOIN calendars c ON e.calendar_id = c.id 
         AND c.client_id = e.client_id 
         AND c.app_id = e.app_id
       WHERE ${whereClause}`,
      params
    );
    
    if (events.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Event not found'
      });
    }
    
    const event = events[0];
    
    // Get attendees with tenant isolation
    let attendeeWhereClause = 'ea.event_id = ?';
    let attendeeParams = [eventId];
    
    if (req.applyRLS) {
      attendeeWhereClause += ` AND ea.client_id = ? AND ea.app_id = ?`;
      attendeeParams.push(req.clientId, req.appId);
    }
    
    const [attendees] = await db.query(
      `SELECT 
         u.id, 
         u.name, 
         u.email,
         ea.status
       FROM event_attendees ea
       JOIN app_users u ON ea.user_id = u.id
       WHERE ${attendeeWhereClause}`,
      attendeeParams
    );
    
    // Format the response
    const formattedEvent = {
      id: event.id,
      title: event.title,
      description: event.description || '',
      type: event.type,
      start: event.start_date,
      end: event.end_date,
      isAllDay: Boolean(event.is_all_day),
      location: event.location || '',
      reminderMinutes: event.reminder_minutes,
      calendar: {
        id: event.calendar_id,
        name: event.calendar_name,
        color: event.calendar_color
      },
      attendees: attendees.map(a => ({
        id: a.id,
        name: a.name,
        email: a.email,
        status: a.status
      }))
    };
    
    return res.json({
      success: true,
      event: formattedEvent
    });
    
  } catch (error) {
    console.error('Error fetching event details:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to fetch event details',
      error: error.message
    });
  }
}

/**
 * Create a new event
 */
async function createEvent(req, res) {
  try {
    const db = req.db;
    
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }
    
    // Ensure tables exist
    await ensureCalendarTables(db);
    
    // Initialize default calendars if needed
    if (req.applyRLS) {
      await initDefaultCalendars(db, req.clientId, req.appId);
    }
    
    const {
      title,
      description,
      type,
      startDate,
      endDate,
      isAllDay,
      location,
      reminderMinutes,
      calendarId,
      attendees
    } = req.body;
    
    // Validate required fields
    if (!title) {
      return res.status(400).json({
        success: false,
        message: 'Event title is required'
      });
    }
    
    if (!startDate) {
      return res.status(400).json({
        success: false,
        message: 'Start date is required'
      });
    }
    
    // Start a transaction
    const conn = await db.getConnection();
    await conn.beginTransaction();
    
    try {
      // Ensure valid calendar ID
      let validCalendarId = calendarId;
      
      // Apply tenant isolation to calendar lookup
      const [calendarCheck] = await conn.query(
        'SELECT id FROM calendars WHERE id = ? AND client_id = ? AND app_id = ?',
        [calendarId, req.clientId, req.appId]
      );
      
      if (calendarCheck.length === 0) {
        // Use default work calendar if specified calendar doesn't exist
        const appIdClean = req.appId.replace(/[^a-zA-Z0-9]/g, '');
        validCalendarId = `cal_${req.clientId}_${appIdClean}_work`;
      }
      
      // Insert the event with tenant isolation
      const [eventResult] = await conn.query(
        `INSERT INTO calendar_events (
           title,
           description,
           type,
           start_date,
           end_date,
           is_all_day,
           location,
           reminder_minutes,
           calendar_id,
           created_by,
           client_id,
           app_id
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          title,
          description || null,
          type || EVENT_TYPES.MEETING,
          new Date(startDate),
          endDate ? new Date(endDate) : null,
          isAllDay ? 1 : 0,
          location || null,
          reminderMinutes || null,
          validCalendarId,
          req.userId || null,
          req.clientId,
          req.appId
        ]
      );
      
      const eventId = eventResult.insertId;
      
      // Add attendees if provided
      if (attendees && Array.isArray(attendees) && attendees.length > 0) {
        for (const attendee of attendees) {
          // Check if attendee is an email or a user ID
          if (typeof attendee === 'string' && attendee.includes('@')) {
            // Handle email - check if user exists within this tenant
            const [userCheck] = await conn.query(
              'SELECT id FROM app_users WHERE email = ? AND client_id = ? AND app_id = ?',
              [attendee, req.clientId, req.appId]
            );
            
            if (userCheck.length > 0) {
              // User exists
              await conn.query(
                'INSERT INTO event_attendees (event_id, user_id, client_id, app_id) VALUES (?, ?, ?, ?)',
                [eventId, userCheck[0].id, req.clientId, req.appId]
              );
            }
          } else if (typeof attendee === 'number' || !isNaN(parseInt(attendee))) {
            // Handle user ID - verify user belongs to this tenant
            const [userCheck] = await conn.query(
              'SELECT id FROM app_users WHERE id = ? AND client_id = ? AND app_id = ?',
              [attendee, req.clientId, req.appId]
            );
            
            if (userCheck.length > 0) {
              await conn.query(
                'INSERT INTO event_attendees (event_id, user_id, client_id, app_id) VALUES (?, ?, ?, ?)',
                [eventId, attendee, req.clientId, req.appId]
              );
            }
          }
        }
      }
      
      // Commit the transaction
      await conn.commit();
      
      return res.json({
        success: true,
        message: 'Event created successfully',
        data: {
          id: eventId
        }
      });
      
    } catch (err) {
      // Rollback in case of error
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
    
  } catch (error) {
    console.error('Error creating event:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to create event',
      error: error.message
    });
  }
}

/**
 * Update an existing event
 */
async function updateEvent(req, res) {
  try {
    const db = req.db;
    
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }
    
    const eventId = req.params.id;
    const {
      title,
      description,
      type,
      startDate,
      endDate,
      isAllDay,
      location,
      reminderMinutes,
      calendarId,
      attendees
    } = req.body;
    
    // Start a transaction
    const conn = await db.getConnection();
    await conn.beginTransaction();
    
    try {
      // Check if event exists with tenant isolation
      let whereClause = 'id = ?';
      let params = [eventId];
      
      if (req.applyRLS) {
        whereClause += ' AND client_id = ? AND app_id = ?';
        params.push(req.clientId, req.appId);
      }
      
      const [eventCheck] = await conn.query(
        `SELECT id FROM calendar_events WHERE ${whereClause}`,
        params
      );
      
      if (eventCheck.length === 0) {
        return res.status(404).json({
          success: false,
          message: 'Event not found or you do not have permission to update it'
        });
      }
      
      // Update the event
      const updateFields = [];
      const updateParams = [];
      
      if (title !== undefined) {
        updateFields.push('title = ?');
        updateParams.push(title);
      }
      
      if (description !== undefined) {
        updateFields.push('description = ?');
        updateParams.push(description);
      }
      
      if (type !== undefined) {
        updateFields.push('type = ?');
        updateParams.push(type);
      }
      
      if (startDate !== undefined) {
        updateFields.push('start_date = ?');
        updateParams.push(new Date(startDate));
      }
      
      if (endDate !== undefined) {
        updateFields.push('end_date = ?');
        updateParams.push(endDate ? new Date(endDate) : null);
      }
      
      if (isAllDay !== undefined) {
        updateFields.push('is_all_day = ?');
        updateParams.push(isAllDay ? 1 : 0);
      }
      
      if (location !== undefined) {
        updateFields.push('location = ?');
        updateParams.push(location);
      }
      
      if (reminderMinutes !== undefined) {
        updateFields.push('reminder_minutes = ?');
        updateParams.push(reminderMinutes);
      }
      
      if (calendarId !== undefined) {
        // Check if calendar exists with tenant isolation
        const [calendarCheck] = await conn.query(
          'SELECT id FROM calendars WHERE id = ? AND client_id = ? AND app_id = ?',
          [calendarId, req.clientId, req.appId]
        );
        
        if (calendarCheck.length > 0) {
          updateFields.push('calendar_id = ?');
          updateParams.push(calendarId);
        }
      }
      
      if (updateFields.length > 0) {
        // Apply tenant isolation in WHERE clause
        let updateWhereClause = 'id = ?';
        let whereParams = [eventId];
        
        if (req.applyRLS) {
          updateWhereClause += ' AND client_id = ? AND app_id = ?';
          whereParams.push(req.clientId, req.appId);
        }
        
        const updateQuery = `
          UPDATE calendar_events 
          SET ${updateFields.join(', ')}
          WHERE ${updateWhereClause}
        `;
        
        await conn.query(updateQuery, [...updateParams, ...whereParams]);
      }
      
      // Update attendees if provided
      if (attendees !== undefined) {
        // Remove existing attendees with tenant isolation
        let deleteWhereClause = 'event_id = ?';
        let deleteParams = [eventId];
        
        if (req.applyRLS) {
          deleteWhereClause += ' AND client_id = ? AND app_id = ?';
          deleteParams.push(req.clientId, req.appId);
        }
        
        await conn.query(
          `DELETE FROM event_attendees WHERE ${deleteWhereClause}`, 
          deleteParams
        );
        
        // Add new attendees
        if (Array.isArray(attendees) && attendees.length > 0) {
          for (const attendee of attendees) {
            // Check if attendee is an email or a user ID
            if (typeof attendee === 'string' && attendee.includes('@')) {
              // Handle email - check if user exists within this tenant
              const [userCheck] = await conn.query(
                'SELECT id FROM app_users WHERE email = ? AND client_id = ? AND app_id = ?',
                [attendee, req.clientId, req.appId]
              );
              
              if (userCheck.length > 0) {
                // User exists
                await conn.query(
                  'INSERT INTO event_attendees (event_id, user_id, client_id, app_id) VALUES (?, ?, ?, ?)',
                  [eventId, userCheck[0].id, req.clientId, req.appId]
                );
              }
            } else if (typeof attendee === 'number' || !isNaN(parseInt(attendee))) {
              // Handle user ID - verify user belongs to this tenant
              const [userCheck] = await conn.query(
                'SELECT id FROM app_users WHERE id = ? AND client_id = ? AND app_id = ?',
                [attendee, req.clientId, req.appId]
              );
              
              if (userCheck.length > 0) {
                await conn.query(
                  'INSERT INTO event_attendees (event_id, user_id, client_id, app_id) VALUES (?, ?, ?, ?)',
                  [eventId, attendee, req.clientId, req.appId]
                );
              }
            }
          }
        }
      }
      
      // Commit the transaction
      await conn.commit();
      
      return res.json({
        success: true,
        message: 'Event updated successfully'
      });
      
    } catch (err) {
      // Rollback in case of error
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
    
  } catch (error) {
    console.error('Error updating event:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to update event',
      error: error.message
    });
  }
}

/**
 * Delete an event
 */
async function deleteEvent(req, res) {
  try {
    const db = req.db;
    const eventId = req.params.id;
    
    // Apply tenant isolation in WHERE clause
    let whereClause = 'id = ?';
    let params = [eventId];
    
    if (req.applyRLS) {
      whereClause += ' AND client_id = ? AND app_id = ?';
      params.push(req.clientId, req.appId);
    }
    
    // Check if event exists
    const [eventCheck] = await db.query(
      `SELECT id FROM calendar_events WHERE ${whereClause}`,
      params
    );
    
    if (eventCheck.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Event not found or you do not have permission to delete it'
      });
    }
    
    // Delete the event with tenant isolation
    // Note: event_attendees will be deleted via ON DELETE CASCADE constraint
    await db.query(
      `DELETE FROM calendar_events WHERE ${whereClause}`,
      params
    );
    
    return res.json({
      success: true,
      message: 'Event deleted successfully'
    });
    
  } catch (error) {
    console.error('Error deleting event:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to delete event',
      error: error.message
    });
  }
}

/**
 * Get all calendars
 */
async function getCalendars(req, res) {
  try {
    const db = req.db;
    
    // Ensure tables exist
    await ensureCalendarTables(db);
    
    // Initialize default calendars if needed
    if (req.applyRLS) {
      await initDefaultCalendars(db, req.clientId, req.appId);
    }
    
    // Apply tenant isolation in WHERE clause
    let whereClause = '1=1';
    let params = [];
    
    if (req.applyRLS) {
      whereClause = 'client_id = ? AND app_id = ?';
      params = [req.clientId, req.appId];
    }
    
    // Get user's calendar preferences with tenant isolation
    let prefWhereClause = 'user_id = ?';
    let prefParams = [req.userId || 0];
    
    if (req.applyRLS) {
      prefWhereClause += ' AND client_id = ? AND app_id = ?';
      prefParams.push(req.clientId, req.appId);
    }
    
    const [preferences] = await db.query(
      `SELECT calendar_id, active 
       FROM user_calendar_preferences 
       WHERE ${prefWhereClause}`,
      prefParams
    );
    
    // Create a map of calendar preferences
    const preferenceMap = {};
    preferences.forEach(pref => {
      preferenceMap[pref.calendar_id] = Boolean(pref.active);
    });
    
    // Get all calendars with tenant isolation
    const [calendars] = await db.query(
      `SELECT 
         id, 
         name, 
         color,
         owner_id,
         is_default
       FROM calendars
       WHERE ${whereClause}
       ORDER BY is_default DESC, name ASC`,
      params
    );
    
    // Format the calendars data
    const formattedCalendars = calendars.map(calendar => ({
      id: calendar.id,
      name: calendar.name,
      color: calendar.color,
      ownerId: calendar.owner_id,
      isDefault: Boolean(calendar.is_default),
      active: preferenceMap[calendar.id] !== undefined 
        ? preferenceMap[calendar.id] 
        : true // Default to active if no preference
    }));
    
    return res.json({
      success: true,
      calendars: formattedCalendars
    });
    
  } catch (error) {
    console.error('Error fetching calendars:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to fetch calendars',
      error: error.message
    });
  }
}

/**
 * Create a new calendar
 */
async function createCalendar(req, res) {
  try {
    const db = req.db;
    
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }
    
    // Ensure tables exist
    await ensureCalendarTables(db);
    
    const { name, color } = req.body;
    
    // Validate required fields
    if (!name) {
      return res.status(400).json({
        success: false,
        message: 'Calendar name is required'
      });
    }
    
    // Generate a unique ID with tenant prefix - FIX: Keep the ID short
    const appIdClean = req.appId ? req.appId.replace(/[^a-zA-Z0-9]/g, '') : 'default';
    // Take just the first 12 chars of the cleaned app ID
    const appIdShort = appIdClean.substring(0, 12);
    const calendarId = `cal_${req.clientId || '0'}_${appIdShort}_${Math.floor(Math.random() * 10000)}`;
    
    // Insert the new calendar with tenant isolation
    await db.query(
      `INSERT INTO calendars (
         id,
         name,
         color,
         owner_id,
         is_default,
         client_id,
         app_id
       ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        calendarId,
        name,
        color || '#4361ee', // Default to blue if no color specified
        req.userId || null,
        0, // Not a default calendar
        req.clientId,
        req.appId
      ]
    );
    
    return res.json({
      success: true,
      message: 'Calendar created successfully',
      data: {
        id: calendarId,
        name,
        color: color || '#4361ee'
      }
    });
    
  } catch (error) {
    console.error('Error creating calendar:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to create calendar',
      error: error.message
    });
  }
}

/**
 * Update a calendar
 */
async function updateCalendar(req, res) {
  try {
    const db = req.db;
    
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }
    
    const calendarId = req.params.id;
    const { name, color } = req.body;
    
    // Apply tenant isolation in WHERE clause
    let whereClause = 'id = ?';
    let params = [calendarId];
    
    if (req.applyRLS) {
      whereClause += ' AND client_id = ? AND app_id = ?';
      params.push(req.clientId, req.appId);
    }
    
    // Check if calendar exists
    const [calendarCheck] = await db.query(
      `SELECT id, is_default FROM calendars WHERE ${whereClause}`,
      params
    );
    
    if (calendarCheck.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Calendar not found or you do not have permission to update it'
      });
    }
    
    // Prevent modification of default calendars
    if (calendarCheck[0].is_default) {
      return res.status(403).json({
        success: false,
        message: 'Default calendars cannot be modified'
      });
    }
    
    // Build update query
    const updateFields = [];
    const updateParams = [];
    
    if (name !== undefined) {
      updateFields.push('name = ?');
      updateParams.push(name);
    }
    
    if (color !== undefined) {
      updateFields.push('color = ?');
      updateParams.push(color);
    }
    
    if (updateFields.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'No fields to update'
      });
    }
    
    // Update the calendar with tenant isolation
    await db.query(
      `UPDATE calendars 
       SET ${updateFields.join(', ')} 
       WHERE ${whereClause}`,
      [...updateParams, ...params]
    );
    
    return res.json({
      success: true,
      message: 'Calendar updated successfully'
    });
    
  } catch (error) {
    console.error('Error updating calendar:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to update calendar',
      error: error.message
    });
  }
}

/**
 * Delete a calendar
 */
async function deleteCalendar(req, res) {
  try {
    const db = req.db;
    const calendarId = req.params.id;
    
    // Apply tenant isolation in WHERE clause
    let whereClause = 'id = ?';
    let params = [calendarId];
    
    if (req.applyRLS) {
      whereClause += ' AND client_id = ? AND app_id = ?';
      params.push(req.clientId, req.appId);
    }
    
    // Check if calendar exists
    const [calendarCheck] = await db.query(
      `SELECT id, is_default FROM calendars WHERE ${whereClause}`,
      params
    );
    
    if (calendarCheck.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Calendar not found or you do not have permission to delete it'
      });
    }
    
    // Prevent deletion of default calendars
    if (calendarCheck[0].is_default) {
      return res.status(403).json({
        success: false,
        message: 'Default calendars cannot be deleted'
      });
    }
    
    // Start a transaction
    const conn = await db.getConnection();
    await conn.beginTransaction();
    
    try {
      // Find default work calendar to move events to
      const appIdClean = req.appId.replace(/[^a-zA-Z0-9]/g, '');
      const defaultCalendarId = `cal_${req.clientId}_${appIdClean}_work`;
      
      // Move events to default 'work' calendar
      let updateWhereClause = 'calendar_id = ?';
      let updateParams = [calendarId];
      
      if (req.applyRLS) {
        updateWhereClause += ' AND client_id = ? AND app_id = ?';
        updateParams.push(req.clientId, req.appId);
      }
      
      await conn.query(
        `UPDATE calendar_events 
         SET calendar_id = ? 
         WHERE ${updateWhereClause}`,
        [defaultCalendarId, ...updateParams]
      );
      
      // Delete calendar preferences
      let prefWhereClause = 'calendar_id = ?';
      let prefParams = [calendarId];
      
      if (req.applyRLS) {
        prefWhereClause += ' AND client_id = ? AND app_id = ?';
        prefParams.push(req.clientId, req.appId);
      }
      
      await conn.query(
        `DELETE FROM user_calendar_preferences 
         WHERE ${prefWhereClause}`,
        prefParams
      );
      
      // Delete the calendar with tenant isolation
      await conn.query(
        `DELETE FROM calendars WHERE ${whereClause}`,
        params
      );
      
      // Commit the transaction
      await conn.commit();
      
      return res.json({
        success: true,
        message: 'Calendar deleted successfully'
      });
      
    } catch (err) {
      // Rollback in case of error
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
    
  } catch (error) {
    console.error('Error deleting calendar:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to delete calendar',
      error: error.message
    });
  }
}

/**
 * Save calendar preferences
 */
async function savePreferences(req, res) {
  try {
    const db = req.db;
    
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        errors: errors.array()
      });
    }
    
    const { preferences } = req.body;
    const userId = req.userId;
    
    if (!userId) {
      return res.status(401).json({
        success: false,
        message: 'Authentication required'
      });
    }
    
    if (!preferences || !Array.isArray(preferences)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid preferences format'
      });
    }
    
    // Ensure tables exist
    await ensureCalendarTables(db);
    
    // Start a transaction
    const conn = await db.getConnection();
    await conn.beginTransaction();
    
    try {
      // Delete existing preferences with tenant isolation
      let whereClause = 'user_id = ?';
      let params = [userId];
      
      if (req.applyRLS) {
        whereClause += ' AND client_id = ? AND app_id = ?';
        params.push(req.clientId, req.appId);
      }
      
      await conn.query(
        `DELETE FROM user_calendar_preferences 
         WHERE ${whereClause}`,
        params
      );
      
      // Insert new preferences with tenant isolation
      for (const pref of preferences) {
        // Verify this calendar belongs to this tenant
        if (req.applyRLS) {
          const [calendarCheck] = await conn.query(
            'SELECT id FROM calendars WHERE id = ? AND client_id = ? AND app_id = ?',
            [pref.calendarId, req.clientId, req.appId]
          );
          
          if (calendarCheck.length === 0) {
            continue; // Skip calendars that don't belong to this tenant
          }
        }
        
        await conn.query(
          `INSERT INTO user_calendar_preferences 
           (user_id, calendar_id, active, client_id, app_id) 
           VALUES (?, ?, ?, ?, ?)`,
          [userId, pref.calendarId, pref.active ? 1 : 0, req.clientId, req.appId]
        );
      }
      
      // Commit the transaction
      await conn.commit();
      
      return res.json({
        success: true,
        message: 'Calendar preferences saved'
      });
      
    } catch (err) {
      // Rollback in case of error
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
    
  } catch (error) {
    console.error('Error saving calendar preferences:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to save calendar preferences',
      error: error.message
    });
  }
}

/**
 * Register all calendar routes
 */
function registerCalendarRoutes(app) {
  // Create a router
  const router = express.Router();

  // Event routes
  router.get('/events', getEvents);
  router.get('/events/upcoming', getUpcomingEvents);
  router.get('/events/:id', getEventById);
  
  router.post('/events', [
    body('title').notEmpty().withMessage('Title is required'),
    body('startDate').notEmpty().withMessage('Start date is required'),
    body('type').isIn(Object.values(EVENT_TYPES)).withMessage('Invalid event type'),
    body('isAllDay').isBoolean().optional(),
    body('reminderMinutes').isInt().optional()
  ], createEvent);
  
  router.put('/events/:id', [
    body('title').notEmpty().optional().withMessage('Title cannot be empty'),
    body('type').isIn(Object.values(EVENT_TYPES)).optional().withMessage('Invalid event type'),
    body('isAllDay').isBoolean().optional(),
    body('reminderMinutes').isInt().optional()
  ], updateEvent);
  
  router.delete('/events/:id', deleteEvent);
  
  // Calendar routes
  router.get('/calendars', getCalendars);
  
  router.post('/calendars', [
    body('name').notEmpty().withMessage('Calendar name is required'),
    body('color').matches(/^#[0-9A-F]{6}$/i).optional().withMessage('Color must be a valid hex color')
  ], createCalendar);
  
  router.put('/calendars/:id', [
    body('name').notEmpty().optional().withMessage('Calendar name cannot be empty'),
    body('color').matches(/^#[0-9A-F]{6}$/i).optional().withMessage('Color must be a valid hex color')
  ], updateCalendar);
  
  router.delete('/calendars/:id', deleteCalendar);
  
  // Preference routes
  router.post('/preferences', [
    body('preferences').isArray().withMessage('Preferences must be an array'),
    body('preferences.*.calendarId').notEmpty().withMessage('Calendar ID is required'),
    body('preferences.*.active').isBoolean().withMessage('Active must be a boolean')
  ], savePreferences);

  // Mount all routes under /calendar
  app.use('/api/calendar', router);
  
  // Also mount app-specific routes
  app.use('/api/apps/:appId/calendar', auth.getAppClient, router);
  
  console.log('Calendar API routes registered');
}

// Export the router and functions
module.exports = {
  registerCalendarRoutes,
  getEvents,
  getUpcomingEvents,
  getEventById,
  createEvent,
  updateEvent,
  deleteEvent,
  getCalendars,
  createCalendar,
  updateCalendar,
  deleteCalendar,
  savePreferences,
  EVENT_TYPES
};
