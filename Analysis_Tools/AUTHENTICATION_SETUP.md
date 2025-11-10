# Authentication System Setup Guide

## 🎯 Overview

A complete authentication system has been integrated into the Derivatives Analysis Dashboard with:
- **Beautiful login page** with gradient animations
- **Database-backed user authentication**
- **Default admin user** creation
- **Protected routes** (all pages require login)
- **Logout functionality** in dashboard header

## 🔐 Default Credentials

**Username:** `admin`  
**Password:** `admin123`

> ⚠️ **Security Note:** Change the default password in production using environment variables or directly in the database.

## 📊 Database Schema

The system automatically creates a `users` table with the following structure:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'user',
    full_name VARCHAR(100),
    email VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP
);
```

## 🚀 First Run

When you start the application for the first time:

1. The `users` table will be automatically created in your PostgreSQL database
2. A default admin user will be created with credentials:
   - Username: `admin`
   - Password: `admin123`

## 🎨 Features

### 1. Beautiful Login Page
- **Gradient background** with animated color shifts
- **Glassmorphism effect** on login card
- **Smooth animations** and transitions
- **Responsive design** for all screen sizes
- **Loading states** and error handling
- **Matches approved design** with Goldmine logo

### 2. Authentication Middleware
- All routes (except `/login` and `/logout`) require authentication
- Unauthenticated users are automatically redirected to login
- Session-based authentication using Flask sessions

### 3. User Management
- Password hashing using SHA256 with salt
- User roles (admin, user)
- Active/inactive user status
- Last login tracking
- User display names

### 4. Logout Functionality
- Logout button in dashboard header
- Shows current user name
- Clean session termination

## 📁 Files Modified/Created

### New Files:
- `Analysis_Tools/app/models/auth_model.py` - Authentication model with database integration
- `Analysis_Tools/app/views/login/login.html` - Beautiful login page template
- `Analysis_Tools/app/static/css/login.css` - Login page styles

### Modified Files:
- `Analysis_Tools/app/__init__.py` - Added auth blueprint and middleware
- `Analysis_Tools/app/controllers/auth_controller.py` - Updated to use database
- `Analysis_Tools/app/views/dashboard_header.html` - Added logout button and user info
- `Analysis_Tools/app/static/css/dashboard_header.css` - Added styles for user section
- `Analysis_Tools/app/controllers/stock_controller.py` - Added indices for header

## 🔧 Configuration

### Environment Variables (Optional)

You can customize the default admin credentials using environment variables:

```bash
export APP_ADMIN_USER="your_admin_username"
export APP_ADMIN_PASS="your_admin_password"
export APP_SECRET_KEY="your_secret_key_for_sessions"
```

### Changing Default Password

**Option 1: Using Environment Variables**
```bash
export APP_ADMIN_PASS="new_password"
```
Then restart the application.

**Option 2: Directly in Database**
```sql
UPDATE users 
SET password_hash = '<hashed_password>' 
WHERE username = 'admin';
```

**Option 3: Using Python**
```python
from Analysis_Tools.app.models.auth_model import update_user_password
update_user_password('admin', 'new_password')
```

## 👥 User Management Functions

The `auth_model.py` provides several functions for user management:

### Create User
```python
from Analysis_Tools.app.models.auth_model import create_user

success, message = create_user(
    username="john_doe",
    password="secure_password",
    role="user",
    full_name="John Doe",
    email="john@example.com"
)
```

### Update Password
```python
from Analysis_Tools.app.models.auth_model import update_user_password

update_user_password('admin', 'new_secure_password')
```

### Get User Info
```python
from Analysis_Tools.app.models.auth_model import get_user

user_info = get_user('admin')
print(user_info)  # Returns user dictionary or None
```

## 🛡️ Security Features

1. **Password Hashing**: All passwords are hashed using SHA256 with a salt
2. **Session Management**: Secure session handling with Flask sessions
3. **SQL Injection Protection**: All queries use parameterized statements
4. **Active User Check**: Only active users can log in
5. **Last Login Tracking**: Tracks when users last logged in

## 🎨 Design Details

### Login Page Colors
- **Background**: Animated gradient (purple to pink)
- **Card**: White with glassmorphism effect
- **Buttons**: Gradient purple matching brand colors
- **Logo**: Goldmine logo with floating animation
- **Typography**: Poppins font (matching main application)

### Header Logout Button
- **Color**: Red gradient matching table headers (#b14141 to #a31313)
- **Style**: Rounded button with hover effects
- **Position**: Right side of header next to indices

## 🔍 Testing

1. **Start the application:**
   ```bash
   python run.py
   ```

2. **Access the login page:**
   - Navigate to `http://localhost:5000/`
   - You should be redirected to `/login`

3. **Login with default credentials:**
   - Username: `admin`
   - Password: `admin123`

4. **Verify authentication:**
   - After login, you should see the dashboard
   - Your username should appear in the header
   - Logout button should be visible

5. **Test logout:**
   - Click the logout button
   - You should be redirected to login page
   - Trying to access dashboard should redirect to login

## 🐛 Troubleshooting

### Issue: "Users table not found"
**Solution:** The table is created automatically on first run. Check database connection in `db_config.py`.

### Issue: "Cannot login with admin/admin123"
**Solution:** 
1. Check if the user was created: `SELECT * FROM users;`
2. Verify database connection
3. Check application logs for errors

### Issue: "Circular import error"
**Solution:** This should not occur, but if it does, ensure all imports are correct and `get_live_indices` is imported properly.

### Issue: "Session not persisting"
**Solution:** 
1. Check `APP_SECRET_KEY` in environment or `__init__.py`
2. Ensure cookies are enabled in browser
3. Check Flask session configuration

## 📝 Notes

- The authentication system uses the same database connection as the main application (`db_config.py`)
- Password hashing uses SHA256 with the app secret key as salt
- Sessions are stored server-side using Flask's default session handling
- All user-related queries use parameterized statements to prevent SQL injection
- The system automatically initializes on first import of `auth_model.py`

## 🔐 Production Recommendations

1. **Change Default Password**: Always change the default admin password in production
2. **Use Strong Secret Key**: Set a strong `APP_SECRET_KEY` environment variable
3. **Enable HTTPS**: Use HTTPS in production to protect session cookies
4. **Regular Security Updates**: Keep Flask and dependencies updated
5. **User Management**: Implement proper user management interface for admins
6. **Password Policy**: Consider implementing password strength requirements
7. **Rate Limiting**: Add rate limiting to login endpoint to prevent brute force attacks
8. **Audit Logging**: Consider adding audit logs for user actions

---

**Created:** Authentication system integrated successfully  
**Last Updated:** Initial implementation

