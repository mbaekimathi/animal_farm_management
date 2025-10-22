from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import pymysql
import os
from datetime import datetime
import hashlib
import secrets
import socket

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-production-secret-key-change-this')

# Database configuration
# Auto-detect environment and use appropriate database settings
def is_localhost():
    """Check if the app is running on localhost"""
    try:
        # Get the hostname
        hostname = socket.gethostname()
        # Check if it's localhost or common local development names
        local_indicators = ['localhost', '127.0.0.1', 'DESKTOP', 'LAPTOP', 'MACBOOK', 'kim']
        return any(indicator.lower() in hostname.lower() for indicator in local_indicators)
    except:
        return False

# Database configuration based on environment
if is_localhost():
    # Local development settings
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'kwetufar_farm',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    
    # Database configuration without database name for initial connection
    DB_CONFIG_NO_DB = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    
    print("Running in LOCAL development mode")
    print("   Database: localhost (root user)")
else:
    # Production/cPanel settings
    DB_CONFIG = {
        'host': 'localhost',  # cPanel usually uses localhost
        'user': 'kwetufar_farm',
        'password': 'Itskimathi007',
        'database': 'kwetufar_farm',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    
    # Database configuration without database name for initial connection
    DB_CONFIG_NO_DB = {
        'host': 'localhost',  # cPanel usually uses localhost
        'user': 'kwetufar_farm',
        'password': 'Itskimathi007',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    
    print("Running in PRODUCTION mode")
    print("   Database: cPanel (kwetufar_farm user)")

def get_db_connection():
    try:
        # Add timeout and connection settings to prevent lock issues
        connection_config = DB_CONFIG.copy()
        connection_config.update({
            'connect_timeout': 60,
            'read_timeout': 60,
            'write_timeout': 60,
            'autocommit': True  # Enable autocommit to prevent lock issues
        })
        connection = pymysql.connect(**connection_config)
        return connection
    except Exception as e:
        print(f"Database connection error: {str(e)}")
        print(f"Connection details: host={DB_CONFIG['host']}, user={DB_CONFIG['user']}, database={DB_CONFIG['database']}")
        raise e

def get_db_connection_no_db():
    try:
        # Add timeout and connection settings to prevent lock issues
        connection_config = DB_CONFIG_NO_DB.copy()
        connection_config.update({
            'connect_timeout': 60,
            'read_timeout': 60,
            'write_timeout': 60,
            'autocommit': True  # Enable autocommit to prevent lock issues
        })
        connection = pymysql.connect(**connection_config)
        return connection
    except Exception as e:
        print(f"Database connection error (no db): {str(e)}")
        print(f"Connection details: host={DB_CONFIG_NO_DB['host']}, user={DB_CONFIG_NO_DB['user']}")
        raise e

def hash_password(password):
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def generate_employee_code():
    """Generate a unique 6-digit employee code"""
    return str(secrets.randbelow(900000) + 100000)

def generate_pig_tag_id(pig_type):
    """Generate a unique tag ID for pigs based on type"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the prefix based on pig type
        prefix_map = {
            'grown_pig': 'P',
            'piglet': 'S', 
            'litter': 'L',
            'batch': 'B'
        }
        prefix = prefix_map.get(pig_type, 'P')
        
        # Get the next available number for this type
        cursor.execute("""
            SELECT MAX(CAST(SUBSTRING(tag_id, 2) AS UNSIGNED)) as max_num 
            FROM pigs 
            WHERE tag_id LIKE %s
        """, (f"{prefix}%",))
        
        result = cursor.fetchone()
        next_num = (result['max_num'] or 0) + 1
        
        # Format as prefix + 3 digits (e.g., P001, S001, etc.)
        tag_id = f"{prefix}{next_num:03d}"
        
        cursor.close()
        conn.close()
        
        return tag_id
        
    except Exception as e:
        print(f"Error generating tag ID: {e}")
        import traceback
        traceback.print_exc()
        # Fallback: start from 001 if database query fails
        return f"{prefix}001"

def generate_litter_id():
    """Generate a unique sequential litter ID (L001, L002, etc.)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the next available litter number
        cursor.execute("""
            SELECT MAX(CAST(SUBSTRING(litter_id, 2) AS UNSIGNED)) as max_num 
            FROM litters 
            WHERE litter_id LIKE 'L%'
        """)
        
        result = cursor.fetchone()
        next_num = (result['max_num'] or 0) + 1
        
        # Format as L + 3 digits (e.g., L001, L002, etc.)
        litter_id = f"L{next_num:03d}"
        
        # Double-check that this ID doesn't already exist
        cursor.execute("SELECT id FROM litters WHERE litter_id = %s", (litter_id,))
        if cursor.fetchone():
            # If it exists, try the next number
            next_num += 1
            litter_id = f"L{next_num:03d}"
        
        cursor.close()
        conn.close()
        
        print(f"Generated litter ID: {litter_id}")
        return litter_id
        
    except Exception as e:
        print(f"Error generating litter ID: {e}")
        import traceback
        traceback.print_exc()
        # Fallback: start from L001 if database query fails
        return f"L001"

def calculate_expected_weight(animal_id=None, litter_id=None, weighing_date=None):
    """Calculate expected weight based on age and weight categories"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get weight categories
        cursor.execute("""
            SELECT start_age, end_age, category_name, min_weight, max_weight, daily_gain
            FROM weight_categories
            ORDER BY start_age
        """)
        categories = cursor.fetchall()
        
        if not categories:
            return None
        
        # Get animal age
        age_days = None
        if animal_id:
            cursor.execute("SELECT birth_date FROM pigs WHERE id = %s", (animal_id,))
            result = cursor.fetchone()
            if result and result['birth_date']:
                age_days = (weighing_date - result['birth_date']).days
        elif litter_id:
            cursor.execute("SELECT farrowing_date FROM litters WHERE id = %s", (litter_id,))
            result = cursor.fetchone()
            if result and result['farrowing_date']:
                age_days = (weighing_date - result['farrowing_date']).days
        
        if not age_days or age_days < 0:
            return None
        
        # Find appropriate category
        for category in categories:
            if age_days >= category['start_age'] and age_days <= category['end_age']:
                # Calculate expected weight based on daily gain
                days_in_category = age_days - category['start_age']
                expected_weight = category['min_weight'] + (days_in_category * category['daily_gain'])
                cursor.close()
                conn.close()
                return round(expected_weight, 2)
        
        cursor.close()
        conn.close()
        return None
        
    except Exception as e:
        print(f"Error calculating expected weight: {str(e)}")
        return None

def create_database_and_tables():
    """Create database and tables if they don't exist"""
    try:
        # First, connect without specifying database
        conn = get_db_connection_no_db()
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        print(f"Database '{DB_CONFIG['database']}' checked/created successfully")
        
        # Use the database
        cursor.execute(f"USE {DB_CONFIG['database']}")
        
        # Create employees table with updated structure
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id INT AUTO_INCREMENT PRIMARY KEY,
                full_name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                phone VARCHAR(20),
                employee_code VARCHAR(6) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                profile_image VARCHAR(255),
                role ENUM('administrator', 'manager', 'employee', 'vet', 'it') DEFAULT 'employee',
                status ENUM('waiting_approval', 'active', 'suspended') DEFAULT 'waiting_approval',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        print("Employees table checked/created successfully")
        
        # Create activity_log table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                employee_id INT NOT NULL,
                action VARCHAR(100) NOT NULL,
                description TEXT,
                table_name VARCHAR(50),
                record_id INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (employee_id) REFERENCES employees(id)
            )
        """)
        print("Activity log table checked/created successfully")
        
        # Create farms table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS farms (
                id INT AUTO_INCREMENT PRIMARY KEY,
                farm_name VARCHAR(100) UNIQUE NOT NULL,
                farm_location VARCHAR(255) NOT NULL,
                created_by INT NOT NULL,
                status ENUM('active', 'inactive', 'suspended') DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES employees(id)
            )
        """)
        print("Farms table checked/created successfully")
        
        # Create pigs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pigs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tag_id VARCHAR(10) UNIQUE NOT NULL,
                farm_id INT NOT NULL,
                pig_type ENUM('grown_pig', 'piglet', 'litter', 'batch') NOT NULL,
                pig_source ENUM('born', 'purchased') NOT NULL,
                breed VARCHAR(100),
                gender ENUM('male', 'female'),
                purpose ENUM('breeding', 'meat'),
                breeding_status ENUM('young', 'available', 'served', 'pregnant') DEFAULT 'young',
                birth_date DATE,
                purchase_date DATE,
                age_days INT,
                registered_by INT NOT NULL,
                status ENUM('active', 'sold', 'deceased', 'transferred', 'dead', 'slaughtered') DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (farm_id) REFERENCES farms(id),
                FOREIGN KEY (registered_by) REFERENCES employees(id)
            )
        """)
        print("Pigs table checked/created successfully")
        
        # Update pigs table status ENUM to include 'dead' and 'slaughtered'
        try:
            cursor.execute("""
                ALTER TABLE pigs 
                MODIFY COLUMN status ENUM('active', 'sold', 'deceased', 'transferred', 'dead', 'slaughtered') DEFAULT 'active'
            """)
            print("Pigs table status ENUM updated successfully")
        except Exception as e:
            print(f"Note: Pigs table status ENUM may already be updated: {e}")
        
        # Create weight_records table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weight_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                animal_id INT NULL,
                litter_id INT NULL,
                weight DECIMAL(8,2) NOT NULL,
                expected_weight DECIMAL(8,2) NULL,
                weight_type ENUM('actual', 'expected') DEFAULT 'actual',
                weighing_date DATE NOT NULL,
                weighing_time TIME,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (animal_id) REFERENCES pigs(id) ON DELETE CASCADE,
                FOREIGN KEY (litter_id) REFERENCES litters(id) ON DELETE CASCADE,
                CHECK (animal_id IS NOT NULL OR litter_id IS NOT NULL)
            )
        """)
        print("Weight records table checked/created successfully")
        
        # Create slaughter_records table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slaughter_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                pig_id INT NULL,
                litter_id INT NULL,
                pig_type ENUM('grown_pig', 'litter') NOT NULL,
                slaughter_date DATE NOT NULL,
                live_weight DECIMAL(8,2) NOT NULL,
                carcass_weight DECIMAL(8,2) NOT NULL,
                dressing_percentage DECIMAL(5,2) NOT NULL,
                meat_grade ENUM('premium', 'grade_a', 'grade_b', 'grade_c', 'standard') NOT NULL,
                price_per_kg DECIMAL(8,2) NOT NULL,
                total_revenue DECIMAL(10,2) NOT NULL,
                buyer_name VARCHAR(255) NOT NULL,
                pigs_count INT DEFAULT 1,
                notes TEXT,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (pig_id) REFERENCES pigs(id) ON DELETE CASCADE,
                FOREIGN KEY (litter_id) REFERENCES litters(id) ON DELETE CASCADE,
                FOREIGN KEY (created_by) REFERENCES employees(id),
                CHECK (pig_id IS NOT NULL OR litter_id IS NOT NULL)
            )
        """)
        print("Slaughter records table checked/created successfully")
        
        # Create dead_pigs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dead_pigs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                pig_id INT NULL,
                litter_id INT NULL,
                pig_type ENUM('grown_pig', 'litter') NOT NULL,
                death_date DATE NOT NULL,
                cause_of_death ENUM('disease', 'injury', 'old_age', 'predator_attack', 'accident', 'birth_complications', 'unknown') NOT NULL,
                weight_at_death DECIMAL(8,2) NOT NULL,
                age_at_death INT NULL,
                additional_details TEXT,
                pigs_count INT DEFAULT 1,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (pig_id) REFERENCES pigs(id) ON DELETE CASCADE,
                FOREIGN KEY (litter_id) REFERENCES litters(id) ON DELETE CASCADE,
                FOREIGN KEY (created_by) REFERENCES employees(id),
                CHECK (pig_id IS NOT NULL OR litter_id IS NOT NULL)
            )
        """)
        print("Dead pigs table checked/created successfully")
        
        # Create sale_records table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sale_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                pig_id INT NULL,
                litter_id INT NULL,
                pig_type ENUM('grown_pig', 'litter') NOT NULL,
                sale_date DATE NOT NULL,
                buyer_name VARCHAR(255) NOT NULL,
                buyer_contact VARCHAR(50),
                sale_price DECIMAL(8,2) NOT NULL,
                total_revenue DECIMAL(10,2) NOT NULL,
                notes TEXT,
                pigs_count INT DEFAULT 1,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (pig_id) REFERENCES pigs(id) ON DELETE CASCADE,
                FOREIGN KEY (litter_id) REFERENCES litters(id) ON DELETE CASCADE,
                FOREIGN KEY (created_by) REFERENCES employees(id),
                CHECK (pig_id IS NOT NULL OR litter_id IS NOT NULL)
            )
        """)
        print("Sale records table checked/created successfully")
        
        # Remove payment_method column from sale_records table if it exists
        try:
            cursor.execute("SHOW COLUMNS FROM sale_records LIKE 'payment_method'")
            if cursor.fetchone():
                cursor.execute("ALTER TABLE sale_records DROP COLUMN payment_method")
                print("Payment method column removed from sale_records table")
        except Exception as e:
            print(f"Note: Payment method column may not exist: {e}")
        
        # Update weight_records table to support litters if needed
        try:
            cursor.execute("SHOW COLUMNS FROM weight_records LIKE 'litter_id'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE weight_records ADD COLUMN litter_id INT NULL AFTER animal_id")
                cursor.execute("ALTER TABLE weight_records ADD FOREIGN KEY (litter_id) REFERENCES litters(id) ON DELETE CASCADE")
                print("Added litter_id column to weight_records table")
            
            # Make animal_id nullable to support litter-only records
            cursor.execute("ALTER TABLE weight_records MODIFY COLUMN animal_id INT NULL")
            print("Made animal_id nullable in weight_records table")
            
            # Add expected weight and weight type columns
            cursor.execute("SHOW COLUMNS FROM weight_records LIKE 'expected_weight'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE weight_records ADD COLUMN expected_weight DECIMAL(8,2) NULL AFTER weight")
                print("Added expected_weight column to weight_records table")
            
            cursor.execute("SHOW COLUMNS FROM weight_records LIKE 'weight_type'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE weight_records ADD COLUMN weight_type ENUM('actual', 'expected') DEFAULT 'actual' AFTER expected_weight")
                print("Added weight_type column to weight_records table")
            
        except Exception as e:
            print(f"Error updating weight_records table: {str(e)}")
        
        # Check if pig_source column exists, if not add it
        try:
            cursor.execute("SHOW COLUMNS FROM pigs LIKE 'pig_source'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE pigs ADD COLUMN pig_source ENUM('born', 'purchased') NOT NULL DEFAULT 'born' AFTER pig_type")
                print("Added pig_source column to pigs table")
        except Exception as e:
            print(f"Error adding pig_source column: {str(e)}")
        
        # Add missing columns to pigs table
        try:
            # Add name column
            cursor.execute("SHOW COLUMNS FROM pigs LIKE 'name'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE pigs ADD COLUMN name VARCHAR(100) AFTER tag_id")
                print("Added name column to pigs table")
            
            # Add gender column (rename from sex if exists)
            cursor.execute("SHOW COLUMNS FROM pigs LIKE 'gender'")
            if not cursor.fetchone():
                cursor.execute("SHOW COLUMNS FROM pigs LIKE 'sex'")
                if cursor.fetchone():
                    cursor.execute("ALTER TABLE pigs CHANGE sex gender ENUM('male', 'female')")
                    print("Renamed sex column to gender in pigs table")
                else:
                    cursor.execute("ALTER TABLE pigs ADD COLUMN gender ENUM('male', 'female') AFTER name")
                    print("Added gender column to pigs table")
            
            # Add birth_date column
            cursor.execute("SHOW COLUMNS FROM pigs LIKE 'birth_date'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE pigs ADD COLUMN birth_date DATE AFTER gender")
                print("Added birth_date column to pigs table")
                
        except Exception as e:
            print(f"Error adding columns to pigs table: {str(e)}")
        
        # Check if breeding_status column exists, if not add it
        try:
            cursor.execute("SHOW COLUMNS FROM pigs LIKE 'breeding_status'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE pigs ADD COLUMN breeding_status ENUM('young', 'available', 'served', 'pregnant') DEFAULT 'young' AFTER purpose")
                print("Added breeding_status column to pigs table")
            else:
                # Check if we need to update the enum values
                cursor.execute("SHOW COLUMNS FROM pigs WHERE Field = 'breeding_status'")
                column_info = cursor.fetchone()
                if column_info and 'farrowed' not in column_info['Type']:
                    # Update the enum to include new values
                    cursor.execute("ALTER TABLE pigs MODIFY COLUMN breeding_status ENUM('young', 'available', 'served', 'pregnant', 'farrowed') DEFAULT 'young'")
                    print("Updated breeding_status enum to include new values")
        except Exception as e:
                            print(f"Warning: Could not check/add breeding_status column: {e}")
        
        # Check if is_edited column exists, if not add it
        try:
            cursor.execute("SHOW COLUMNS FROM pigs LIKE 'is_edited'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE pigs ADD COLUMN is_edited BOOLEAN DEFAULT FALSE AFTER updated_at")
                print("Added is_edited column to pigs table")
        except Exception as e:
                            print(f"Warning: Could not check/add is_edited column: {e}")
        
        # Create breeding_records table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS breeding_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sow_id INT NOT NULL,
                boar_id INT NOT NULL,
                mating_date DATE NOT NULL,
                expected_due_date DATE,
                status ENUM('served', 'pregnant', 'cancelled', 'completed') DEFAULT 'served',
                notes TEXT,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (sow_id) REFERENCES pigs(id),
                FOREIGN KEY (boar_id) REFERENCES pigs(id),
                FOREIGN KEY (created_by) REFERENCES employees(id)
            )
        """)
        print("Breeding records table checked/created successfully")
        
        # Check and update breeding_records status enum if needed
        try:
            cursor.execute("SHOW COLUMNS FROM breeding_records WHERE Field = 'status'")
            column_info = cursor.fetchone()
            if column_info and 'completed' not in column_info['Type']:
                # Update the enum to include new values
                cursor.execute("ALTER TABLE breeding_records MODIFY COLUMN status ENUM('served', 'pregnant', 'cancelled', 'completed') DEFAULT 'served'")
                print("Updated breeding_records status enum to include 'completed'")
        except Exception as e:
                            print(f"Warning: Could not check/update breeding_records status enum: {e}")

        # Check and add weaning fields to farrowing_activities table if needed
        try:
            cursor.execute("SHOW COLUMNS FROM farrowing_activities WHERE Field = 'weaning_weight'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE farrowing_activities ADD COLUMN weaning_weight DECIMAL(5,2) NULL COMMENT 'Weight at weaning (for weaning activity)'")
                print("Added weaning_weight column to farrowing_activities table")
            
            cursor.execute("SHOW COLUMNS FROM farrowing_activities WHERE Field = 'weaning_date'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE farrowing_activities ADD COLUMN weaning_date DATETIME NULL COMMENT 'Date and time of weaning (for weaning activity)'")
                print("Added weaning_date column to farrowing_activities table")
            
            cursor.execute("SHOW COLUMNS FROM farrowing_activities WHERE Field = 'completed_by'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE farrowing_activities ADD COLUMN completed_by INT NULL COMMENT 'Employee who completed the activity'")
                print("Added completed_by column to farrowing_activities table")
                
                # Add foreign key constraint for completed_by
                try:
                    cursor.execute("ALTER TABLE farrowing_activities ADD CONSTRAINT fk_farrowing_activities_completed_by FOREIGN KEY (completed_by) REFERENCES employees(id)")
                    print("Added foreign key constraint for completed_by column")
                except Exception as fk_error:
                    print(f"Warning: Could not add foreign key constraint for completed_by: {fk_error}")
        except Exception as e:
                            print(f"Warning: Could not check/add weaning fields to farrowing_activities table: {e}")

        # Check and update litters status enum if needed
        try:
            cursor.execute("SHOW COLUMNS FROM litters WHERE Field = 'status'")
            column_info = cursor.fetchone()
            if column_info and 'unweaned' not in column_info['Type']:
                # First update existing 'active' records to 'unweaned'
                cursor.execute("UPDATE litters SET status = 'unweaned' WHERE status = 'active'")
                print("Updated existing 'active' litter records to 'unweaned'")
                
                # Then update the enum to include 'unweaned' and remove 'active'
                cursor.execute("ALTER TABLE litters MODIFY COLUMN status ENUM('unweaned', 'weaned', 'sold', 'deceased') DEFAULT 'unweaned'")
                print("Updated litters status enum to use 'unweaned' instead of 'active'")
        except Exception as e:
                            print(f"Warning: Could not check/update litters status enum: {e}")
        
        # Create failed_conceptions table to track failed breeding attempts
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_conceptions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sow_id INT NOT NULL,
                boar_id INT NOT NULL,
                mating_date DATE NOT NULL,
                failure_date DATE NOT NULL,
                failure_reason TEXT,
                notes TEXT,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sow_id) REFERENCES pigs(id),
                FOREIGN KEY (boar_id) REFERENCES pigs(id),
                FOREIGN KEY (created_by) REFERENCES employees(id)
            )
        """)
        print("Failed conceptions table checked/created successfully")
        
        # Create farrowing_records table to track successful farrowings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS farrowing_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                breeding_id INT NOT NULL,
                farrowing_date DATE NOT NULL,
                alive_piglets INT NOT NULL,
                still_births INT NOT NULL,
                dead_piglets INT DEFAULT 0,
                weak_piglets INT DEFAULT 0,
                avg_weight DECIMAL(5,2) NOT NULL,
                health_notes TEXT,
                notes TEXT,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (breeding_id) REFERENCES breeding_records(id),
                FOREIGN KEY (created_by) REFERENCES employees(id)
            )
        """)
        print("Farrowing records table checked/created successfully")
        
        # Add missing columns to farrowing_records table if they don't exist
        try:
            cursor.execute("ALTER TABLE farrowing_records ADD COLUMN dead_piglets INT DEFAULT 0")
            print("Added dead_piglets column to farrowing_records")
        except Exception as e:
            if "Duplicate column name" not in str(e):
                print(f"Error adding dead_piglets column: {str(e)}")
        
        try:
            cursor.execute("ALTER TABLE farrowing_records ADD COLUMN weak_piglets INT DEFAULT 0")
            print("Added weak_piglets column to farrowing_records")
        except Exception as e:
            if "Duplicate column name" not in str(e):
                print(f"Error adding weak_piglets column: {str(e)}")
        
        try:
            cursor.execute("ALTER TABLE farrowing_records ADD COLUMN notes TEXT")
            print("Added notes column to farrowing_records")
        except Exception as e:
            if "Duplicate column name" not in str(e):
                print(f"Error adding notes column: {str(e)}")
        
        try:
            cursor.execute("ALTER TABLE farrowing_records ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
            print("Added updated_at column to farrowing_records")
        except Exception as e:
            if "Duplicate column name" not in str(e):
                print(f"Error adding updated_at column: {str(e)}")
        
        # Create farrowing_activities table to track farrowing activities
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS farrowing_activities (
                id INT AUTO_INCREMENT PRIMARY KEY,
                farrowing_record_id INT NOT NULL,
                activity_name VARCHAR(100) NOT NULL,
                due_day INT NOT NULL,
                due_date DATE NOT NULL,
                completed BOOLEAN DEFAULT FALSE,
                completed_date DATETIME NULL,
                weaning_weight DECIMAL(5,2) NULL,
                weaning_date DATETIME NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (farrowing_record_id) REFERENCES farrowing_records(id)
            )
        """)
        print("Farrowing activities table checked/created successfully")
        
        # Add updated_at column to farrowing_activities if it doesn't exist
        try:
            cursor.execute("SHOW COLUMNS FROM farrowing_activities LIKE 'updated_at'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE farrowing_activities ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at")
                print("Added updated_at column to farrowing_activities table")
        except Exception as e:
            print(f"Error adding updated_at column to farrowing_activities: {str(e)}")
        
        # Create litters table to track litter information
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS litters (
                id INT AUTO_INCREMENT PRIMARY KEY,
                litter_id VARCHAR(20) UNIQUE NOT NULL,
                farrowing_record_id INT NOT NULL,
                sow_id INT NOT NULL,
                boar_id INT,
                farrowing_date DATE NOT NULL,
                total_piglets INT NOT NULL,
                alive_piglets INT NOT NULL,
                still_births INT DEFAULT 0,
                avg_weight DECIMAL(5,2),
                weaning_weight DECIMAL(5,2),
                weaning_date DATE,
                status ENUM('unweaned', 'weaned', 'sold', 'deceased', 'dead', 'slaughtered') DEFAULT 'unweaned',
                notes TEXT,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (farrowing_record_id) REFERENCES farrowing_records(id),
                FOREIGN KEY (sow_id) REFERENCES pigs(id),
                FOREIGN KEY (boar_id) REFERENCES pigs(id),
                FOREIGN KEY (created_by) REFERENCES employees(id)
            )
        """)
        print("Litters table checked/created successfully")
        
        # Update litters table status ENUM to include 'dead' and 'slaughtered'
        try:
            cursor.execute("""
                ALTER TABLE litters 
                MODIFY COLUMN status ENUM('unweaned', 'weaned', 'sold', 'deceased', 'dead', 'slaughtered') DEFAULT 'unweaned'
            """)
            print("Litters table status ENUM updated successfully")
        except Exception as e:
            print(f"Note: Litters table status ENUM may already be updated: {e}")
        
        # Create cows table
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS cows (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ear_tag VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(100),
                    breed VARCHAR(100),
                    color_markings TEXT,
                    gender ENUM('male', 'female') NOT NULL,
                    birth_date DATE,
                    age_days INT,
                    source ENUM('born', 'purchased') NOT NULL,
                    purchase_date DATE,
                    purchase_place VARCHAR(255),
                    sire_ear_tag VARCHAR(20),
                    sire_details TEXT,
                    dam_ear_tag VARCHAR(20),
                    dam_details TEXT,
                    status ENUM('active', 'sold', 'deceased', 'transferred') DEFAULT 'active',
                    registered_by INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (registered_by) REFERENCES employees(id)
                )
            """)
        print("Cows table checked/created successfully")

        # Create cow_edit_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cow_edit_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cow_id INT NOT NULL,
                field_name VARCHAR(50) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_by INT NOT NULL,
                edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (cow_id) REFERENCES cows(id) ON DELETE CASCADE,
                FOREIGN KEY (edited_by) REFERENCES employees(id)
            )
        """)
        print("Cow edit history table checked/created successfully")

        # Create milk_production table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS milk_production (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cow_id INT NOT NULL,
                production_date DATE NOT NULL,
                milking_session ENUM('morning', 'afternoon', 'evening') NOT NULL,
                milk_quantity DECIMAL(10,2) NOT NULL,
                fat_percentage DECIMAL(5,2),
                protein_percentage DECIMAL(5,2),
                milk_grade VARCHAR(50),
                milk_quality_assessment ENUM('good_quality', 'moderate_quality', 'poor_quality') DEFAULT 'moderate_quality',
                additional_notes TEXT,
                recorded_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (cow_id) REFERENCES cows(id),
                FOREIGN KEY (recorded_by) REFERENCES employees(id)
            )
        """)
        print("Milk production table checked/created successfully")

        # Create milk_production_edit_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS milk_production_edit_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                production_id INT NOT NULL,
                field_name VARCHAR(50) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_by INT NOT NULL,
                edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (production_id) REFERENCES milk_production(id) ON DELETE CASCADE,
                FOREIGN KEY (edited_by) REFERENCES employees(id)
            )
        """)
        print("Milk production edit history table checked/created successfully")

        # Create milk_sales_usage table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS milk_sales_usage (
                id INT AUTO_INCREMENT PRIMARY KEY,
                transaction_type ENUM('sale', 'usage') NOT NULL,
                transaction_date DATE NOT NULL,
                buyer VARCHAR(255),
                quantity_sold DECIMAL(10,2),
                price_per_liter DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                quantity_used DECIMAL(10,2),
                purpose_of_use ENUM('calf_feeding', 'home_consumption', 'processing', 'wastage_spoiled'),
                recorded_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (recorded_by) REFERENCES employees(id)
            )
        """)
        print("Milk sales usage table checked/created successfully")

        # Create slaughter_records_edit_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slaughter_records_edit_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                record_id INT NOT NULL,
                field_name VARCHAR(50) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_by INT NOT NULL,
                edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (record_id) REFERENCES slaughter_records(id) ON DELETE CASCADE,
                FOREIGN KEY (edited_by) REFERENCES employees(id)
            )
        """)
        print("Slaughter records edit history table checked/created successfully")

        # Create death_records_edit_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS death_records_edit_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                record_id INT NOT NULL,
                field_name VARCHAR(50) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_by INT NOT NULL,
                edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (record_id) REFERENCES dead_pigs(id) ON DELETE CASCADE,
                FOREIGN KEY (edited_by) REFERENCES employees(id)
            )
        """)
        print("Death records edit history table checked/created successfully")

        # Create sale_records_edit_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sale_records_edit_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                record_id INT NOT NULL,
                field_name VARCHAR(50) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_by INT NOT NULL,
                edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (record_id) REFERENCES sale_records(id) ON DELETE CASCADE,
                FOREIGN KEY (edited_by) REFERENCES employees(id)
            )
        """)
        print("Sale records edit history table checked/created successfully")

        # Create farrowing_records_edit_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS farrowing_records_edit_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                record_id INT NOT NULL,
                field_name VARCHAR(50) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_by INT NOT NULL,
                edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (record_id) REFERENCES farrowing_records(id) ON DELETE CASCADE,
                FOREIGN KEY (edited_by) REFERENCES employees(id)
            )
        """)
        print("Farrowing records edit history table checked/created successfully")

        # Create cow_breeding table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cow_breeding (
                id INT AUTO_INCREMENT PRIMARY KEY,
                dam_id INT NOT NULL,
                sire_id INT NOT NULL,
                breeding_date DATE NOT NULL,
                expected_calving_date DATE NOT NULL,
                pregnancy_status ENUM('served', 'conceived', 'lactating', 'available') DEFAULT 'served',
                conception_cancelled BOOLEAN DEFAULT FALSE,
                cancellation_reason TEXT,
                cancellation_date DATE,
                birth_date DATE,
                lactation_start_date DATE,
                lactation_end_date DATE,
                recorded_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (dam_id) REFERENCES cows(id),
                FOREIGN KEY (sire_id) REFERENCES cows(id),
                FOREIGN KEY (recorded_by) REFERENCES employees(id)
            )
        """)
        print("Cow breeding table checked/created successfully")

        # Check if calving_date column exists and rename it to birth_date
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'cow_breeding' 
            AND COLUMN_NAME = 'calving_date'
        """)
        
        if cursor.fetchone():
            print("Renaming calving_date column to birth_date...")
            cursor.execute("ALTER TABLE cow_breeding CHANGE COLUMN calving_date birth_date DATE")
            print("Column renamed successfully")

        # Create calves table
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS calves (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    calf_id VARCHAR(50) NOT NULL UNIQUE,
                    name VARCHAR(100),
                    breed VARCHAR(100) NOT NULL,
                    color_markings TEXT,
                    gender ENUM('male', 'female') NOT NULL,
                    birth_date DATE NOT NULL,
                    dam_id INT NOT NULL,
                    sire_id INT NOT NULL,
                    status ENUM('active', 'sold', 'deceased') DEFAULT 'active',
                    recorded_by INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (dam_id) REFERENCES cows(id),
                    FOREIGN KEY (sire_id) REFERENCES cows(id),
                    FOREIGN KEY (recorded_by) REFERENCES employees(id)
                )
            """)
            print("Calves table checked/created successfully")
        except Exception as e:
            print(f"Error creating calves table: {e}")
            raise e
        
        # Create weight_settings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weight_settings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                setting_name VARCHAR(100) NOT NULL,
                setting_value TEXT,
                setting_type ENUM('text', 'number', 'boolean', 'json') DEFAULT 'text',
                description TEXT,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES employees(id),
                UNIQUE KEY unique_setting (setting_name)
            )
        """)
        print("Weight settings table checked/created successfully")
        
        # Create weight_categories table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weight_categories (
                id INT AUTO_INCREMENT PRIMARY KEY,
                start_age INT NOT NULL,
                end_age INT NOT NULL,
                category_name VARCHAR(100) NOT NULL,
                min_weight DECIMAL(5,2) NOT NULL,
                max_weight DECIMAL(5,2) NOT NULL,
                daily_gain DECIMAL(4,2) NOT NULL,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES employees(id)
            )
        """)
        print("Weight categories table checked/created successfully")
        
        # Create vaccination_schedule table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vaccination_schedule (
                id INT AUTO_INCREMENT PRIMARY KEY,
                day_number INT NOT NULL,
                day_description VARCHAR(100),
                reason TEXT NOT NULL,
                medicine_activity TEXT NOT NULL,
                dosage_amount VARCHAR(100),
                interval_duration VARCHAR(100),
                additional_notes TEXT,
                medicine_image VARCHAR(255),
                animal_image VARCHAR(255),
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES employees(id),
                UNIQUE KEY unique_day (day_number)
            )
        """)
        print("Vaccination schedule table checked/created successfully")
        
        # Create vaccination_records table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vaccination_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                animal_id INT NOT NULL,
                animal_type ENUM('pig', 'litter', 'batch') NOT NULL,
                schedule_id INT NOT NULL,
                completed_date DATE NOT NULL,
                completion_notes TEXT,
                completed_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (schedule_id) REFERENCES vaccination_schedule(id),
                FOREIGN KEY (completed_by) REFERENCES employees(id),
                UNIQUE KEY unique_animal_schedule (animal_id, animal_type, schedule_id)
            )
        """)
        print("Vaccination records table checked/created successfully")
        
        # Check if sample data exists, if not insert it
        cursor.execute("SELECT COUNT(*) as count FROM employees")
        employee_count = cursor.fetchone()['count']
        
        # Check if weight categories exist, if not insert sample data
        cursor.execute("SELECT COUNT(*) as count FROM weight_categories")
        category_count = cursor.fetchone()['count']
        
        if category_count == 0:
            # Insert sample weight categories
            sample_categories = [
                (0, 30, 'Piglet', 1.0, 8.0, 0.2, 1),  # 0-30 days, 1-8kg, 0.2kg/day gain
                (31, 60, 'Weaner', 8.0, 20.0, 0.3, 1),  # 31-60 days, 8-20kg, 0.3kg/day gain
                (61, 120, 'Grower', 20.0, 50.0, 0.4, 1),  # 61-120 days, 20-50kg, 0.4kg/day gain
                (121, 180, 'Finisher', 50.0, 90.0, 0.5, 1),  # 121-180 days, 50-90kg, 0.5kg/day gain
                (181, 365, 'Breeder', 90.0, 150.0, 0.2, 1)  # 181-365 days, 90-150kg, 0.2kg/day gain
            ]
            
            for category in sample_categories:
                cursor.execute("""
                    INSERT INTO weight_categories (start_age, end_age, category_name, min_weight, max_weight, daily_gain, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, category)
            
            print("Sample weight categories inserted successfully")
        
        if employee_count == 0:
            try:
                # Insert sample employee data with hashed passwords
                admin_password = hash_password('admin123')
                manager_password = hash_password('manager123')
                employee_password = hash_password('employee123')
                vet_password = hash_password('vet123')
                it_password = hash_password('it123')
                
                cursor.execute("""
                    INSERT INTO employees (full_name, email, phone, employee_code, password, role, status) VALUES
                    ('John Doe', 'john.doe@farm.com', '+254700000001', '123456', %s, 'administrator', 'active'),
                    ('Jane Smith', 'jane.smith@farm.com', '+254700000002', '234567', %s, 'manager', 'active'),
                    ('Bob Wilson', 'bob.wilson@farm.com', '+254700000003', '345678', %s, 'employee', 'active'),
                    ('Dr. Sarah Johnson', 'sarah.johnson@farm.com', '+254700000004', '456789', %s, 'vet', 'active'),
                    ('Mike Tech', 'mike.tech@farm.com', '+254700000005', '567890', %s, 'it', 'active')
                """, (admin_password, manager_password, employee_password, vet_password, it_password))
                print("Sample employee data inserted successfully")
            except Exception as e:
                print(f"Error inserting employees: {e}")
                # Continue with the rest of the setup even if employees fail
        
        # Insert default weight categories if they don't exist
        cursor.execute("SELECT COUNT(*) as count FROM weight_categories")
        category_count = cursor.fetchone()['count']
        
        if category_count == 0:
            try:
                # First check if we have any employees, if not create a default one
                cursor.execute("SELECT COUNT(*) as count FROM employees")
                emp_count = cursor.fetchone()['count']
                
                if emp_count == 0:
                    # Create a default admin user
                    admin_password = hash_password('admin123')
                    cursor.execute("""
                        INSERT INTO employees (full_name, email, phone, employee_code, password, role, status) VALUES
                        ('System Admin', 'admin@farm.com', '+254700000000', '000000', %s, 'administrator', 'active')
                    """, (admin_password,))
                    print("Default admin user created")
                
                # Insert default weight categories
                cursor.execute("""
                    INSERT INTO weight_categories (start_age, end_age, category_name, min_weight, max_weight, daily_gain, created_by) VALUES
                    (1, 7, 'Neonatal', 1.5, 2.0, 0.09, 1),
                    (8, 28, 'Pre-weaning', 2.2, 8.5, 0.31, 1),
                    (29, 56, 'Nursery', 9.1, 25.0, 0.59, 1),
                    (57, 84, 'Grower', 25.7, 45.0, 0.71, 1),
                    (85, 140, 'Finisher', 45.7, 85.0, 0.71, 1),
                    (141, 180, 'Late Finisher', 85.5, 110.0, 0.63, 1)
                """)
                print("Default weight categories inserted successfully")
            except Exception as e:
                print(f"Error inserting weight categories: {e}")
                # Continue with the rest of the setup
        
        # Insert default weight settings if they don't exist
        cursor.execute("SELECT COUNT(*) as count FROM weight_settings")
        settings_count = cursor.fetchone()['count']
        
        if settings_count == 0:
            try:
                # Insert default weight settings
                cursor.execute("""
                    INSERT INTO weight_settings (setting_name, setting_value, setting_type, description, created_by) VALUES
                    ('weight_unit', 'kg', 'text', 'Default weight unit for tracking', 1),
                    ('weighing_frequency', 'weekly', 'text', 'How often pigs should be weighed', 1),
                    ('alert_threshold', '10', 'number', 'Weight change threshold for alerts (%)', 1),
                    ('auto_calculate', 'true', 'boolean', 'Automatically calculate growth rates', 1),
                    ('weight_loss_alerts', 'true', 'boolean', 'Send alerts for weight loss', 1),
                    ('data_export', 'false', 'boolean', 'Enable automatic data export', 1)
                """)
                print("Default weight settings inserted successfully")
            except Exception as e:
                print(f"Error inserting weight settings: {e}")
                # Continue with the rest of the setup
        
        # Insert vaccination schedule sample data if it doesn't exist
        cursor.execute("SELECT COUNT(*) as count FROM vaccination_schedule")
        vaccination_count = cursor.fetchone()['count']
        
        if vaccination_count == 0:
            try:
                # Get the first employee ID to use as created_by
                cursor.execute("SELECT id FROM employees ORDER BY id ASC LIMIT 1")
                employee = cursor.fetchone()
                if employee:
                    created_by_id = employee['id']
                    
                    # Insert sample vaccination schedule data
                    sample_vaccinations = [
                        (0, 'Birth', 'Prevent anemia, boost immunity. Iron deficiency is common in piglets and can lead to poor growth and development.', 'Iron injection; ensure colostrum intake. Administer 200mg iron dextran injection intramuscularly.', '200mg', 'Single dose', 'Critical for piglet survival. Monitor for injection site reactions.', created_by_id),
                        (3, 'Early development', 'Early disease protection. Young piglets are highly susceptible to respiratory diseases.', 'Mycoplasma hyopneumoniae (optional), PCV2 (some products). Administer according to farm-specific protocols.', '2ml', 'Single dose or as per protocol', 'Optional based on farm disease history and veterinary recommendation.', created_by_id),
                        (14, '2 weeks', 'Build early immunity. This is the optimal time to establish immunity against common pig diseases.', 'PCV2 (circovirus) - 1st dose; Mycoplasma - 1st dose. Administer 2ml intramuscularly in the neck region.', '2ml', 'First of two doses', 'Ensure piglets are healthy before vaccination. Monitor for any adverse reactions.', created_by_id),
                        (21, '3 weeks', 'Boost immunity at weaning. Weaning is a stressful period that can compromise immunity.', 'PRRS (if farm affected), Erysipelas (optional start). Administer according to farm PRRS status.', '2ml', 'As per farm protocol', 'PRRS vaccination depends on farm status. Consult with veterinarian for specific recommendations.', created_by_id),
                        (28, '4 weeks', 'Reinforce protection. Booster vaccinations ensure adequate immunity levels are maintained.', 'Booster: PCV2, Mycoplasma. Second dose of PCV2 and Mycoplasma vaccines to ensure complete immunity.', '2ml', 'Booster dose', 'Complete the vaccination series started at 2 weeks of age.', created_by_id),
                        (35, '5 weeks', 'Respiratory & gut disease prevention. Growing pigs are susceptible to respiratory and gastrointestinal diseases.', 'Swine influenza, Salmonella, Glässer\'s disease (risk-based). Administer based on farm disease history.', '2ml', 'Single dose or as needed', 'Risk-based vaccination. Consider farm history and seasonal disease patterns.', created_by_id),
                        (56, '8 weeks', 'Maintain health as pigs grow. Continued protection against diseases that can affect growing pigs.', 'Erysipelas booster, influenza booster (if needed). Administer 2ml intramuscularly in the neck region.', '2ml', 'Booster doses', 'Monitor for any signs of disease before vaccination.', created_by_id),
                        (90, '12 weeks', 'Grower stage - finishers. Protection during the finishing phase to ensure optimal growth.', 'Optional boosters (farm dependent). Administer based on farm-specific protocols and disease pressure.', '2ml', 'As needed', 'Farm-dependent vaccination. Consult with veterinarian for specific recommendations.', created_by_id),
                        (150, '5 months', 'Slaughter age - keep pigs healthy. Final protection before slaughter to ensure food safety.', 'Optional: Erysipelas / Salmonella booster (if outbreaks). Administer only if disease outbreaks occur.', '2ml', 'Emergency vaccination', 'Only if disease outbreaks occur. Consult with veterinarian immediately.', created_by_id),
                        (180, '6 months (Breeding)', 'Protect fertility & reproduction. Breeding animals require specific vaccinations.', 'Parvovirus, Leptospira, Erysipelas (before mating). Administer 2-4 weeks before breeding.', '2ml', 'Pre-breeding vaccination', 'Critical for reproductive health. Ensure vaccination before first breeding.', created_by_id),
                        (240, 'Pregnancy (3-5 weeks before farrowing)', 'Protect piglets via colostrum. Maternal vaccination provides passive immunity to piglets.', 'Vaccinate against E. coli, Clostridium perfringens, Rotavirus. Administer 3-5 weeks before expected farrowing.', '2ml', 'Pre-farrowing vaccination', 'Critical for piglet survival. Ensure adequate time for immunity development before farrowing.', created_by_id)
                    ]
                    
                    for vaccination in sample_vaccinations:
                        cursor.execute("""
                            INSERT INTO vaccination_schedule (day_number, day_description, reason, medicine_activity, dosage_amount, interval_duration, additional_notes, created_by)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, vaccination)
                    
                    print("Sample vaccination schedule data inserted successfully")
                else:
                    print("No employees found, skipping vaccination schedule sample data")
            except Exception as e:
                print(f"Error inserting vaccination schedule: {e}")
                # Continue with the rest of the setup
        
        # Create indexes for better performance
        try:
            cursor.execute("CREATE INDEX idx_employees_code ON employees(employee_code)")
            cursor.execute("CREATE INDEX idx_employees_email ON employees(email)")
            cursor.execute("CREATE INDEX idx_employees_status ON employees(status)")
            cursor.execute("CREATE INDEX idx_employees_role ON employees(role)")
            cursor.execute("CREATE INDEX idx_activity_log_date ON activity_log(created_at)")
            print("Database indexes created successfully")
        except Exception as e:
            # Indexes might already exist, that's okay
            pass
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Database setup completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error setting up database: {str(e)}")
        return False

def log_activity(employee_id, action, description, table_name=None, record_id=None):
    """Log employee activity"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO activity_log (employee_id, action, description, table_name, record_id)
            VALUES (%s, %s, %s, %s, %s)
        """, (employee_id, action, description, table_name, record_id))
        # No need to commit since autocommit is enabled
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error logging activity: {e}")
        # Don't raise the error to prevent breaking the main functionality

def update_pig_ages(cursor):
    """Update pig ages and breeding status based on current date"""
    try:
        # Get all pigs with birth dates
        cursor.execute("""
            SELECT id, birth_date, pig_type, purpose, breeding_status 
            FROM pigs 
            WHERE birth_date IS NOT NULL AND status = 'active'
        """)
        pigs = cursor.fetchall()
        
        updated_count = 0
        breeding_status_updates = 0
        
        for pig in pigs:
            try:
                # Calculate current age
                birth_dt = pig['birth_date']
                # Convert to date if it's datetime
                if isinstance(birth_dt, datetime):
                    birth_dt = birth_dt.date()
                current_age = (datetime.now().date() - birth_dt).days
                
                # Update age
                cursor.execute("UPDATE pigs SET age_days = %s WHERE id = %s", (current_age, pig['id']))
                
                # Update breeding status for grown pigs with breeding purpose
                if pig['pig_type'] == 'grown_pig' and pig['purpose'] == 'breeding':
                    new_breeding_status = 'available' if current_age >= 200 else 'young'
                    
                    # Only update if status changed
                    if pig['breeding_status'] != new_breeding_status:
                        cursor.execute("""
                            UPDATE pigs 
                            SET breeding_status = %s 
                            WHERE id = %s
                        """, (new_breeding_status, pig['id']))
                        breeding_status_updates += 1
                
                updated_count += 1
                
            except Exception as pig_error:
                print(f"Error updating pig {pig.get('id', 'unknown')}: {pig_error}")
                continue  # Continue with next pig instead of failing completely
        
        print(f"Updated ages for {updated_count} pigs, {breeding_status_updates} breeding status changes")
        return updated_count
        
    except Exception as e:
        print(f"Error updating pig ages: {e}")
        # Don't raise the error to prevent breaking login
        return 0

def get_role_dashboard_url(role):
    """Get the appropriate dashboard URL based on employee role"""
    role_urls = {
        'administrator': '/admin/dashboard',
        'manager': '/manager/dashboard',
        'employee': '/employee/dashboard',
        'vet': '/vet/dashboard',
        'it': '/it/dashboard'
    }
    return role_urls.get(role, '/employee/dashboard')

@app.route('/')
def landing():
    return render_template('landing.html')

@app.route('/landing')
def landing_page():
    return render_template('landing.html')

@app.route('/solutions')
def solutions():
    return render_template('solutions.html')

@app.route('/features')
def features():
    return render_template('features.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/contact')
def contact():
    return render_template('contact.html')

@app.route('/fix-db-schema')
def fix_database_schema():
    """Fix database schema issues"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Make animal_id nullable in weight_records
        cursor.execute("ALTER TABLE weight_records MODIFY COLUMN animal_id INT NULL")
        print("Made animal_id nullable in weight_records table")
        
        # Add litter_id column if it doesn't exist
        cursor.execute("SHOW COLUMNS FROM weight_records LIKE 'litter_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE weight_records ADD COLUMN litter_id INT NULL AFTER animal_id")
            cursor.execute("ALTER TABLE weight_records ADD FOREIGN KEY (litter_id) REFERENCES litters(id) ON DELETE CASCADE")
            print("Added litter_id column to weight_records table")
        
        # Add expected weight and weight type columns
        cursor.execute("SHOW COLUMNS FROM weight_records LIKE 'expected_weight'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE weight_records ADD COLUMN expected_weight DECIMAL(8,2) NULL AFTER weight")
            print("Added expected_weight column to weight_records table")
        
        cursor.execute("SHOW COLUMNS FROM weight_records LIKE 'weight_type'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE weight_records ADD COLUMN weight_type ENUM('actual', 'expected') DEFAULT 'actual' AFTER expected_weight")
            print("Added weight_type column to weight_records table")
        
        # Check if weight categories exist, if not insert sample data
        cursor.execute("SELECT COUNT(*) as count FROM weight_categories")
        category_count = cursor.fetchone()['count']
        
        if category_count == 0:
            # Insert sample weight categories
            sample_categories = [
                (0, 30, 'Piglet', 1.0, 8.0, 0.2, 1),  # 0-30 days, 1-8kg, 0.2kg/day gain
                (31, 60, 'Weaner', 8.0, 20.0, 0.3, 1),  # 31-60 days, 8-20kg, 0.3kg/day gain
                (61, 120, 'Grower', 20.0, 50.0, 0.4, 1),  # 61-120 days, 20-50kg, 0.4kg/day gain
                (121, 180, 'Finisher', 50.0, 90.0, 0.5, 1),  # 121-180 days, 50-90kg, 0.5kg/day gain
                (181, 365, 'Breeder', 90.0, 150.0, 0.2, 1)  # 181-365 days, 90-150kg, 0.2kg/day gain
            ]
            
            for category in sample_categories:
                cursor.execute("""
                    INSERT INTO weight_categories (start_age, end_age, category_name, min_weight, max_weight, daily_gain, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, category)
            
            print("Sample weight categories inserted successfully")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Database schema fixed successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error fixing database schema: {str(e)}'
        }), 500

@app.route('/test-db')
def test_database():
    """Test database connection and return status"""
    try:
        # Test connection without database
        conn_no_db = get_db_connection_no_db()
        conn_no_db.close()
        
        # Test connection with database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Test a simple query
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        # Determine environment
        environment = "LOCAL" if is_localhost() else "PRODUCTION"
        
        return jsonify({
            'success': True,
            'message': 'Database connection successful',
            'environment': environment,
            'config': {
                'host': DB_CONFIG['host'],
                'user': DB_CONFIG['user'],
                'database': DB_CONFIG['database'],
                'charset': DB_CONFIG['charset']
            },
            'test_result': result
        })
        
    except Exception as e:
        # Determine environment
        environment = "LOCAL" if is_localhost() else "PRODUCTION"
        
        return jsonify({
            'success': False,
            'message': f'Database connection failed: {str(e)}',
            'environment': environment,
            'config': {
                'host': DB_CONFIG['host'],
                'user': DB_CONFIG['user'],
                'database': DB_CONFIG['database'],
                'charset': DB_CONFIG['charset']
            },
            'error': str(e)
        }), 500

@app.route('/employee/login')
def employee_login():
    return render_template('employee_login.html')

@app.route('/employee/signup')
def employee_signup():
    return render_template('employee_signup.html')

@app.route('/employee/dashboard')
def employee_dashboard():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_dashboard.html', user=user_data)

@app.route('/employee/pig-management')
def employee_pig_management():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_pig_management.html', user=user_data)

@app.route('/employee/cow-management')
def employee_cow_management():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_cow_management.html', user=user_data)

@app.route('/employee/chicken-management')
def employee_chicken_management():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_chicken_management.html', user=user_data)

@app.route('/employee/pig-management/register')
def employee_pig_register():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_pig_register.html', user=user_data)

@app.route('/employee/cow-management/register')
def employee_cow_register():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_cow_register.html', user=user_data)

@app.route('/employee/cow-management/cow-production')
def employee_cow_production():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_cow_production.html', user=user_data)

@app.route('/employee/chicken-management/chicken-production')
def employee_chicken_production():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_chicken_production.html', user=user_data)

@app.route('/employee/chicken-management/register')
def employee_chicken_register_page():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_chicken_register.html', user=user_data)

@app.route('/admin/dashboard')
def admin_dashboard():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    # Fetch real data from database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Pigs data
        cursor.execute("""
            SELECT 
                COUNT(*) as total_pigs,
                SUM(CASE WHEN pig_type = 'grown_pig' AND gender = 'female' AND breeding_status IN ('available', 'served', 'pregnant') THEN 1 ELSE 0 END) as breeding_sows,
                SUM(CASE WHEN pig_type = 'piglet' THEN 1 ELSE 0 END) as piglets,
                SUM(CASE WHEN pig_type = 'litter' THEN 1 ELSE 0 END) as litters
            FROM pigs 
            WHERE status = 'active'
        """)
        pigs_data = cursor.fetchone()
        
        # Litter data (piglets from litters)
        cursor.execute("""
            SELECT 
                SUM(total_piglets) as total_piglets_from_litters,
                SUM(alive_piglets) as alive_piglets_from_litters,
                COUNT(*) as total_litters
            FROM litters 
            WHERE status IN ('unweaned', 'weaned')
        """)
        litter_data = cursor.fetchone()
        
        # Cows data
        cursor.execute("""
            SELECT 
                COUNT(*) as total_cows,
                SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) as female_cows,
                SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) as male_cows
            FROM cows 
            WHERE status = 'active'
        """)
        cows_data = cursor.fetchone()
        
        # Milk production data (average daily production)
        cursor.execute("""
            SELECT 
                AVG(daily_milk) as avg_daily_milk_production,
                COUNT(DISTINCT cow_id) as cows_milked,
                AVG(milk_quantity) as avg_milk_per_cow
            FROM (
                SELECT 
                    cow_id,
                    production_date,
                    SUM(milk_quantity) as daily_milk,
                    AVG(milk_quantity) as milk_quantity
                FROM milk_production 
                WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                GROUP BY cow_id, production_date
            ) as daily_totals
        """)
        milk_data = cursor.fetchone()
        
        # Upcoming activities (notifications from different departments)
        cursor.execute("""
            SELECT 
                'Breeding' as department,
                'Pigs' as animal_type,
                COUNT(*) as notification_count,
                CONCAT('Pregnant pigs due in 3 days: ', COUNT(*)) as description
            FROM pigs p
            JOIN breeding_records br ON p.id = br.sow_id
            WHERE p.status = 'active' 
            AND p.breeding_status = 'pregnant'
            AND br.expected_due_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 3 DAY)
            UNION ALL
            SELECT 
                'Breeding' as department,
                'Cows' as animal_type,
                COUNT(*) as notification_count,
                CONCAT('Pregnant cows due in 3 days: ', COUNT(*)) as description
            FROM cows c
            JOIN cow_breeding cb ON c.id = cb.dam_id
            WHERE c.status = 'active' 
            AND c.gender = 'female'
            AND cb.expected_calving_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 3 DAY)
            UNION ALL
            SELECT 
                'Health' as department,
                'Pigs' as animal_type,
                COUNT(*) as notification_count,
                'Health notifications for pigs' as description
            FROM pigs 
            WHERE status = 'active'
            UNION ALL
            SELECT 
                'Health' as department,
                'Cows' as animal_type,
                COUNT(*) as notification_count,
                'Health notifications for cows' as description
            FROM cows 
            WHERE status = 'active'
            UNION ALL
            SELECT 
                'Medical' as department,
                'Pigs' as animal_type,
                COUNT(*) as notification_count,
                'Medical notifications for pigs' as description
            FROM pigs 
            WHERE status = 'active'
            UNION ALL
            SELECT 
                'Medical' as department,
                'Cows' as animal_type,
                COUNT(*) as notification_count,
                'Medical notifications for cows' as description
            FROM cows 
            WHERE status = 'active'
            UNION ALL
            SELECT 
                'Medical' as department,
                'Chickens' as animal_type,
                0 as notification_count,
                'Medical notifications for chickens (Coming Soon)' as description
            ORDER BY department, animal_type
        """)
        upcoming_activities = cursor.fetchall()
        
        # Calculate totals
        total_animals = (pigs_data['total_pigs'] or 0) + (cows_data['total_cows'] or 0)
        total_piglets = (pigs_data['piglets'] or 0) + (litter_data['alive_piglets_from_litters'] or 0)  # piglets + alive piglets from litters
        
        # Prepare dashboard data
        dashboard_data = {
            'pigs': {
                'total_pigs': pigs_data['total_pigs'] or 0,
                'breeding_sows': pigs_data['breeding_sows'] or 0,
                'piglets': total_piglets,
                'litters': pigs_data['litters'] or 0
            },
            'cows': {
                'total_cows': cows_data['total_cows'] or 0,
                'female_cows': cows_data['female_cows'] or 0,
                'male_cows': cows_data['male_cows'] or 0,
                'avg_daily_milk_production': milk_data['avg_daily_milk_production'] or 0,
                'cows_milked': milk_data['cows_milked'] or 0,
                'avg_milk_per_cow': milk_data['avg_milk_per_cow'] or 0
            },
            'totals': {
                'total_animals': total_animals,
                'daily_production': milk_data['avg_daily_milk_production'] or 0,  # average daily milk production
                'system_health': 95  # This could be calculated based on various factors
            },
            'upcoming_activities': upcoming_activities
        }
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error fetching dashboard data: {str(e)}")
        # Fallback data in case of error
        dashboard_data = {
            'pigs': {'total_pigs': 0, 'breeding_sows': 0, 'piglets': 0, 'litters': 0},
            'cows': {'total_cows': 0, 'female_cows': 0, 'male_cows': 0, 'avg_daily_milk_production': 0, 'cows_milked': 0, 'avg_milk_per_cow': 0},
            'totals': {'total_animals': 0, 'daily_production': 0, 'system_health': 0},
            'upcoming_activities': []
        }
    
    return render_template('admin_dashboard.html', user=user_data, dashboard_data=dashboard_data)

@app.route('/manager/dashboard')
def manager_dashboard():
    if 'employee_id' not in session or session.get('employee_role') != 'manager':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('manager_dashboard.html', user=user_data)

@app.route('/api/manager/dashboard/stats', methods=['GET'])
def get_manager_dashboard_stats():
    """Get manager dashboard statistics"""
    if 'employee_id' not in session or session.get('employee_role') != 'manager':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get team members count
        cursor.execute("SELECT COUNT(*) FROM employees WHERE status = 'active' AND role != 'manager'")
        team_members = cursor.fetchone()[0]
        
        # Get active tasks count (placeholder - would need tasks table)
        active_tasks = 12  # Placeholder
        
        # Get completed tasks today (placeholder)
        completed_today = 5  # Placeholder
        
        # Get team performance (placeholder)
        team_performance = 85  # Placeholder
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'team_members': team_members,
                'active_tasks': active_tasks,
                'completed_today': completed_today,
                'team_performance': team_performance
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Failed to get dashboard stats: {str(e)}'})

@app.route('/api/manager/team/list', methods=['GET'])
def get_manager_team_list():
    """Get team members for manager"""
    if 'employee_id' not in session or session.get('employee_role') != 'manager':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all active team members (excluding managers)
        cursor.execute("""
            SELECT id, name, role, status, email, created_at
            FROM employees 
            WHERE status = 'active' AND role != 'manager'
            ORDER BY created_at DESC
        """)
        team_members = cursor.fetchall()
        
        # Convert to JSON-serializable format
        serializable_team = []
        for member in team_members:
            serializable_member = {
                'id': member[0],
                'name': member[1],
                'role': member[2],
                'status': member[3],
                'email': member[4],
                'created_at': member[5].isoformat() if member[5] else None
            }
            serializable_team.append(serializable_member)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'team_members': serializable_team
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Failed to get team list: {str(e)}'})

@app.route('/api/manager/activity/recent', methods=['GET'])
def get_manager_recent_activity():
    """Get recent team activity"""
    if 'employee_id' not in session or session.get('employee_role') != 'manager':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get recent activity from activity_log
        cursor.execute("""
            SELECT al.activity_type, al.description, al.created_at, e.name as employee_name
            FROM activity_log al
            LEFT JOIN employees e ON al.employee_id = e.id
            ORDER BY al.created_at DESC
            LIMIT 10
        """)
        activities = cursor.fetchall()
        
        # Convert to JSON-serializable format
        serializable_activities = []
        for activity in activities:
            serializable_activity = {
                'activity_type': activity[0],
                'description': activity[1],
                'created_at': activity[2].isoformat() if activity[2] else None,
                'employee_name': activity[3]
            }
            serializable_activities.append(serializable_activity)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'activities': serializable_activities
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Failed to get recent activity: {str(e)}'})

@app.route('/vet/dashboard')
def vet_dashboard():
    if 'employee_id' not in session or session.get('employee_role') != 'vet':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('vet_dashboard.html', user=user_data)

@app.route('/it/dashboard')
def it_dashboard():
    if 'employee_id' not in session or session.get('employee_role') != 'it':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('it_dashboard.html', user=user_data)

@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.get_json()
    employee_code = data.get('employee_code')
    password = data.get('password')
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if employee exists and password matches
        hashed_password = hash_password(password)
        sql = "SELECT * FROM employees WHERE employee_code = %s AND password = %s AND is_active = TRUE"
        cursor.execute(sql, (employee_code, hashed_password))
        employee = cursor.fetchone()
        
        if employee:
            # Check if employee status is active
            if employee['status'] != 'active':
                cursor.close()
                conn.close()
                if employee['status'] == 'waiting_approval':
                    return {'success': False, 'message': 'Your account is pending approval. Please contact your administrator.'}
                elif employee['status'] == 'suspended':
                    return {'success': False, 'message': 'Your account has been suspended. Please contact your administrator.'}
                else:
                    return {'success': False, 'message': 'Your account is not active. Please contact your administrator.'}
            
            # Update pig ages on login
            try:
                update_pig_ages(cursor)
                print(f"Updated pig ages for employee {employee['full_name']} login")
            except Exception as age_error:
                print(f"Warning: Could not update pig ages: {age_error}")
                # Don't fail login if age update fails
            
            # Update breeding statuses on login
            try:
                update_breeding_statuses()
                print(f"✅ Updated breeding statuses for employee {employee['full_name']} login")
            except Exception as breeding_error:
                print(f"Warning: Could not update breeding statuses: {breeding_error}")
                # Don't fail login if breeding update fails
            
            cursor.close()
            conn.close()
            
            session['employee_id'] = employee['id']
            session['employee_name'] = employee['full_name']
            session['employee_role'] = employee['role']
            session['employee_status'] = employee['status']
            
            # Log login activity
            log_activity(employee['id'], 'LOGIN', f'Employee {employee["full_name"]} logged in successfully')
            
            # Get appropriate dashboard URL based on role
            dashboard_url = get_role_dashboard_url(employee['role'])
            
            return {'success': True, 'redirect': dashboard_url}
        else:
            cursor.close()
            conn.close()
            return {'success': False, 'message': 'Invalid employee code or password'}
            
    except Exception as e:
        return {'success': False, 'message': 'Database error'}

@app.route('/api/check-employee-code', methods=['POST'])
def api_check_employee_code():
    data = request.get_json()
    employee_code = data.get('employee_code')
    
    if not employee_code:
        return {'success': False, 'message': 'Employee code is required'}
    
    if len(employee_code) != 6 or not employee_code.isdigit():
        return {'success': False, 'message': 'Employee code must be exactly 6 digits'}
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if employee code already exists
        cursor.execute("SELECT id FROM employees WHERE employee_code = %s", (employee_code,))
        existing_employee = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if existing_employee:
            return {'success': False, 'message': 'Employee code already registered'}
        else:
            return {'success': True, 'message': 'Employee code is available'}
            
    except Exception as e:
        print(f"Database error in check-employee-code: {str(e)}")
        return {'success': False, 'message': 'Database error'}

@app.route('/api/signup', methods=['POST'])
def api_signup():
    data = request.get_json()
    full_name = data.get('full_name')
    email = data.get('email')
    phone = data.get('phone')
    employee_code = data.get('employee_code')
    password = data.get('password')
    confirm_password = data.get('confirm_password')
    
    # Validation
    if not all([full_name, email, phone, employee_code, password, confirm_password]):
        return {'success': False, 'message': 'All fields are required'}
    
    if len(employee_code) != 6 or not employee_code.isdigit():
        return {'success': False, 'message': 'Employee code must be exactly 6 digits'}
    
    if password != confirm_password:
        return {'success': False, 'message': 'Passwords do not match'}
    
    if len(password) < 6:
        return {'success': False, 'message': 'Password must be at least 6 characters long'}
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if email already exists
        cursor.execute("SELECT id FROM employees WHERE email = %s", (email,))
        if cursor.fetchone():
            return {'success': False, 'message': 'Email already registered'}
        
        # Check if employee code already exists
        cursor.execute("SELECT id FROM employees WHERE employee_code = %s", (employee_code,))
        if cursor.fetchone():
            return {'success': False, 'message': 'Employee code already registered'}
        
        # Hash password and insert new employee with waiting_approval status
        hashed_password = hash_password(password)
        cursor.execute("""
            INSERT INTO employees (full_name, email, phone, employee_code, password, role, status)
            VALUES (%s, %s, %s, %s, %s, 'employee', 'waiting_approval')
        """, (full_name, email, phone, employee_code, hashed_password))
        
        employee_id = cursor.lastrowid
        
        # Log signup activity
        log_activity(employee_id, 'SIGNUP', f'New employee {full_name} registered with code {employee_code} - Status: Waiting Approval')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            'success': True, 
            'message': f'Account created successfully! Your account is pending approval from an administrator.',
            'employee_code': employee_code
        }
        
    except Exception as e:
        print(f"Database error in signup: {str(e)}")
        return {'success': False, 'message': 'Registration failed. Please try again.'}

# Profile and Settings Routes
@app.route('/profile')
def profile():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('profile.html', user=user_data)

@app.route('/settings')
def settings():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('settings.html', user=user_data)

@app.route('/admin/settings')
def admin_settings():
    """Admin Settings page - System configuration and management"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_settings.html', user=user_data)

@app.route('/app-settings')
def app_settings():
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('app_settings.html', user=user_data)

# Admin Management Routes
@app.route('/admin/role-view')
def admin_role_view():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_role_view.html', user=user_data)

@app.route('/admin/human-resource')
def admin_human_resource():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_human_resource.html', user=user_data)

@app.route('/admin/farm-management')
def admin_farm_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_management.html', user=user_data)

@app.route('/admin/analytics')
def admin_analytics():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_analytics.html', user=user_data)

# Farm Management Routes
@app.route('/admin/farm/register')
def admin_farm_register():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_register.html', user=user_data)

@app.route('/admin/farm/view/<int:farm_id>')
def admin_farm_view(farm_id):
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_view.html', user=user_data, farm_id=farm_id)

@app.route('/admin/farm/register-pigs')
def admin_farm_register_pigs():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_register_pigs.html', user=user_data)

@app.route('/admin/farm/pig-management')
def admin_farm_pig_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_pig_management.html', user=user_data)

@app.route('/admin/farm/cow-management')
def admin_farm_cow_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_cow_management.html', user=user_data)

@app.route('/admin/farm/chicken-management')
def admin_farm_chicken_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_chicken_management.html', user=user_data)

@app.route('/admin/farm/chicken-registration', methods=['POST'])
def chicken_registration():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        # Get form data
        chicken_id = request.form.get('chicken_id')
        batch_name = request.form.get('batch_name')
        chicken_type = request.form.get('chicken_type')
        breed_name = request.form.get('breed_name')
        gender = request.form.get('gender')
        hatch_date = request.form.get('hatch_date')
        age_days = request.form.get('age_days')
        source = request.form.get('source')
        coop_number = request.form.get('coop_number')
        quantity = request.form.get('quantity')
        current_status = request.form.get('current_status', 'active')
        
        # Validate required fields
        if not all([chicken_id, batch_name, chicken_type, breed_name, gender, hatch_date, source, coop_number, quantity]):
            return jsonify({'success': False, 'message': 'All required fields must be filled'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create chickens table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chickens (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chicken_id VARCHAR(20) UNIQUE NOT NULL,
                batch_name VARCHAR(100) NOT NULL,
                chicken_type ENUM('broiler', 'kienyeji', 'layer') NOT NULL,
                breed_name VARCHAR(100) NOT NULL,
                gender ENUM('male', 'female') NOT NULL,
                hatch_date DATE NOT NULL,
                age_days INT NOT NULL,
                source VARCHAR(100) NOT NULL,
                coop_number INT NOT NULL,
                quantity INT NOT NULL DEFAULT 1,
                current_status ENUM('active', 'sold', 'dead', 'culled') DEFAULT 'active',
                registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_chicken_type (chicken_type),
                INDEX idx_batch_name (batch_name),
                INDEX idx_coop_number (coop_number),
                INDEX idx_current_status (current_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Insert chicken data
        cursor.execute("""
            INSERT INTO chickens (
                chicken_id, batch_name, chicken_type, breed_name, gender, 
                hatch_date, age_days, source, coop_number, quantity, 
                current_status, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            chicken_id, batch_name, chicken_type, breed_name, gender,
            hatch_date, age_days, source, coop_number, quantity,
            current_status, session['employee_id']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Chicken {chicken_id} registered successfully!',
            'chicken_id': chicken_id
        })
        
    except Exception as e:
        print(f"Error registering chicken: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error registering chicken: {str(e)}'
        })

@app.route('/admin/farm/chicken-flock-management')
def admin_farm_chicken_flock_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    # Fetch chicken data from database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all chickens grouped by type
        cursor.execute("""
            SELECT 
                chicken_id,
                batch_name,
                chicken_type,
                breed_name,
                gender,
                hatch_date,
                age_days,
                source,
                coop_number,
                quantity,
                current_status,
                registration_date
            FROM chickens 
            WHERE current_status = 'active'
            ORDER BY chicken_type, batch_name, chicken_id
        """)
        chickens = cursor.fetchall()
        
        # Group chickens by type
        chickens_by_type = {
            'broiler': [],
            'kienyeji': [],
            'layer': []
        }
        
        for chicken in chickens:
            chickens_by_type[chicken['chicken_type']].append(chicken)
        
        # Get statistics
        cursor.execute("""
            SELECT 
                chicken_type,
                COUNT(*) as total_count,
                SUM(quantity) as total_quantity,
                AVG(age_days) as avg_age,
                COUNT(DISTINCT batch_name) as batch_count
            FROM chickens 
            WHERE current_status = 'active'
            GROUP BY chicken_type
        """)
        stats = cursor.fetchall()
        
        # Create stats dictionary
        stats_dict = {}
        for stat in stats:
            stats_dict[stat['chicken_type']] = {
                'total_count': stat['total_count'],
                'total_quantity': stat['total_quantity'],
                'avg_age': round(stat['avg_age'] or 0, 1),
                'batch_count': stat['batch_count']
            }
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error fetching chicken data: {str(e)}")
        chickens_by_type = {'broiler': [], 'kienyeji': [], 'layer': []}
        stats_dict = {}
    
    return render_template('admin_farm_chicken_flock_management.html', 
                         user=user_data, 
                         chickens_by_type=chickens_by_type,
                         stats=stats_dict)

@app.route('/admin/farm/chicken-settings')
def admin_farm_chicken_settings():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    # Fetch existing chicken stages from database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create chicken_stages table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_stages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                category ENUM('broiler', 'kienyeji', 'layer') NOT NULL,
                stage_name VARCHAR(100) NOT NULL,
                start_day INT NOT NULL,
                end_day INT NOT NULL,
                description TEXT,
                created_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_category (category),
                INDEX idx_stage_name (stage_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Create chicken_medications table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_medications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                category ENUM('broiler', 'kienyeji', 'layer') NOT NULL,
                medication_name VARCHAR(200) NOT NULL,
                start_day INT NOT NULL,
                end_day INT NOT NULL,
                purpose TEXT,
                image_filename VARCHAR(255),
                created_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_category (category),
                INDEX idx_medication_name (medication_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Create chicken_weight_standards table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_weight_standards (
                id INT AUTO_INCREMENT PRIMARY KEY,
                category ENUM('broiler', 'kienyeji', 'layer') NOT NULL,
                age_days INT NOT NULL,
                expected_weight DECIMAL(6,3) NOT NULL COMMENT 'Weight in kilograms',
                description TEXT,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_category (category),
                INDEX idx_age_days (age_days),
                UNIQUE KEY unique_category_age (category, age_days)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Get all existing stages
        cursor.execute("""
            SELECT 
                id,
                category,
                stage_name,
                start_day,
                end_day,
                description,
                created_at
            FROM chicken_stages 
            ORDER BY category, start_day
        """)
        stages = cursor.fetchall()
        
        # Get all existing medications
        cursor.execute("""
            SELECT 
                id,
                category,
                medication_name,
                start_day,
                end_day,
                purpose,
                image_filename,
                created_at
            FROM chicken_medications 
            ORDER BY category, start_day
        """)
        medications = cursor.fetchall()
        
        # Get all existing weight standards
        cursor.execute("""
            SELECT 
                id,
                category,
                age_days,
                expected_weight,
                description,
                created_at
            FROM chicken_weight_standards 
            ORDER BY category, age_days
        """)
        weight_standards = cursor.fetchall()
        
        # Group stages by category
        stages_by_category = {
            'broiler': [],
            'kienyeji': [],
            'layer': []
        }
        
        for stage in stages:
            stages_by_category[stage['category']].append(stage)
        
        # Group medications by category
        medications_by_category = {
            'broiler': [],
            'kienyeji': [],
            'layer': []
        }
        
        for medication in medications:
            medications_by_category[medication['category']].append(medication)
        
        # Group weight standards by category
        weight_standards_by_category = {
            'broiler': [],
            'kienyeji': [],
            'layer': []
        }
        
        for weight_standard in weight_standards:
            weight_standards_by_category[weight_standard['category']].append(weight_standard)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error fetching chicken settings data: {str(e)}")
        # Initialize variables in case of error
        stages_by_category = {'broiler': [], 'kienyeji': [], 'layer': []}
        medications_by_category = {'broiler': [], 'kienyeji': [], 'layer': []}
        weight_standards_by_category = {'broiler': [], 'kienyeji': [], 'layer': []}
    
    return render_template('admin_farm_chicken_settings.html',
                         user=user_data,
                         stages_by_category=stages_by_category,
                         medications_by_category=medications_by_category,
                         weight_standards_by_category=weight_standards_by_category)

@app.route('/admin/farm/chicken-stage', methods=['POST'])
def add_chicken_stage():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        # Get form data
        category = request.form.get('category')
        stage_name = request.form.get('stage_name')
        start_day = int(request.form.get('start_day'))
        end_day = int(request.form.get('end_day'))
        description = request.form.get('description', '')
        
        # Validate required fields
        if not all([category, stage_name, start_day, end_day]):
            return jsonify({'success': False, 'message': 'All required fields must be filled'})
        
        # Validate day range
        if start_day >= end_day:
            return jsonify({'success': False, 'message': 'End day must be greater than start day'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for overlapping stages
        cursor.execute("""
            SELECT COUNT(*) as count FROM chicken_stages 
            WHERE category = %s 
            AND ((start_day <= %s AND end_day > %s) OR (start_day < %s AND end_day >= %s))
        """, (category, start_day, start_day, end_day, end_day))
        
        overlap_check = cursor.fetchone()
        if overlap_check['count'] > 0:
            return jsonify({'success': False, 'message': 'Stage overlaps with existing stage in this category'})
        
        # Insert new stage
        cursor.execute("""
            INSERT INTO chicken_stages (category, stage_name, start_day, end_day, description, created_by)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (category, stage_name, start_day, end_day, description, session['employee_id']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Stage "{stage_name}" added successfully for {category} category!'
        })
        
    except Exception as e:
        print(f"Error adding chicken stage: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error adding stage: {str(e)}'
        })

@app.route('/admin/farm/chicken-stage/<int:stage_id>', methods=['PUT'])
def update_chicken_stage(stage_id):
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        # Get form data
        category = request.form.get('category')
        stage_name = request.form.get('stage_name')
        start_day = int(request.form.get('start_day'))
        end_day = int(request.form.get('end_day'))
        description = request.form.get('description', '')
        
        # Validate required fields
        if not all([category, stage_name, start_day, end_day]):
            return jsonify({'success': False, 'message': 'All required fields must be filled'})
        
        # Validate day range
        if start_day >= end_day:
            return jsonify({'success': False, 'message': 'End day must be greater than start day'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for overlapping stages (excluding current stage)
        cursor.execute("""
            SELECT COUNT(*) as count FROM chicken_stages 
            WHERE category = %s 
            AND id != %s
            AND ((start_day <= %s AND end_day > %s) OR (start_day < %s AND end_day >= %s))
        """, (category, stage_id, start_day, start_day, end_day, end_day))
        
        overlap_check = cursor.fetchone()
        if overlap_check['count'] > 0:
            return jsonify({'success': False, 'message': 'Stage overlaps with existing stage in this category'})
        
        # Update stage
        cursor.execute("""
            UPDATE chicken_stages 
            SET category = %s, stage_name = %s, start_day = %s, end_day = %s, description = %s
            WHERE id = %s
        """, (category, stage_name, start_day, end_day, description, stage_id))
        
        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'Stage not found'})
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Stage "{stage_name}" updated successfully!'
        })
        
    except Exception as e:
        print(f"Error updating chicken stage: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error updating stage: {str(e)}'
        })

@app.route('/admin/farm/chicken-stage/<int:stage_id>', methods=['DELETE'])
def delete_chicken_stage(stage_id):
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get stage name before deletion for confirmation message
        cursor.execute("SELECT stage_name FROM chicken_stages WHERE id = %s", (stage_id,))
        stage = cursor.fetchone()
        
        if not stage:
            return jsonify({'success': False, 'message': 'Stage not found'})
        
        # Delete stage
        cursor.execute("DELETE FROM chicken_stages WHERE id = %s", (stage_id,))
        
        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'Stage not found'})
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Stage "{stage["stage_name"]}" deleted successfully!'
        })
        
    except Exception as e:
        print(f"Error deleting chicken stage: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error deleting stage: {str(e)}'
        })

@app.route('/admin/farm/chicken-medication', methods=['POST'])
def add_chicken_medication():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        # Get form data
        category = request.form.get('category')
        medication_name = request.form.get('medication_name')
        start_day = int(request.form.get('start_day'))
        end_day = int(request.form.get('end_day'))
        purpose = request.form.get('purpose', '')
        
        # Handle file upload
        medication_image = request.files.get('medication_image')
        image_filename = None
        
        if medication_image and medication_image.filename:
            # Create uploads directory if it doesn't exist
            import os
            upload_dir = 'static/uploads/medications'
            os.makedirs(upload_dir, exist_ok=True)
            
            # Generate unique filename
            import uuid
            file_extension = medication_image.filename.rsplit('.', 1)[1].lower() if '.' in medication_image.filename else 'jpg'
            image_filename = f"{uuid.uuid4().hex}.{file_extension}"
            
            # Save file
            medication_image.save(os.path.join(upload_dir, image_filename))
        
        # Validate required fields
        if not all([category, medication_name, start_day, end_day]):
            return jsonify({'success': False, 'message': 'All required fields must be filled'})
        
        # Validate day range
        if start_day >= end_day:
            return jsonify({'success': False, 'message': 'End day must be greater than start day'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create chicken_medications table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_medications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                category ENUM('broiler', 'kienyeji', 'layer') NOT NULL,
                medication_name VARCHAR(200) NOT NULL,
                start_day INT NOT NULL,
                end_day INT NOT NULL,
                purpose TEXT,
                image_filename VARCHAR(255),
                created_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_category (category),
                INDEX idx_medication_name (medication_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Insert medication data
        cursor.execute("""
            INSERT INTO chicken_medications (category, medication_name, start_day, end_day, purpose, image_filename, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (category, medication_name, start_day, end_day, purpose, image_filename, session['employee_id']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Medication "{medication_name}" added successfully for {category} category!'
        })
        
    except Exception as e:
        print(f"Error adding chicken medication: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error adding medication: {str(e)}'
        })

@app.route('/admin/farm/chicken-medication/<int:medication_id>', methods=['PUT'])
def update_chicken_medication(medication_id):
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        # Get form data
        category = request.form.get('category')
        medication_name = request.form.get('medication_name')
        start_day = int(request.form.get('start_day'))
        end_day = int(request.form.get('end_day'))
        purpose = request.form.get('purpose', '')
        
        # Handle file upload
        medication_image = request.files.get('medication_image')
        image_filename = None
        
        if medication_image and medication_image.filename:
            # Create uploads directory if it doesn't exist
            import os
            upload_dir = 'static/uploads/medications'
            os.makedirs(upload_dir, exist_ok=True)
            
            # Generate unique filename
            import uuid
            file_extension = medication_image.filename.rsplit('.', 1)[1].lower() if '.' in medication_image.filename else 'jpg'
            image_filename = f"{uuid.uuid4().hex}.{file_extension}"
            
            # Save file
            medication_image.save(os.path.join(upload_dir, image_filename))
        
        # Validate required fields
        if not all([category, medication_name, start_day, end_day]):
            return jsonify({'success': False, 'message': 'All required fields must be filled'})
        
        # Validate day range
        if start_day >= end_day:
            return jsonify({'success': False, 'message': 'End day must be greater than start day'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Update medication (only update image if new one provided)
        if image_filename:
            cursor.execute("""
                UPDATE chicken_medications 
                SET category = %s, medication_name = %s, start_day = %s, end_day = %s, 
                    purpose = %s, image_filename = %s
                WHERE id = %s
            """, (category, medication_name, start_day, end_day, purpose, image_filename, medication_id))
        else:
            cursor.execute("""
                UPDATE chicken_medications 
                SET category = %s, medication_name = %s, start_day = %s, end_day = %s, purpose = %s
                WHERE id = %s
            """, (category, medication_name, start_day, end_day, purpose, medication_id))
        
        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'Medication not found'})
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Medication "{medication_name}" updated successfully!'
        })
        
    except Exception as e:
        print(f"Error updating chicken medication: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error updating medication: {str(e)}'
        })

@app.route('/admin/farm/chicken-medication/<int:medication_id>', methods=['DELETE'])
def delete_chicken_medication(medication_id):
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get medication name before deletion for confirmation message
        cursor.execute("SELECT medication_name FROM chicken_medications WHERE id = %s", (medication_id,))
        medication = cursor.fetchone()
        
        if not medication:
            return jsonify({'success': False, 'message': 'Medication not found'})
        
        # Delete medication
        cursor.execute("DELETE FROM chicken_medications WHERE id = %s", (medication_id,))
        
        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'Medication not found'})
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Medication "{medication["medication_name"]}" deleted successfully!'
        })
        
    except Exception as e:
        print(f"Error deleting chicken medication: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error deleting medication: {str(e)}'
        })

@app.route('/admin/farm/chicken-health')
def admin_farm_chicken_health():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    # Fetch chicken data and stages from database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all active chickens
        cursor.execute("""
            SELECT 
                chicken_id,
                batch_name,
                chicken_type,
                breed_name,
                gender,
                hatch_date,
                age_days,
                source,
                coop_number,
                quantity,
                current_status,
                registration_date
            FROM chickens 
            WHERE current_status = 'active'
            ORDER BY chicken_type, age_days
        """)
        chickens = cursor.fetchall()
        
        # Get all chicken stages
        cursor.execute("""
            SELECT 
                id,
                category,
                stage_name,
                start_day,
                end_day,
                description
            FROM chicken_stages 
            ORDER BY category, start_day
        """)
        stages = cursor.fetchall()
        
        # Group chickens by type and then by stage
        chickens_by_category_and_stage = {
            'broiler': {},
            'kienyeji': {},
            'layer': {}
        }
        
        # Create stage lookup for each category
        stage_lookup = {'broiler': {}, 'kienyeji': {}, 'layer': {}}
        for stage in stages:
            stage_lookup[stage['category']][stage['id']] = stage
        
        # Group chickens by category first
        chickens_by_type = {'broiler': [], 'kienyeji': [], 'layer': []}
        for chicken in chickens:
            chickens_by_type[chicken['chicken_type']].append(chicken)
        
        # For each category, group chickens by their current stage
        for category in ['broiler', 'kienyeji', 'layer']:
            category_chickens = chickens_by_type[category]
            category_stages = stage_lookup[category]
            
            # Initialize stage groups
            stage_groups = {}
            for stage_id, stage in category_stages.items():
                stage_groups[stage_id] = {
                    'stage_info': stage,
                    'chickens': []
                }
            
            # Add "No Stage" group for chickens that don't fit any stage
            stage_groups['no_stage'] = {
                'stage_info': {
                    'stage_name': 'No Stage',
                    'start_day': 0,
                    'end_day': 0,
                    'description': 'Chickens that do not fit into any defined stage'
                },
                'chickens': []
            }
            
            # Assign chickens to appropriate stages
            for chicken in category_chickens:
                assigned = False
                chicken_age = chicken['age_days']
                
                # Check if chicken fits into any defined stage
                for stage_id, stage in category_stages.items():
                    if stage['start_day'] <= chicken_age <= stage['end_day']:
                        stage_groups[stage_id]['chickens'].append(chicken)
                        assigned = True
                        break
                
                # If not assigned to any stage, put in "No Stage"
                if not assigned:
                    stage_groups['no_stage']['chickens'].append(chicken)
            
            chickens_by_category_and_stage[category] = stage_groups
        
        # Get statistics
        cursor.execute("""
            SELECT 
                chicken_type,
                COUNT(*) as total_count,
                SUM(quantity) as total_quantity,
                AVG(age_days) as avg_age,
                COUNT(DISTINCT batch_name) as batch_count
            FROM chickens 
            WHERE current_status = 'active'
            GROUP BY chicken_type
        """)
        stats = cursor.fetchall()
        
        # Create stats dictionary
        stats_dict = {}
        for stat in stats:
            stats_dict[stat['chicken_type']] = {
                'total_count': stat['total_count'],
                'total_quantity': stat['total_quantity'],
                'avg_age': round(stat['avg_age'] or 0, 1),
                'batch_count': stat['batch_count']
            }
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error fetching chicken health data: {str(e)}")
        chickens_by_category_and_stage = {
            'broiler': {},
            'kienyeji': {},
            'layer': {}
        }
        stats_dict = {}
    
    return render_template('admin_farm_chicken_health.html', 
                         user=user_data, 
                         chickens_by_category_and_stage=chickens_by_category_and_stage,
                         stats=stats_dict)

@app.route('/admin/farm/chicken-upcoming-medications')
def admin_farm_chicken_upcoming_medications():
    print("DEBUG: Route accessed - /admin/farm/chicken-upcoming-medications")
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    # Initialize variables
    stages_by_category = {'broiler': [], 'kienyeji': [], 'layer': []}
    medications_by_category = {'broiler': [], 'kienyeji': [], 'layer': []}
    weight_standards_by_category = {'broiler': [], 'kienyeji': [], 'layer': []}
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all active chickens
        cursor.execute("""
            SELECT 
                chicken_id,
                batch_name,
                chicken_type,
                breed_name,
                gender,
                hatch_date,
                age_days,
                source,
                coop_number,
                quantity,
                current_status,
                registration_date
            FROM chickens 
            WHERE current_status = 'active'
            ORDER BY chicken_type, age_days
        """)
        chickens = cursor.fetchall()
        
        # Get all medications
        cursor.execute("""
            SELECT 
                id,
                category,
                medication_name,
                start_day,
                end_day,
                purpose,
                image_filename,
                created_at
            FROM chicken_medications 
            ORDER BY category, start_day
        """)
        medications = cursor.fetchall()
        
        # Get all weight standards
        cursor.execute("""
            SELECT 
                id,
                category,
                age_days,
                expected_weight,
                description,
                created_at
            FROM chicken_weight_standards 
            ORDER BY category, age_days
        """)
        weight_standards = cursor.fetchall()
        
        # Create medication tracking table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_medication_tracking (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chicken_id VARCHAR(20) NOT NULL,
                medication_id INT NOT NULL,
                scheduled_date DATE NOT NULL,
                completed_date DATE NULL,
                status ENUM('pending', 'completed', 'missed') DEFAULT 'pending',
                notes TEXT,
                administered_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_medication_id (medication_id),
                INDEX idx_status (status),
                INDEX idx_scheduled_date (scheduled_date),
                FOREIGN KEY (medication_id) REFERENCES chicken_medications(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Get existing medication tracking records
        cursor.execute("""
            SELECT 
                cmt.id,
                cmt.chicken_id,
                cmt.medication_id,
                cmt.scheduled_date,
                cmt.completed_date,
                cmt.status,
                cmt.notes,
                cm.medication_name,
                cm.category,
                cm.purpose
            FROM chicken_medication_tracking cmt
            JOIN chicken_medications cm ON cmt.medication_id = cm.id
            ORDER BY cmt.scheduled_date, cmt.chicken_id
        """)
        tracking_records = cursor.fetchall()
        
        # Group chickens by type
        chickens_by_type = {'broiler': [], 'kienyeji': [], 'layer': []}
        for chicken in chickens:
            chickens_by_type[chicken['chicken_type']].append(chicken)
        
        # Group medications by category
        medications_by_category = {'broiler': [], 'kienyeji': [], 'layer': []}
        for medication in medications:
            medications_by_category[medication['category']].append(medication)
        
        # Group weight standards by category
        weight_standards_by_category = {'broiler': [], 'kienyeji': [], 'layer': []}
        for weight_standard in weight_standards:
            weight_standards_by_category[weight_standard['category']].append(weight_standard)
        
        # Create medications with their chickens list
        medications_with_chickens = []
        
        for medication in medications:
            eligible_chickens = []
            
            # Find all chickens that need this medication
            for chicken in chickens:
                chicken_age = chicken['age_days']
                
                # Check if this medication is already tracked and completed for this chicken
                existing_tracking = None
                for tracking in tracking_records:
                    if (tracking['chicken_id'] == chicken['chicken_id'] and 
                        tracking['medication_id'] == medication['id']):
                        existing_tracking = tracking
                        break
                
                # Only include if not completed (regardless of age)
                if not existing_tracking or existing_tracking['status'] != 'completed':
                    # Calculate status and urgency
                    if chicken_age <= medication['end_day']:
                        # Chicken is still eligible (within or before the medication period)
                        days_remaining = medication['end_day'] - chicken_age
                        urgency = 'high' if days_remaining <= 2 else 'medium' if days_remaining <= 5 else 'low'
                        status = 'eligible'
                    else:
                        # Chicken is past the medication period but still show it
                        days_overdue = chicken_age - medication['end_day']
                        urgency = 'overdue'
                        status = 'overdue'
                    
                    eligible_chickens.append({
                        'chicken': chicken,
                        'tracking': existing_tracking,
                        'days_remaining': medication['end_day'] - chicken_age,
                        'days_overdue': max(0, chicken_age - medication['end_day']),
                        'urgency': urgency,
                        'status': status
                    })
            
            # Only add medication if it has eligible chickens
            if eligible_chickens:
                # Calculate overall urgency for this medication
                has_overdue = any(chicken['status'] == 'overdue' for chicken in eligible_chickens)
                has_high_priority = any(chicken['urgency'] == 'high' for chicken in eligible_chickens)
                
                if has_overdue:
                    overall_urgency = 'overdue'
                elif has_high_priority:
                    overall_urgency = 'high'
                else:
                    overall_urgency = 'medium'
                
                medications_with_chickens.append({
                    'medication': medication,
                    'chickens': eligible_chickens,
                    'total_chickens': len(eligible_chickens),
                    'overdue_count': len([c for c in eligible_chickens if c['status'] == 'overdue']),
                    'eligible_count': len([c for c in eligible_chickens if c['status'] == 'eligible']),
                    'overall_urgency': overall_urgency
                })
        
        # Sort by urgency
        medications_with_chickens.sort(key=lambda x: (
            0 if x['overall_urgency'] == 'overdue' else 1 if x['overall_urgency'] == 'high' else 2,
            x['medication']['medication_name']
        ))
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error fetching upcoming medications data: {str(e)}")
        medications_with_chickens = []
    
    return render_template('admin_farm_chicken_upcoming_medications.html',
                         user=user_data,
                         medications_with_chickens=medications_with_chickens)

@app.route('/admin/farm/chicken-health-analytics')
def admin_farm_chicken_health_analytics():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all active chickens grouped by category
        cursor.execute("""
            SELECT 
                chicken_id,
                batch_name,
                chicken_type,
                breed_name,
                gender,
                hatch_date,
                age_days,
                source,
                coop_number,
                quantity,
                current_status,
                registration_date
            FROM chickens 
            WHERE current_status = 'active'
            ORDER BY chicken_type, age_days
        """)
        chickens = cursor.fetchall()
        
        # Group chickens by type
        chickens_by_type = {'broiler': [], 'kienyeji': [], 'layer': []}
        for chicken in chickens:
            chickens_by_type[chicken['chicken_type']].append(chicken)
        
        # Calculate statistics for each category
        category_stats = {}
        for category in ['broiler', 'kienyeji', 'layer']:
            category_chickens = chickens_by_type[category]
            if category_chickens:
                ages = [chicken['age_days'] for chicken in category_chickens]
                category_stats[category] = {
                    'total': len(category_chickens),
                    'avg_age': sum(ages) / len(ages) if ages else 0,
                    'min_age': min(ages) if ages else 0,
                    'max_age': max(ages) if ages else 0,
                    'males': len([c for c in category_chickens if c['gender'] == 'male']),
                    'females': len([c for c in category_chickens if c['gender'] == 'female'])
                }
            else:
                category_stats[category] = {
                    'total': 0,
                    'avg_age': 0,
                    'min_age': 0,
                    'max_age': 0,
                    'males': 0,
                    'females': 0
                }
        
    except Exception as e:
        print(f"Error fetching chicken health analytics data: {str(e)}")
        chickens_by_type = {'broiler': [], 'kienyeji': [], 'layer': []}
        category_stats = {
            'broiler': {'total': 0, 'avg_age': 0, 'min_age': 0, 'max_age': 0, 'males': 0, 'females': 0},
            'kienyeji': {'total': 0, 'avg_age': 0, 'min_age': 0, 'max_age': 0, 'males': 0, 'females': 0},
            'layer': {'total': 0, 'avg_age': 0, 'min_age': 0, 'max_age': 0, 'males': 0, 'females': 0}
        }
    
    # Get medication analytics data
    try:
        # Get medication completion statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_medications,
                SUM(CASE WHEN cmt.status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN cmt.status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN cmt.status = 'overdue' THEN 1 ELSE 0 END) as overdue
            FROM chicken_medication_tracking cmt
        """)
        medication_stats = cursor.fetchone()
        
        # Get weight performance by category
        cursor.execute("""
            SELECT 
                c.chicken_type,
                AVG(cwt.weight_percentage) as avg_performance,
                COUNT(cwt.id) as total_checks,
                SUM(CASE WHEN cwt.weight_category = 'healthy' THEN 1 ELSE 0 END) as healthy_count,
                SUM(CASE WHEN cwt.weight_category = 'underweight' THEN 1 ELSE 0 END) as underweight_count,
                SUM(CASE WHEN cwt.weight_category = 'overweight' THEN 1 ELSE 0 END) as overweight_count
            FROM chicken_weight_tracking cwt
            JOIN chickens c ON cwt.chicken_id = c.chicken_id
            GROUP BY c.chicken_type
        """)
        weight_stats = cursor.fetchall()
        
        # Get category performance metrics
        cursor.execute("""
            SELECT 
                chicken_type,
                COUNT(*) as total_chickens,
                AVG(age_days) as avg_age,
                SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) as males,
                SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) as females
            FROM chickens 
            WHERE current_status = 'active'
            GROUP BY chicken_type
        """)
        category_performance = cursor.fetchall()
        
        # Get weight tracking data for line charts (last 7 days by category)
        cursor.execute("""
            SELECT 
                c.chicken_type,
                DATE(cwt.checked_at) as check_date,
                AVG(cwt.actual_weight) as avg_actual_weight,
                AVG(cwt.expected_weight) as avg_expected_weight,
                AVG(cwt.weight_percentage) as avg_performance
            FROM chicken_weight_tracking cwt
            JOIN chickens c ON cwt.chicken_id = c.chicken_id
            WHERE cwt.checked_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            GROUP BY c.chicken_type, DATE(cwt.checked_at)
            ORDER BY c.chicken_type, check_date
        """)
        weight_tracking_data = cursor.fetchall()
        
    except Exception as e:
        print(f"Error fetching analytics data: {str(e)}")
        medication_stats = {'total_medications': 0, 'completed': 0, 'pending': 0, 'overdue': 0}
        weight_stats = []
        category_performance = []
        weight_tracking_data = []
    
    finally:
        # Always close connections in finally block
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    # Ensure we have default data for all categories
    if not weight_stats:
        weight_stats = [
            {'chicken_type': 'broiler', 'avg_performance': 0, 'total_checks': 0, 'healthy_count': 0, 'underweight_count': 0, 'overweight_count': 0},
            {'chicken_type': 'kienyeji', 'avg_performance': 0, 'total_checks': 0, 'healthy_count': 0, 'underweight_count': 0, 'overweight_count': 0},
            {'chicken_type': 'layer', 'avg_performance': 0, 'total_checks': 0, 'healthy_count': 0, 'underweight_count': 0, 'overweight_count': 0}
        ]
    
    if not category_performance:
        category_performance = [
            {'chicken_type': 'broiler', 'total_chickens': 0, 'avg_age': 0, 'males': 0, 'females': 0},
            {'chicken_type': 'kienyeji', 'total_chickens': 0, 'avg_age': 0, 'males': 0, 'females': 0},
            {'chicken_type': 'layer', 'total_chickens': 0, 'avg_age': 0, 'males': 0, 'females': 0}
        ]
    
    return render_template('admin_farm_chicken_health_analytics.html',
                         user=user_data,
                         chickens_by_type=chickens_by_type,
                         category_stats=category_stats,
                         medication_stats=medication_stats,
                         weight_stats=weight_stats,
                         category_performance=category_performance,
                         weight_tracking_data=weight_tracking_data)

@app.route('/admin/farm/chicken-production')
def admin_farm_chicken_production():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_chicken_production.html', user=user_data)

@app.route('/admin/farm/chicken-production-management')
def admin_farm_chicken_production_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    # Fetch production data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create audit table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_production_audit (
                id INT AUTO_INCREMENT PRIMARY KEY,
                production_id INT NOT NULL,
                chicken_id VARCHAR(50) NOT NULL,
                production_type ENUM('eggs', 'meat') NOT NULL,
                field_name VARCHAR(50) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                edited_by INT NOT NULL,
                edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_production_id (production_id),
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_edited_at (edited_at)
            )
        """)
        
        # Get all production records with chicken details and edit status
        cursor.execute("""
            SELECT 
                cp.id,
                cp.production_type,
                cp.chicken_id,
                cp.chicken_category,
                cp.production_date,
                cp.production_time,
                cp.quantity,
                cp.notes,
                cp.created_at,
                c.batch_name,
                c.breed_name,
                c.coop_number,
                CASE WHEN EXISTS(SELECT 1 FROM chicken_production_audit WHERE production_id = cp.id) THEN 1 ELSE 0 END as is_edited
            FROM chicken_production cp
            LEFT JOIN chickens c ON cp.chicken_id COLLATE utf8mb4_unicode_ci = c.chicken_id COLLATE utf8mb4_unicode_ci
            ORDER BY cp.created_at DESC
        """)
        productions = cursor.fetchall()
        
        # Convert timedelta to time string for display
        for production in productions:
            if production['production_time']:
                if hasattr(production['production_time'], 'strftime'):
                    # It's already a time object
                    production['production_time_str'] = production['production_time'].strftime('%H:%M')
                else:
                    # It's a timedelta object, convert to time string
                    total_seconds = int(production['production_time'].total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    production['production_time_str'] = f"{hours:02d}:{minutes:02d}"
            else:
                production['production_time_str'] = str(production['production_time'])
        
        # Get meat production details for meat productions
        meat_productions = []
        for production in productions:
            if production['production_type'] == 'meat':
                cursor.execute("""
                    SELECT chicken_number, alive_weight, dead_weight
                    FROM chicken_meat_production
                    WHERE production_id = %s
                    ORDER BY chicken_number
                """, (production['id'],))
                meat_details = cursor.fetchall()
                production['meat_details'] = meat_details
            else:
                production['meat_details'] = []
        
        # Calculate statistics
        total_productions = len(productions)
        egg_productions = len([p for p in productions if p['production_type'] == 'eggs'])
        meat_productions_count = len([p for p in productions if p['production_type'] == 'meat'])
        total_eggs = sum(p['quantity'] for p in productions if p['production_type'] == 'eggs')
        total_meat_chickens = sum(p['quantity'] for p in productions if p['production_type'] == 'meat')
        
        # Get recent productions (last 7 days)
        cursor.execute("""
            SELECT COUNT(*) as recent_count
            FROM chicken_production
            WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        """)
        recent_productions = cursor.fetchone()['recent_count']
        
        stats = {
            'total_productions': total_productions,
            'egg_productions': egg_productions,
            'meat_productions': meat_productions_count,
            'total_eggs': total_eggs,
            'total_meat_chickens': total_meat_chickens,
            'recent_productions': recent_productions
        }
        
    except Exception as e:
        print(f"Error fetching production data: {str(e)}")
        productions = []
        stats = {
            'total_productions': 0,
            'egg_productions': 0,
            'meat_productions': 0,
            'total_eggs': 0,
            'total_meat_chickens': 0,
            'recent_productions': 0
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return render_template('admin_farm_chicken_production_management.html', 
                         user=user_data, 
                         productions=productions, 
                         stats=stats)

@app.route('/admin/farm/chicken-production-edit/<int:production_id>', methods=['GET'])
def admin_farm_chicken_production_edit(production_id):
    """Get production details for editing"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get production details
        cursor.execute("""
            SELECT 
                cp.id,
                cp.production_type,
                cp.chicken_id,
                cp.chicken_category,
                cp.production_date,
                cp.production_time,
                cp.quantity,
                cp.notes,
                c.batch_name,
                c.breed_name,
                c.coop_number
            FROM chicken_production cp
            LEFT JOIN chickens c ON cp.chicken_id COLLATE utf8mb4_unicode_ci = c.chicken_id COLLATE utf8mb4_unicode_ci
            WHERE cp.id = %s
        """, (production_id,))
        
        production = cursor.fetchone()
        if not production:
            return jsonify({'error': 'Production not found'}), 404
        
        # Get meat production details if it's a meat production
        meat_details = []
        if production['production_type'] == 'meat':
            cursor.execute("""
                SELECT chicken_number, alive_weight, dead_weight
                FROM chicken_meat_production
                WHERE production_id = %s
                ORDER BY chicken_number
            """, (production_id,))
            meat_details = cursor.fetchall()
        
        # Convert time to string for JSON serialization
        if production['production_time']:
            if hasattr(production['production_time'], 'strftime'):
                production['production_time_str'] = production['production_time'].strftime('%H:%M')
            else:
                total_seconds = int(production['production_time'].total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                production['production_time_str'] = f"{hours:02d}:{minutes:02d}"
        else:
            production['production_time_str'] = ""
        
        production['production_date_str'] = production['production_date'].strftime('%Y-%m-%d')
        production['meat_details'] = meat_details
        
        # Convert datetime objects to strings for JSON serialization
        if production['production_date']:
            production['production_date'] = production['production_date'].strftime('%Y-%m-%d')
        if production['production_time']:
            if hasattr(production['production_time'], 'strftime'):
                production['production_time'] = production['production_time'].strftime('%H:%M')
            else:
                total_seconds = int(production['production_time'].total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                production['production_time'] = f"{hours:02d}:{minutes:02d}"
        
        return jsonify(production)
        
    except Exception as e:
        print(f"Error fetching production details: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/admin/farm/chicken-production-update/<int:production_id>', methods=['POST'])
def admin_farm_chicken_production_update(production_id):
    """Update production record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Get original production data for audit
            cursor.execute("""
                SELECT production_date, production_time, quantity, notes, chicken_id, production_type
                FROM chicken_production WHERE id = %s
            """, (production_id,))
            original_data = cursor.fetchone()
            
            # Update production record
            cursor.execute("""
                UPDATE chicken_production 
                SET production_date = %s, production_time = %s, quantity = %s, notes = %s
                WHERE id = %s
            """, (
                data.get('production_date'),
                data.get('production_time'),
                data.get('quantity'),
                data.get('notes', ''),
                production_id
            ))
            
            # Audit table should already exist from management route
            
            # Track changes
            changes = []
            if original_data['production_date'].strftime('%Y-%m-%d') != data.get('production_date'):
                changes.append(('production_date', original_data['production_date'].strftime('%Y-%m-%d'), data.get('production_date')))
            if str(original_data['production_time']) != data.get('production_time'):
                changes.append(('production_time', str(original_data['production_time']), data.get('production_time')))
            if str(original_data['quantity']) != str(data.get('quantity')):
                changes.append(('quantity', str(original_data['quantity']), str(data.get('quantity'))))
            if (original_data['notes'] or '') != (data.get('notes') or ''):
                changes.append(('notes', original_data['notes'] or '', data.get('notes') or ''))
            
            # Insert audit records
            for field_name, old_value, new_value in changes:
                cursor.execute("""
                    INSERT INTO chicken_production_audit 
                    (production_id, chicken_id, production_type, field_name, old_value, new_value, edited_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    production_id,
                    original_data['chicken_id'],
                    original_data['production_type'],
                    field_name,
                    old_value,
                    new_value,
                    session['employee_id']
                ))
            
            # If meat production, update weight details
            if data.get('production_type') == 'meat' and data.get('meat_details'):
                # Delete existing meat details
                cursor.execute("DELETE FROM chicken_meat_production WHERE production_id = %s", (production_id,))
                
                # Insert new meat details
                for detail in data['meat_details']:
                    cursor.execute("""
                        INSERT INTO chicken_meat_production 
                        (production_id, chicken_number, alive_weight, dead_weight)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        production_id,
                        detail['chicken_number'],
                        detail['alive_weight'],
                        detail['dead_weight']
                    ))
            
            conn.commit()
            return jsonify({'success': True, 'message': 'Production updated successfully'})
            
        except Exception as e:
            conn.rollback()
            print(f"Error updating production: {str(e)}")
            return jsonify({'success': False, 'message': f'Database error: {str(e)}'})
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        print(f"Error processing production update: {str(e)}")
        return jsonify({'success': False, 'message': f'Server error: {str(e)}'})

@app.route('/admin/farm/chicken-production-delete/<int:production_id>', methods=['DELETE'])
def admin_farm_chicken_production_delete(production_id):
    """Delete production record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get production details before deletion
        cursor.execute("SELECT production_type, chicken_id, quantity FROM chicken_production WHERE id = %s", (production_id,))
        production = cursor.fetchone()
        
        if not production:
            return jsonify({'success': False, 'message': 'Production not found'})
        
        # If it's a meat production, we need to restore chicken quantity
        if production['production_type'] == 'meat':
            # Get current chicken quantity
            cursor.execute("SELECT quantity FROM chickens WHERE chicken_id = %s", (production['chicken_id'],))
            chicken = cursor.fetchone()
            
            if chicken:
                # Restore the slaughtered quantity
                new_quantity = chicken['quantity'] + production['quantity']
                cursor.execute("""
                    UPDATE chickens 
                    SET quantity = %s, current_status = 'active'
                    WHERE chicken_id = %s
                """, (new_quantity, production['chicken_id']))
        
        # Delete production record (meat details will be deleted by CASCADE)
        cursor.execute("DELETE FROM chicken_production WHERE id = %s", (production_id,))
        
        conn.commit()
        return jsonify({'success': True, 'message': 'Production deleted successfully'})
        
    except Exception as e:
        conn.rollback()
        print(f"Error deleting production: {str(e)}")
        return jsonify({'success': False, 'message': f'Database error: {str(e)}'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/admin/farm/chicken-production-audit/<int:production_id>')
def admin_farm_chicken_production_audit(production_id):
    """Get audit history for a production record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get audit history
        cursor.execute("""
            SELECT 
                cpa.field_name,
                cpa.old_value,
                cpa.new_value,
                cpa.edited_at,
                COALESCE(e.full_name, 'Unknown User') as edited_by_name
            FROM chicken_production_audit cpa
            LEFT JOIN employees e ON cpa.edited_by = e.id
            WHERE cpa.production_id = %s
            ORDER BY cpa.edited_at DESC
        """, (production_id,))
        
        audit_records = cursor.fetchall()
        
        # Convert datetime to string for JSON serialization
        for record in audit_records:
            if record['edited_at']:
                record['edited_at'] = record['edited_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify(audit_records)
        
    except Exception as e:
        print(f"Error fetching audit history: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/admin/farm/chicken-production-analytics')
def admin_farm_chicken_production_analytics():
    """Chicken production analytics page"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    # Fetch chickens with production data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all chickens that have production records
        cursor.execute("""
            SELECT DISTINCT
                c.chicken_id,
                c.batch_name,
                c.breed_name,
                c.chicken_type,
                c.gender,
                c.coop_number,
                c.quantity,
                c.current_status,
                COUNT(cp.id) as production_count,
                SUM(CASE WHEN cp.production_type = 'eggs' THEN cp.quantity ELSE 0 END) as total_eggs,
                SUM(CASE WHEN cp.production_type = 'meat' THEN cp.quantity ELSE 0 END) as total_meat_production,
                MAX(cp.production_date) as last_production_date
            FROM chickens c
            INNER JOIN chicken_production cp ON c.chicken_id COLLATE utf8mb4_unicode_ci = cp.chicken_id COLLATE utf8mb4_unicode_ci
            WHERE c.current_status IN ('active', 'sold')
            GROUP BY c.chicken_id, c.batch_name, c.breed_name, c.chicken_type, c.gender, c.coop_number, c.quantity, c.current_status
            ORDER BY last_production_date DESC, c.chicken_id
        """)
        chickens = cursor.fetchall()
        
        # Calculate statistics
        total_chickens = len(chickens)
        active_chickens = len([c for c in chickens if c['current_status'] == 'active'])
        sold_chickens = len([c for c in chickens if c['current_status'] == 'sold'])
        total_eggs = sum(c['total_eggs'] for c in chickens)
        total_meat_production = sum(c['total_meat_production'] for c in chickens)
        
        # Get recent productions (last 7 days)
        cursor.execute("""
            SELECT COUNT(*) as recent_count
            FROM chicken_production
            WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        """)
        recent_productions = cursor.fetchone()['recent_count']
        
        stats = {
            'total_chickens': total_chickens,
            'active_chickens': active_chickens,
            'sold_chickens': sold_chickens,
            'total_eggs': total_eggs,
            'total_meat_production': total_meat_production,
            'recent_productions': recent_productions
        }
        
    except Exception as e:
        print(f"Error fetching production analytics: {str(e)}")
        chickens = []
        stats = {
            'total_chickens': 0,
            'active_chickens': 0,
            'sold_chickens': 0,
            'total_eggs': 0,
            'total_meat_production': 0,
            'recent_productions': 0
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return render_template('admin_farm_chicken_production_analytics.html', 
                         user=user_data, 
                         chickens=chickens, 
                         stats=stats)

@app.route('/admin/farm/chicken-production-history/<chicken_id>')
def admin_farm_chicken_production_history(chicken_id):
    """Get production history for a specific chicken"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get chicken details
        cursor.execute("""
            SELECT chicken_id, batch_name, breed_name, chicken_type, gender, coop_number, quantity, current_status
            FROM chickens 
            WHERE chicken_id = %s
        """, (chicken_id,))
        chicken = cursor.fetchone()
        
        if not chicken:
            return jsonify({'error': 'Chicken not found'}), 404
        
        # Get production history
        cursor.execute("""
            SELECT 
                cp.id,
                cp.production_type,
                cp.production_date,
                cp.production_time,
                cp.quantity,
                cp.notes,
                cp.created_at
            FROM chicken_production cp
            WHERE cp.chicken_id = %s
            ORDER BY cp.production_date DESC, cp.production_time DESC
        """, (chicken_id,))
        productions = cursor.fetchall()
        
        # Get meat production details for meat productions
        for production in productions:
            if production['production_type'] == 'meat':
                cursor.execute("""
                    SELECT chicken_number, alive_weight, dead_weight
                    FROM chicken_meat_production
                    WHERE production_id = %s
                    ORDER BY chicken_number
                """, (production['id'],))
                meat_details = cursor.fetchall()
                production['meat_details'] = meat_details
            else:
                production['meat_details'] = []
        
        # Convert time to string for JSON serialization
        for production in productions:
            if production['production_time']:
                if hasattr(production['production_time'], 'strftime'):
                    production['production_time_str'] = production['production_time'].strftime('%H:%M')
                else:
                    total_seconds = int(production['production_time'].total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    production['production_time_str'] = f"{hours:02d}:{minutes:02d}"
            else:
                production['production_time_str'] = ""
            
            production['production_date_str'] = production['production_date'].strftime('%Y-%m-%d')
        
        return jsonify({
            'chicken': chicken,
            'productions': productions
        })
        
    except Exception as e:
        print(f"Error fetching chicken production history: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/chickens/search')
def api_chickens_search():
    """API endpoint for searching chickens by category and ID"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    category = request.args.get('category')
    query = request.args.get('query', '').strip()
    
    print(f"Chicken search request: category={category}, query={query}")
    
    if not category or not query:
        print("Missing category or query")
        return jsonify([])
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, let's check if there are any chickens at all
        cursor.execute("SELECT COUNT(*) as total FROM chickens WHERE current_status = 'active'")
        total_chickens = cursor.fetchone()
        print(f"Total active chickens in database: {total_chickens['total']}")
        
        # Search for chickens by category and ID (partial match)
        search_pattern = f'%{query}%'
        print(f"Searching with pattern: {search_pattern}")
        
        cursor.execute("""
            SELECT chicken_id, batch_name, breed_name, gender, age_days, coop_number, quantity
            FROM chickens 
            WHERE chicken_type = %s 
            AND current_status = 'active' 
            AND chicken_id LIKE %s
            ORDER BY chicken_id
            LIMIT 10
        """, (category, search_pattern))
        
        chickens = cursor.fetchall()
        print(f"Found {len(chickens)} chickens matching search criteria")
        
        # If no results, let's show all chickens of that category for debugging
        if len(chickens) == 0:
            cursor.execute("""
                SELECT chicken_id, batch_name, breed_name, gender, age_days, coop_number, quantity
                FROM chickens 
                WHERE chicken_type = %s 
                AND current_status = 'active'
                ORDER BY chicken_id
                LIMIT 5
            """, (category,))
            all_chickens = cursor.fetchall()
            print(f"All chickens of type {category}: {[c['chicken_id'] for c in all_chickens]}")
        
        return jsonify(chickens)
        
    except Exception as e:
        print(f"Error searching chickens: {str(e)}")
        return jsonify({'error': str(e)})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/admin/farm/chicken-production-register', methods=['POST'])
def admin_farm_chicken_production_register():
    """Handle production registration form submission"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['production_type', 'chicken_category', 'chicken_id_search']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field} is required'})
        
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Create chicken_production table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chicken_production (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    production_type ENUM('eggs', 'meat') NOT NULL,
                    chicken_id VARCHAR(50) NOT NULL,
                    chicken_category VARCHAR(20) NOT NULL,
                    production_date DATE NOT NULL,
                    production_time TIME NOT NULL,
                    quantity INT NOT NULL,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by INT,
                    INDEX idx_chicken_id (chicken_id),
                    INDEX idx_production_date (production_date),
                    INDEX idx_production_type (production_type)
                )
            """)
            
            # Create chicken_meat_production table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chicken_meat_production (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    production_id INT,
                    chicken_number INT NOT NULL,
                    alive_weight DECIMAL(6,3) NOT NULL,
                    dead_weight DECIMAL(6,3) NOT NULL,
                    FOREIGN KEY (production_id) REFERENCES chicken_production(id) ON DELETE CASCADE
                )
            """)
            
            # Insert production record
            production_date = data.get('egg_collection_date') or data.get('slaughter_date')
            production_time = data.get('egg_collection_time') or data.get('slaughter_time')
            quantity = data.get('egg_count') or data.get('slaughter_count')
            
            cursor.execute("""
                INSERT INTO chicken_production 
                (production_type, chicken_id, chicken_category, production_date, production_time, quantity, notes, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['production_type'],
                data['chicken_id_search'],
                data['chicken_category'],
                production_date,
                production_time,
                quantity,
                data.get('notes', ''),
                session['employee_id']
            ))
            
            production_id = cursor.lastrowid
            
            # If meat production, insert weight details and update chicken quantity
            if data['production_type'] == 'meat':
                alive_weights = data.get('alive_weights', [])
                dead_weights = data.get('dead_weights', [])
                
                for i, (alive_weight, dead_weight) in enumerate(zip(alive_weights, dead_weights)):
                    cursor.execute("""
                        INSERT INTO chicken_meat_production 
                        (production_id, chicken_number, alive_weight, dead_weight)
                        VALUES (%s, %s, %s, %s)
                    """, (production_id, i + 1, alive_weight, dead_weight))
                
                # Update chicken quantity and status
                slaughtered_count = int(data.get('slaughter_count', 0))
                if slaughtered_count > 0:
                    # Get current chicken quantity and status
                    cursor.execute("""
                        SELECT quantity, current_status FROM chickens WHERE chicken_id = %s
                    """, (data['chicken_id_search'],))
                    current_chicken = cursor.fetchone()
                    
                    if current_chicken:
                        current_quantity = current_chicken['quantity']
                        current_status = current_chicken['current_status']
                        
                        # Check if chicken is still active
                        if current_status != 'active':
                            return jsonify({'success': False, 'message': f'Chicken {data["chicken_id_search"]} is not active (status: {current_status})'})
                        
                        # Check if trying to slaughter more than available
                        if slaughtered_count > current_quantity:
                            return jsonify({'success': False, 'message': f'Cannot slaughter {slaughtered_count} chickens. Only {current_quantity} available for chicken {data["chicken_id_search"]}'})
                        
                        new_quantity = max(0, current_quantity - slaughtered_count)
                        
                        # Update chicken quantity
                        cursor.execute("""
                            UPDATE chickens 
                            SET quantity = %s 
                            WHERE chicken_id = %s
                        """, (new_quantity, data['chicken_id_search']))
                        
                        # If quantity becomes 0, update status to 'sold'
                        if new_quantity == 0:
                            cursor.execute("""
                                UPDATE chickens 
                                SET current_status = 'sold' 
                                WHERE chicken_id = %s
                            """, (data['chicken_id_search'],))
                            
                            print(f"Chicken {data['chicken_id_search']} status updated to 'sold' - all chickens slaughtered")
                        else:
                            print(f"Chicken {data['chicken_id_search']} quantity reduced from {current_quantity} to {new_quantity}")
                    else:
                        return jsonify({'success': False, 'message': f'Chicken {data["chicken_id_search"]} not found in database'})
            
            conn.commit()
            return jsonify({'success': True, 'message': 'Production registered successfully'})
            
        except Exception as e:
            conn.rollback()
            print(f"Error registering production: {str(e)}")
            return jsonify({'success': False, 'message': f'Database error: {str(e)}'})
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    except Exception as e:
        print(f"Error processing production registration: {str(e)}")
        return jsonify({'success': False, 'message': f'Server error: {str(e)}'})

@app.route('/admin/farm/chicken-detail/<chicken_id>')
def admin_farm_chicken_detail(chicken_id):
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get chicken details
        cursor.execute("""
            SELECT 
                chicken_id,
                batch_name,
                chicken_type,
                breed_name,
                gender,
                hatch_date,
                age_days,
                source,
                coop_number,
                quantity,
                current_status,
                registration_date
            FROM chickens 
            WHERE chicken_id = %s
        """, (chicken_id,))
        
        chicken = cursor.fetchone()
        if not chicken:
            return redirect(url_for('admin_farm_chicken_health_analytics'))
        
        # Get medical journey (medication tracking)
        cursor.execute("""
            SELECT 
                cmt.id,
                cmt.created_at as checked_at,
                cm.medication_name,
                cm.category,
                cm.start_day,
                cm.end_day,
                cm.purpose,
                cm.image_filename,
                cmt.status,
                '' as notes
            FROM chicken_medication_tracking cmt
            JOIN chicken_medications cm ON cmt.medication_id = cm.id
            WHERE cmt.chicken_id = %s
            ORDER BY cmt.created_at DESC
        """, (chicken_id,))
        
        medical_journey = cursor.fetchall()
        
        # Get weight journey
        cursor.execute("""
            SELECT 
                cwt.id,
                cwt.checked_at,
                cwt.actual_weight,
                cwt.expected_weight,
                cwt.weight_percentage,
                cwt.weight_category,
                cws.age_days as standard_age,
                cws.description as standard_description
            FROM chicken_weight_tracking cwt
            JOIN chicken_weight_standards cws ON cwt.weight_standard_id = cws.id
            WHERE cwt.chicken_id = %s
            ORDER BY cwt.checked_at DESC
        """, (chicken_id,))
        
        weight_journey = cursor.fetchall()
        
        # Get upcoming medications for this chicken
        cursor.execute("""
            SELECT 
                cm.id,
                cm.medication_name,
                cm.start_day,
                cm.end_day,
                cm.purpose,
                cm.image_filename,
                CASE 
                    WHEN %s >= cm.start_day AND %s <= cm.end_day THEN 'current'
                    WHEN %s < cm.start_day THEN 'upcoming'
                    ELSE 'past'
                END as status
            FROM chicken_medications cm
            WHERE cm.category = %s
            AND NOT EXISTS (
                SELECT 1 FROM chicken_medication_tracking cmt 
                WHERE cmt.chicken_id = %s AND cmt.medication_id = cm.id
            )
            ORDER BY cm.start_day
        """, (chicken['age_days'], chicken['age_days'], chicken['age_days'], chicken['chicken_type'], chicken_id))
        
        upcoming_medications = cursor.fetchall()
        
        # Get upcoming weight checks for this chicken
        cursor.execute("""
            SELECT 
                cws.id,
                cws.age_days,
                cws.expected_weight,
                cws.description,
                CASE 
                    WHEN %s >= cws.age_days THEN 'ready'
                    ELSE 'pending'
                END as status
            FROM chicken_weight_standards cws
            WHERE cws.category = %s
            AND NOT EXISTS (
                SELECT 1 FROM chicken_weight_tracking cwt 
                WHERE cwt.chicken_id = %s AND cwt.weight_standard_id = cws.id
            )
            ORDER BY cws.age_days
        """, (chicken['age_days'], chicken['chicken_type'], chicken_id))
        
        upcoming_weight_checks = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error fetching chicken detail data: {str(e)}")
        return redirect(url_for('admin_farm_chicken_health_analytics'))
    
    return render_template('admin_farm_chicken_detail.html',
                         user=user_data,
                         chicken=chicken,
                         medical_journey=medical_journey,
                         weight_journey=weight_journey,
                         upcoming_medications=upcoming_medications,
                         upcoming_weight_checks=upcoming_weight_checks)

@app.route('/admin/farm/chicken-weight-standard', methods=['POST'])
def add_chicken_weight_standard():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        category = request.form.get('category')
        age_days = request.form.get('age_days')
        expected_weight = request.form.get('expected_weight')
        description = request.form.get('description', '')
        
        if not category or not age_days or not expected_weight:
            return jsonify({'success': False, 'message': 'Category, age, and expected weight are required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create chicken_weight_standards table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_weight_standards (
                id INT AUTO_INCREMENT PRIMARY KEY,
                category ENUM('broiler', 'kienyeji', 'layer') NOT NULL,
                age_days INT NOT NULL,
                expected_weight DECIMAL(6,3) NOT NULL COMMENT 'Weight in kilograms',
                description TEXT,
                created_by INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_category (category),
                INDEX idx_age_days (age_days),
                UNIQUE KEY unique_category_age (category, age_days)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Check if weight standard already exists for this category and age
        cursor.execute("""
            SELECT id FROM chicken_weight_standards 
            WHERE category = %s AND age_days = %s
        """, (category, age_days))
        
        existing = cursor.fetchone()
        if existing:
            return jsonify({'success': False, 'message': f'Weight standard already exists for {category} at {age_days} days'})
        
        # Insert new weight standard
        cursor.execute("""
            INSERT INTO chicken_weight_standards (category, age_days, expected_weight, description, created_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (category, age_days, expected_weight, description, session['employee_id']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Weight standard added successfully for {category} at {age_days} days!'
        })
        
    except Exception as e:
        print(f"Error adding weight standard: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error adding weight standard: {str(e)}'
        })

@app.route('/admin/farm/add-sample-medications')
def add_sample_medications():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Add sample medications for older chickens
        sample_medications = [
            # For broiler (36 days old)
            ('broiler', 'Growth Booster', 30, 45, 'Enhance growth and weight gain', session['employee_id']),
            ('broiler', 'Antibiotic Treatment', 35, 40, 'Prevent bacterial infections', session['employee_id']),
            
            # For kienyeji (96 days old) 
            ('kienyeji', 'Laying Support', 90, 120, 'Support egg production', session['employee_id']),
            ('kienyeji', 'Calcium Supplement', 95, 105, 'Strengthen eggshells', session['employee_id']),
            
            # For layer (116 days old)
            ('layer', 'Egg Production Booster', 110, 130, 'Maximize egg laying', session['employee_id']),
            ('layer', 'Vitamin D3', 115, 125, 'Support bone health', session['employee_id']),
        ]
        
        for med in sample_medications:
            cursor.execute("""
                INSERT INTO chicken_medications (category, medication_name, start_day, end_day, purpose, created_by)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, med)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"✅ Added {len(sample_medications)} sample medications for your chickens' current ages!"
        
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/admin/farm/debug-medications')
def debug_medications():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check chickens
        cursor.execute("SELECT COUNT(*) as count FROM chickens WHERE current_status = 'active'")
        chicken_count = cursor.fetchone()['count']
        
        # Check medications
        cursor.execute("SELECT COUNT(*) as count FROM chicken_medications")
        medication_count = cursor.fetchone()['count']
        
        # Get sample data
        cursor.execute("SELECT chicken_id, age_days, chicken_type FROM chickens WHERE current_status = 'active' LIMIT 5")
        sample_chickens = cursor.fetchall()
        
        cursor.execute("SELECT medication_name, start_day, end_day, category FROM chicken_medications LIMIT 5")
        sample_medications = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return f"""
        <h1>Debug Information</h1>
        <p>Active chickens: {chicken_count}</p>
        <p>Total medications: {medication_count}</p>
        <h2>Sample Chickens:</h2>
        <pre>{sample_chickens}</pre>
        <h2>Sample Medications:</h2>
        <pre>{sample_medications}</pre>
        """
        
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/admin/farm/chicken-medication-complete', methods=['POST'])
def mark_medication_complete():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        chicken_id = request.form.get('chicken_id')
        medication_id = request.form.get('medication_id')
        notes = request.form.get('notes', '')
        
        if not chicken_id or not medication_id:
            return jsonify({'success': False, 'message': 'Chicken ID and Medication ID are required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if tracking record exists
        cursor.execute("""
            SELECT id, status FROM chicken_medication_tracking 
            WHERE chicken_id = %s AND medication_id = %s
        """, (chicken_id, medication_id))
        
        existing_record = cursor.fetchone()
        
        if existing_record:
            # Update existing record
            cursor.execute("""
                UPDATE chicken_medication_tracking 
                SET status = 'completed', 
                    completed_date = CURDATE(),
                    notes = %s,
                    administered_by = %s
                WHERE id = %s
            """, (notes, session['employee_id'], existing_record['id']))
        else:
            # Create new tracking record
            cursor.execute("""
                INSERT INTO chicken_medication_tracking 
                (chicken_id, medication_id, scheduled_date, completed_date, status, notes, administered_by)
                VALUES (%s, %s, CURDATE(), CURDATE(), 'completed', %s, %s)
            """, (chicken_id, medication_id, notes, session['employee_id']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Medication marked as completed for chicken {chicken_id}!'
        })
        
    except Exception as e:
        print(f"Error marking medication as complete: {str(e)}")
        return jsonify({
            'success': False, 
            'message': f'Error marking medication as complete: {str(e)}'
        })

@app.route('/admin/farm/breeding-management')
def admin_farm_breeding_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_breeding_management.html', user=user_data)

@app.route('/admin/farm/health-management')
def admin_farm_health_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_health_management.html', user=user_data)

@app.route('/admin/farm/weight-assessment')
def admin_farm_weight_assessment():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_weight_assessment.html', user=user_data)

@app.route('/admin/farm/weight-settings')
def admin_farm_weight_settings():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_weight_settings.html', user=user_data)

@app.route('/admin/farm/insert-weight')
def admin_farm_insert_weight():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all litters with sow information
        cursor.execute("""
            SELECT l.*, 
                   p.tag_id as sow_tag_id, p.breed as sow_breed,
                   f.farm_name as farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            ORDER BY l.farrowing_date DESC
        """)
        litters = cursor.fetchall()
        
        # Get all pigs
        cursor.execute("""
            SELECT p.*, f.farm_name,
                   CASE 
                       WHEN p.birth_date IS NOT NULL THEN DATEDIFF(CURDATE(), p.birth_date)
                       ELSE NULL 
                   END as age_days
            FROM pigs p
            LEFT JOIN farms f ON p.farm_id = f.id
            ORDER BY p.created_at DESC
        """)
        pigs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return render_template('admin_farm_insert_weight.html', 
                             user=user_data, 
                             litters=litters, 
                             pigs=pigs)
        
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return render_template('admin_farm_insert_weight.html', 
                             user=user_data, 
                             litters=[], 
                             pigs=[])

@app.route('/admin/farm/weight-analysis')
def admin_farm_weight_analysis():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    # Get animal_id or litter_id from query parameters
    animal_id = request.args.get('animal_id')
    litter_id = request.args.get('litter_id')
    
    return render_template('admin_farm_weight_analysis.html', 
                         user=user_data, 
                         animal_id=animal_id, 
                         litter_id=litter_id)

@app.route('/admin/farm/weight-analytics')
def admin_farm_weight_analytics():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all litters with sow information
        cursor.execute("""
            SELECT l.*, 
                   p.tag_id as sow_tag_id, p.breed as sow_breed,
                   f.farm_name as farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            ORDER BY l.farrowing_date DESC
        """)
        litters = cursor.fetchall()
        
        # Get all pigs
        cursor.execute("""
            SELECT p.*, f.farm_name,
                   CASE 
                       WHEN p.birth_date IS NOT NULL THEN DATEDIFF(CURDATE(), p.birth_date)
                       ELSE NULL 
                   END as age_days
            FROM pigs p
            LEFT JOIN farms f ON p.farm_id = f.id
            ORDER BY p.created_at DESC
        """)
        pigs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return render_template('admin_farm_weight_analytics.html', 
                             user=user_data, 
                             litters=litters, 
                             pigs=pigs)
        
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return render_template('admin_farm_weight_analytics.html', 
                             user=user_data, 
                             litters=[], 
                             pigs=[])

@app.route('/admin/farm/vaccinations')
def admin_farm_vaccinations():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_vaccinations.html', user=user_data)

@app.route('/admin/farm/vaccination-settings')
def admin_farm_vaccination_settings():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_vaccination_settings.html', user=user_data)

@app.route('/admin/farm/vaccination-tracking')
def admin_farm_vaccination_tracking():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_vaccination_tracking.html', user=user_data)

@app.route('/admin/farm/location')
def admin_farm_location():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_location.html', user=user_data)

@app.route('/admin/farm/slaughter')
def admin_farm_slaughter():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_slaughter.html', user=user_data)

@app.route('/admin/farm/slaughter/view-production')
def admin_farm_slaughter_view_production():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_slaughter_view_production.html', user=user_data)

@app.route('/admin/farm/feeding-management')
def admin_farm_feeding_management():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_feeding_management.html', user=user_data)

@app.route('/admin/farm/health-status')
def admin_farm_health_status():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_health_status.html', user=user_data)

@app.route('/admin/farm/pig-analytics')
def admin_farm_pig_analytics():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('admin_farm_pig_analytics.html', user=user_data)

# Admin Role Access Routes - Allow admin to view role-specific dashboards
@app.route('/admin/access/manager')
def admin_access_manager():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get admin user data but display as manager
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': 'manager',  # Temporarily show as manager
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('manager_dashboard.html', user=user_data)

@app.route('/admin/access/employee')
def admin_access_employee():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get admin user data but display as employee
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': 'employee',  # Temporarily show as employee
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('employee_dashboard.html', user=user_data)

@app.route('/admin/access/vet')
def admin_access_vet():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get admin user data but display as vet
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': 'vet',  # Temporarily show as vet
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('vet_dashboard.html', user=user_data)

@app.route('/admin/access/it')
def admin_access_it():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get admin user data but display as IT
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': 'it',  # Temporarily show as IT
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    return render_template('it_dashboard.html', user=user_data)

# HR Management API Routes
@app.route('/api/hr/employees', methods=['GET'])
def get_employees():
    """Get all employees for HR management"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, full_name, email, phone, employee_code, role, status, 
                   created_at, profile_image
            FROM employees 
            ORDER BY created_at DESC
        """)
        employees = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'employees': employees})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/hr/approve-employee', methods=['POST'])
def approve_employee():
    """Approve or reject pending employee"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        employee_id = data.get('employee_id')
        action = data.get('action')  # 'approve' or 'reject'
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if action == 'approve':
            cursor.execute("UPDATE employees SET status = 'active' WHERE id = %s", (employee_id,))
            action_desc = 'approved'
        else:
            cursor.execute("DELETE FROM employees WHERE id = %s", (employee_id,))
            action_desc = 'rejected'
        
        conn.commit()
        
        # Log activity
        log_activity(session['employee_id'], 'EMPLOYEE_APPROVAL', 
                    f'Employee {employee_id} {action_desc}')
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': f'Employee {action_desc} successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/hr/update-status', methods=['POST'])
def update_employee_status():
    """Suspend or unsuspend employee"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        employee_id = data.get('employee_id')
        new_status = data.get('status')  # 'active' or 'suspended'
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("UPDATE employees SET status = %s WHERE id = %s", (new_status, employee_id))
        conn.commit()
        
        # Log activity
        log_activity(session['employee_id'], 'STATUS_UPDATE', 
                    f'Employee {employee_id} status changed to {new_status}')
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': f'Employee status updated to {new_status}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/hr/update-employee', methods=['POST'])
def update_employee():
    """Update employee details"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        employee_id = data.get('employee_id')
        full_name = data.get('full_name')
        email = data.get('email')
        phone = data.get('phone')
        role = data.get('role')
        password = data.get('password')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build update query based on whether password is provided
        if password:
            hashed_password = hash_password(password)
            cursor.execute("""
                UPDATE employees 
                SET full_name = %s, email = %s, phone = %s, role = %s, password = %s 
                WHERE id = %s
            """, (full_name, email, phone, role, hashed_password, employee_id))
            update_message = 'Employee details and password updated successfully'
        else:
            cursor.execute("""
                UPDATE employees 
                SET full_name = %s, email = %s, phone = %s, role = %s 
                WHERE id = %s
            """, (full_name, email, phone, role, employee_id))
            update_message = 'Employee details updated successfully'
        
        conn.commit()
        
        # Log activity
        log_activity(session['employee_id'], 'EMPLOYEE_UPDATE', 
                    f'Employee {employee_id} details updated')
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': update_message})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/hr/update-permissions', methods=['POST'])
def update_permissions():
    """Update employee permissions"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        employee_id = data.get('employee_id')
        permissions = data.get('permissions', {})
        
        # For now, we'll store permissions as JSON in a comment field
        # In a real system, you'd have a separate permissions table
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Log activity
        log_activity(session['employee_id'], 'PERMISSION_UPDATE', 
                    f'Employee {employee_id} permissions updated')
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Permissions updated successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/hr/update-finance', methods=['POST'])
def update_finance():
    """Update employee finance details"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        employee_id = data.get('employee_id')
        salary = data.get('salary')
        bank_account = data.get('bank_account')
        payment_method = data.get('payment_method')
        
        # For now, we'll log this activity
        # In a real system, you'd have a separate finance table
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Log activity
        log_activity(session['employee_id'], 'FINANCE_UPDATE', 
                    f'Employee {employee_id} finance details updated')
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Finance details updated successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Farm Management API Routes
@app.route('/api/farm/register', methods=['POST'])
def register_farm():
    """Register a new farm"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        farm_name = data.get('farm_name')
        farm_location = data.get('farm_location')
        
        # Validation
        if not farm_name or not farm_location:
            return jsonify({'success': False, 'message': 'Farm name and location are required'})
        
        if len(farm_name.strip()) < 2:
            return jsonify({'success': False, 'message': 'Farm name must be at least 2 characters long'})
        
        if len(farm_location.strip()) < 3:
            return jsonify({'success': False, 'message': 'Farm location must be at least 3 characters long'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if farm name already exists
        cursor.execute("SELECT id FROM farms WHERE farm_name = %s", (farm_name.strip(),))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Farm name already exists'})
        
        # Insert new farm
        cursor.execute("""
            INSERT INTO farms (farm_name, farm_location, created_by, status)
            VALUES (%s, %s, %s, 'active')
        """, (farm_name.strip(), farm_location.strip(), session['employee_id']))
        
        farm_id = cursor.lastrowid
        
        # Log activity
        log_activity(session['employee_id'], 'FARM_REGISTRATION', 
                    f'New farm "{farm_name}" registered at {farm_location}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Farm "{farm_name}" registered successfully',
            'farm_id': farm_id
        })
        
    except Exception as e:
        print(f"Farm registration error: {str(e)}")
        return jsonify({'success': False, 'message': 'Failed to register farm. Please try again.'})

@app.route('/api/farm/statistics', methods=['GET'])
def get_farm_statistics():
    """Get farm statistics for dashboard"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total farms
        cursor.execute("SELECT COUNT(*) as total_farms FROM farms WHERE status = 'active'")
        total_farms = cursor.fetchone()['total_farms']
        
        # Get total pigs
        cursor.execute("SELECT COUNT(*) as total_pigs FROM pigs WHERE status = 'active'")
        total_pigs = cursor.fetchone()['total_pigs']
        
        # Get growth rate (placeholder - will be calculated based on pig growth data)
        growth_rate = "0%"  # Placeholder for now
        
        # Get health issues (placeholder - will be updated when health records table is created)
        health_issues = 0  # Placeholder for now
        
        # Get breeding statistics
        cursor.execute("SELECT COUNT(*) as total_breeding FROM breeding_records WHERE status = 'served' OR status = 'pregnant'")
        total_breeding = cursor.fetchone()['total_breeding']
        
        cursor.execute("SELECT COUNT(*) as total_failed FROM failed_conceptions")
        total_failed = cursor.fetchone()['total_failed']
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'statistics': {
                'total_farms': total_farms,
                'total_pigs': total_pigs,
                'growth_rate': growth_rate,
                'health_issues': health_issues,
                'total_breeding': total_breeding,
                'total_failed': total_failed
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/farm/list', methods=['GET'])
def get_farms_list():
    """Get all farms for display"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all farms with creator information
        cursor.execute("""
            SELECT f.*, e.full_name as created_by_name 
            FROM farms f 
            LEFT JOIN employees e ON f.created_by = e.id 
            WHERE f.status = 'active'
            ORDER BY f.created_at DESC
        """)
        farms = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'farms': farms
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/farms', methods=['GET'])
def get_farms():
    """Get all farms for dropdown selection"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all active farms
        cursor.execute("""
            SELECT id, name, location 
            FROM farms 
            WHERE status = 'active'
            ORDER BY name ASC
        """)
        farms = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'farms': farms
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/farm/details/<int:farm_id>', methods=['GET'])
def get_farm_details(farm_id):
    """Get specific farm details"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get farm details with creator information
        cursor.execute("""
            SELECT f.*, e.full_name as created_by_name 
            FROM farms f 
            LEFT JOIN employees e ON f.created_by = e.id 
            WHERE f.id = %s AND f.status = 'active'
        """, (farm_id,))
        farm = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not farm:
            return jsonify({'error': 'Farm not found'}), 404
        
        return jsonify({
            'success': True,
            'farm': farm
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/farm/pigs/<int:farm_id>', methods=['GET'])
def get_farm_pigs(farm_id):
    """Get all pigs registered to a specific farm"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all pigs for the specific farm
        cursor.execute("""
            SELECT p.id, p.tag_id, p.farm_id, p.pig_type, p.pig_source, p.breed, p.gender, 
                   p.purpose, p.breeding_status, p.birth_date, p.purchase_date, p.age_days,
                   p.registered_by, p.status, p.created_at, p.updated_at, f.farm_name 
            FROM pigs p 
            LEFT JOIN farms f ON p.farm_id = f.id 
            WHERE p.farm_id = %s AND p.status = 'active'
            ORDER BY p.created_at DESC
        """, (farm_id,))
        pigs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'pigs': pigs
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/farm/litters/<int:farm_id>', methods=['GET'])
def get_farm_litters(farm_id):
    """Get all litters for a specific farm"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all litters for the specific farm with sow information
        cursor.execute("""
            SELECT l.id, l.litter_id, l.farrowing_record_id, l.sow_id, l.boar_id,
                   l.total_piglets, l.alive_piglets, l.still_births,
                   l.avg_weight, l.weaning_weight, l.weaning_date, l.farrowing_date,
                   l.status, l.created_at, l.updated_at,
                   p.tag_id as sow_tag, p.breed as sow_breed,
                   f.farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE p.farm_id = %s AND l.status IN ('unweaned', 'weaned')
            ORDER BY l.created_at DESC
        """, (farm_id,))
        litters = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'litters': litters
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Pig Management API Routes
@app.route('/api/pig/register', methods=['POST'])
def register_pig():
    """Register a new pig"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        farm_id = data.get('farm_id')
        pig_type = data.get('pig_type')
        pig_source = data.get('pig_source')
        breed = data.get('breed')
        gender = data.get('gender')
        purpose = data.get('purpose')
        birth_date = data.get('birth_date')
        purchase_date = data.get('purchase_date')
        
        # Validation
        if not farm_id or not pig_type or not pig_source:
            return jsonify({'success': False, 'message': 'Farm, pig type, and pig source are required'})
        
        if pig_type not in ['grown_pig', 'piglet', 'litter', 'batch']:
            return jsonify({'success': False, 'message': 'Invalid pig type'})
        
        if pig_source not in ['born', 'purchased']:
            return jsonify({'success': False, 'message': 'Invalid pig source'})
        
        # Validate date requirements based on pig source
        if not birth_date:
            return jsonify({'success': False, 'message': 'Birth date is required for all pigs'})
        
        if pig_source == 'purchased' and not purchase_date:
            return jsonify({'success': False, 'message': 'Purchase date is required for purchased pigs'})
        
        # For grown pigs, breed, gender, and purpose are required
        if pig_type == 'grown_pig':
            if not breed or not gender or not purpose:
                return jsonify({'success': False, 'message': 'Breed, gender, and purpose are required for grown pigs'})
        
        # Calculate age in days if birth date is provided
        age_days = None
        if birth_date:
            try:
                birth_dt = datetime.strptime(birth_date, '%Y-%m-%d')
                age_days = (datetime.now() - birth_dt).days
            except ValueError:
                return jsonify({'success': False, 'message': 'Invalid birth date format'})
        
        # Determine breeding status for grown pigs based on age
        breeding_status = None
        if pig_type == 'grown_pig' and purpose == 'breeding' and age_days is not None:
            breeding_status = 'available' if age_days >= 200 else 'young'
        elif pig_type == 'grown_pig' and purpose == 'meat':
            breeding_status = None  # Meat pigs don't have breeding status
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if farm exists
        cursor.execute("SELECT id FROM farms WHERE id = %s AND status = 'active'", (farm_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Farm not found or inactive'})
        
        # Generate unique tag ID
        tag_id = generate_pig_tag_id(pig_type)
        print(f"Generated tag ID: {tag_id} for pig type: {pig_type}")
        
        # Insert new pig
        print(f"Inserting pig with data: tag_id={tag_id}, farm_id={farm_id}, pig_type={pig_type}, pig_source={pig_source}, breed={breed}, gender={gender}, purpose={purpose}, breeding_status={breeding_status}, birth_date={birth_date}, purchase_date={purchase_date}, age_days={age_days}, registered_by={session['employee_id']}")
        
        cursor.execute("""
            INSERT INTO pigs (tag_id, farm_id, pig_type, pig_source, breed, gender, purpose, breeding_status, birth_date, purchase_date, age_days, registered_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (tag_id, farm_id, pig_type, pig_source, breed, gender, purpose, breeding_status, birth_date, purchase_date, age_days, session['employee_id']))
        
        pig_id = cursor.lastrowid
        print(f"Pig inserted successfully with ID: {pig_id}")
        
        # Log activity
        log_activity(session['employee_id'], 'PIG_REGISTRATION', 
                    f'New {pig_type} registered with tag {tag_id} at farm {farm_id}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Pig registered successfully with tag {tag_id}',
            'pig_id': pig_id,
            'tag_id': tag_id
        })
        
    except Exception as e:
        print(f"Pig registration error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Failed to register pig: {str(e)}'})

@app.route('/api/pig/registration-stats', methods=['GET'])
def get_pig_registration_stats():
    """Get pig registration statistics for dashboard"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total pigs
        cursor.execute("SELECT COUNT(*) as total FROM pigs WHERE status = 'active'")
        total_pigs = cursor.fetchone()['total']
        
        # Get active pigs
        cursor.execute("SELECT COUNT(*) as active FROM pigs WHERE status = 'active'")
        active_pigs = cursor.fetchone()['active']
        
        # Get breeding pigs
        cursor.execute("SELECT COUNT(*) as breeding FROM pigs WHERE status = 'active' AND purpose = 'breeding'")
        breeding_pigs = cursor.fetchone()['breeding']
        
        # Get today's registrations
        cursor.execute("SELECT COUNT(*) as today FROM pigs WHERE DATE(created_at) = CURDATE()")
        today_registrations = cursor.fetchone()['today']
        
        # Get recent registrations (last 5)
        cursor.execute("""
            SELECT p.id, p.tag_id, p.breed, p.pig_type, p.status, p.created_at, f.farm_name
            FROM pigs p 
            LEFT JOIN farms f ON p.farm_id = f.id 
            WHERE p.status = 'active'
            ORDER BY p.created_at DESC 
            LIMIT 5
        """)
        recent_pigs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'total_pigs': total_pigs,
                'active_pigs': active_pigs,
                'breeding_pigs': breeding_pigs,
                'today_registrations': today_registrations
            },
            'recent_pigs': recent_pigs
        })
        
    except Exception as e:
        print(f"Error getting pig registration stats: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/pig/list', methods=['GET'])
def get_pigs_list():
    """Get all pigs for display"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all pigs with farm and registration information
        cursor.execute("""
            SELECT p.*, f.farm_name, e.full_name as registered_by_name 
            FROM pigs p 
            LEFT JOIN farms f ON p.farm_id = f.id 
            LEFT JOIN employees e ON p.registered_by = e.id 
            WHERE p.status = 'active'
            ORDER BY p.created_at DESC
        """)
        pigs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'pigs': pigs
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pig/generate-tag-id', methods=['POST'])
def generate_tag_id():
    """Generate a new tag ID for a pig type"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        pig_type = data.get('pig_type')
        
        if not pig_type or pig_type not in ['grown_pig', 'piglet', 'litter', 'batch']:
            return jsonify({'success': False, 'message': 'Invalid pig type'})
        
        # Generate the tag ID
        tag_id = generate_pig_tag_id(pig_type)
        
        return jsonify({
            'success': True,
            'tag_id': tag_id
        })
        
    except Exception as e:
        print(f"Tag ID generation error: {str(e)}")
        return jsonify({'success': False, 'message': 'Failed to generate tag ID'})

@app.route('/api/pig/update/<int:pig_id>', methods=['PUT'])
def update_pig(pig_id):
    """Update pig details"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Get current pig data for comparison
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM pigs WHERE id = %s
        """, (pig_id,))
        
        current_pig = cursor.fetchone()
        if not current_pig:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Pig not found'})
        
        # Prepare update data
        update_fields = []
        update_values = []
        changes = []
        
        # Check each field for changes
        fields_to_check = {
            'farm_id': 'Farm',
            'pig_type': 'Pig Type',
            'pig_source': 'Pig Source',
            'breed': 'Breed',
            'gender': 'Gender',
            'purpose': 'Purpose',
            'birth_date': 'Birth Date',
            'purchase_date': 'Purchase Date',
            'status': 'Status'
        }
        
        # Special handling for tag ID changes
        tag_id_changed = False
        new_tag_id = None
        if 'tag_id' in data and data['tag_id'] != current_pig['tag_id']:
            new_tag_id = data['tag_id']
            tag_id_changed = True
            changes.append(f"Tag ID: {current_pig['tag_id']} → {new_tag_id}")
        
        for field, display_name in fields_to_check.items():
            if field in data and data[field] != current_pig[field]:
                if data[field] is not None:
                    update_fields.append(f"{field} = %s")
                    update_values.append(data[field])
                    old_value = current_pig[field] if current_pig[field] is not None else 'None'
                    new_value = data[field]
                    changes.append(f"{display_name}: {old_value} → {new_value}")
                else:
                    update_fields.append(f"{field} = NULL")
                    old_value = current_pig[field] if current_pig[field] is not None else 'None'
                    changes.append(f"{display_name}: {old_value} → None")
        
        if not update_fields and not tag_id_changed:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'No changes detected'})
        
        # Update age_days if birth_date changed
        if 'birth_date' in data and data['birth_date'] != current_pig['birth_date']:
            if data['birth_date']:
                try:
                    birth_dt = datetime.strptime(data['birth_date'], '%Y-%m-%d').date()
                    age_days = (datetime.now().date() - birth_dt).days
                    update_fields.append("age_days = %s")
                    update_values.append(age_days)
                    changes.append(f"Age: {current_pig['age_days'] or 'Unknown'} days → {age_days} days")
                except ValueError:
                    cursor.close()
                    conn.close()
                    return jsonify({'success': False, 'message': 'Invalid birth date format'})
        
        # Always recalculate age_days for all pigs to ensure accuracy
        if 'birth_date' in data:
            try:
                birth_dt = datetime.strptime(data['birth_date'], '%Y-%m-%d').date()
                age_days = (datetime.now().date() - birth_dt).days
                update_fields.append("age_days = %s")
                update_values.append(age_days)
                if 'birth_date' not in [f.split(' = ')[0] for f in update_fields if 'birth_date' in f]:
                    changes.append(f"Age recalculated: {age_days} days")
            except ValueError:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'message': 'Invalid birth date format'})
        
        # Auto-set breeding status for grown pigs
        if 'pig_type' in data and data['pig_type'] == 'grown_pig':
            # Get the purpose and age to determine breeding status
            purpose = data.get('purpose', current_pig.get('purpose'))
            age_days = None
            
            # Calculate age if birth_date is provided
            if 'birth_date' in data and data['birth_date']:
                try:
                    birth_dt = datetime.strptime(data['birth_date'], '%Y-%m-%d').date()
                    age_days = (datetime.now().date() - birth_dt).days
                except ValueError:
                    pass
            
            # Determine breeding status based on purpose and age
            if purpose == 'breeding' and age_days is not None:
                new_breeding_status = 'available' if age_days >= 200 else 'young'
                update_fields.append("breeding_status = %s")
                update_values.append(new_breeding_status)
                changes.append(f"Breeding Status: {current_pig.get('breeding_status', 'None')} → {new_breeding_status}")
            elif purpose == 'meat':
                # Meat pigs don't have breeding status
                update_fields.append("breeding_status = NULL")
                changes.append(f"Breeding Status: {current_pig.get('breeding_status', 'None')} → None (meat pig)")
        
        # Add updated_at timestamp
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        
        # Execute update
        if update_fields:
            update_query = f"UPDATE pigs SET {', '.join(update_fields)} WHERE id = %s"
            update_values.append(pig_id)
            cursor.execute(update_query, update_values)
        
        # Handle tag ID change separately (if changed)
        if tag_id_changed:
            # Add "E" prefix to show the pig has been edited
            edited_tag_id = f"E{new_tag_id}"
            cursor.execute("""
                UPDATE pigs SET tag_id = %s WHERE id = %s
            """, (edited_tag_id, pig_id))
            changes.append(f"Tag ID: {current_pig['tag_id']} → {edited_tag_id} (edited)")
        
        # Handle status changes with reason
        status_reason = data.get('status_reason')
        if 'status' in data and data['status'] != current_pig['status']:
            old_status = current_pig['status']
            new_status = data['status']
            
            if status_reason:
                changes.append(f"Status: {old_status} → {new_status} (Reason: {status_reason})")
                # Log status change with reason
                log_activity(session['employee_id'], 'PIG_STATUS_CHANGE', 
                           f'Changed pig {current_pig["tag_id"]} status from {old_status} to {new_status}. Reason: {status_reason}')
            else:
                changes.append(f"Status: {old_status} → {new_status}")
                # Log status change without reason
                log_activity(session['employee_id'], 'PIG_STATUS_CHANGE', 
                           f'Changed pig {current_pig["tag_id"]} status from {old_status} to {new_status}')
        
        # Mark pig as edited in the database
        cursor.execute("""
            UPDATE pigs SET is_edited = 1 WHERE id = %s
        """, (pig_id,))
        
        # Log activity
        log_activity(session['employee_id'], 'PIG_UPDATE', 
                    f'Updated pig {current_pig["tag_id"]} with changes: {", ".join(changes)}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Pig updated successfully',
            'changes': changes
        })
        
    except Exception as e:
        print(f"Error updating pig: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Failed to update pig: {str(e)}'})

@app.route('/api/pig/details/<int:pig_id>', methods=['GET'])
def get_pig_details(pig_id):
    """Get specific pig details for editing"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get pig details with farm information
        cursor.execute("""
            SELECT p.*, f.farm_name 
            FROM pigs p 
            LEFT JOIN farms f ON p.farm_id = f.id 
            WHERE p.id = %s
        """, (pig_id,))
        
        pig = cursor.fetchone()
        if not pig:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Pig not found'})
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'pig': pig
        })
        
    except Exception as e:
        print(f"Error getting pig details: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get pig details: {str(e)}'})

@app.route('/api/pig/delete/<int:pig_id>', methods=['DELETE'])
def delete_pig(pig_id):
    """Delete a pig from the system"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, get pig details for logging
        cursor.execute("""
            SELECT tag_id, farm_id, pig_type, breed, gender, purpose, status 
            FROM pigs WHERE id = %s
        """, (pig_id,))
        
        pig = cursor.fetchone()
        if not pig:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Pig not found'})
        
        # Check if pig has any related records that might prevent deletion
        # Check for breeding records
        cursor.execute("SELECT COUNT(*) as count FROM breeding_records WHERE sow_id = %s OR boar_id = %s", (pig_id, pig_id))
        breeding_result = cursor.fetchone()
        breeding_count = breeding_result['count'] if breeding_result else 0
        
        if breeding_count > 0:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False, 
                'message': f'Cannot delete pig {pig["tag_id"]}. This pig has {breeding_count} breeding record(s). Please remove breeding records first.'
            })
        
        # Check for litters (if this is a sow)
        cursor.execute("SELECT COUNT(*) as count FROM litters WHERE sow_id = %s", (pig_id,))
        litter_result = cursor.fetchone()
        litter_count = litter_result['count'] if litter_result else 0
        
        if litter_count > 0:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False, 
                'message': f'Cannot delete pig {pig["tag_id"]}. This sow has {litter_count} litter(s). Please remove litters first.'
            })
        
        # Check for weight records (uses animal_id)
        cursor.execute("SELECT COUNT(*) as count FROM weight_records WHERE animal_id = %s", (pig_id,))
        weight_result = cursor.fetchone()
        weight_count = weight_result['count'] if weight_result else 0
        
        # Check for slaughter records (uses pig_id)
        cursor.execute("SELECT COUNT(*) as count FROM slaughter_records WHERE pig_id = %s", (pig_id,))
        slaughter_result = cursor.fetchone()
        slaughter_count = slaughter_result['count'] if slaughter_result else 0
        
        # Check for death records (uses pig_id)
        cursor.execute("SELECT COUNT(*) as count FROM dead_pigs WHERE pig_id = %s", (pig_id,))
        death_result = cursor.fetchone()
        death_count = death_result['count'] if death_result else 0
        
        # Check for sale records (uses pig_id)
        cursor.execute("SELECT COUNT(*) as count FROM sale_records WHERE pig_id = %s", (pig_id,))
        sale_result = cursor.fetchone()
        sale_count = sale_result['count'] if sale_result else 0
        
        # Delete related records first (in order of dependencies)
        if weight_count > 0:
            cursor.execute("DELETE FROM weight_records WHERE animal_id = %s", (pig_id,))
            print(f"Deleted {weight_count} weight records for pig {pig['tag_id']}")
        
        if slaughter_count > 0:
            cursor.execute("DELETE FROM slaughter_records WHERE pig_id = %s", (pig_id,))
            print(f"Deleted {slaughter_count} slaughter records for pig {pig['tag_id']}")
        
        if death_count > 0:
            cursor.execute("DELETE FROM dead_pigs WHERE pig_id = %s", (pig_id,))
            print(f"Deleted {death_count} death records for pig {pig['tag_id']}")
        
        if sale_count > 0:
            cursor.execute("DELETE FROM sale_records WHERE pig_id = %s", (pig_id,))
            print(f"Deleted {sale_count} sale records for pig {pig['tag_id']}")
        
        # Delete the pig
        cursor.execute("DELETE FROM pigs WHERE id = %s", (pig_id,))
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Pig not found or already deleted'})
        
        # Log the deletion activity
        log_activity(session['employee_id'], 'PIG_DELETED', 
                   f'Pig deleted: {pig["tag_id"]} (ID: {pig_id}) - Type: {pig["pig_type"]}, Breed: {pig["breed"]}, Status: {pig["status"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Pig {pig["tag_id"]} has been successfully deleted from the system'
        })
        
    except Exception as e:
        print(f"Error deleting pig: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Failed to delete pig: {str(e)}'})

@app.route('/api/pig/validate-tag-id', methods=['POST'])
def validate_pig_tag_id():
    """Validate pig tag ID and check for duplicates"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        tag_id = data.get('tag_id')
        current_pig_id = data.get('current_pig_id')  # To exclude current pig from duplicate check
        
        if not tag_id:
            return jsonify({'success': False, 'message': 'Tag ID is required'})
        
        # Validate tag ID format (letter + numbers)
        if not tag_id[0].isalpha() or not tag_id[1:].isdigit():
            return jsonify({'success': False, 'message': 'Tag ID must start with a letter followed by numbers'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for duplicates (excluding current pig if editing)
        if current_pig_id:
            cursor.execute("""
                SELECT id FROM pigs WHERE tag_id = %s AND id != %s
            """, (tag_id, current_pig_id))
        else:
            cursor.execute("""
                SELECT id FROM pigs WHERE tag_id = %s
            """, (tag_id,))
        
        existing_pig = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if existing_pig:
            return jsonify({
                'success': False, 
                'message': f'Tag ID "{tag_id}" already exists. Please choose a different one.'
            })
        
        return jsonify({
            'success': True,
            'message': f'Tag ID "{tag_id}" is available'
        })
        
    except Exception as e:
        print(f"Error validating tag ID: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to validate tag ID: {str(e)}'})

@app.route('/api/pig/recalculate-ages', methods=['POST'])
def recalculate_pig_ages():
    """Recalculate ages for all pigs based on birth dates"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all pigs with birth dates
        cursor.execute("""
            SELECT id, birth_date, age_days FROM pigs 
            WHERE birth_date IS NOT NULL AND status = 'active'
        """)
        pigs = cursor.fetchall()
        
        updated_count = 0
        for pig in pigs:
            if pig['birth_date']:
                try:
                    birth_dt = datetime.strptime(str(pig['birth_date']), '%Y-%m-%d').date()
                    age_days = (datetime.now().date() - birth_dt).days
                    
                    # Update age if it's different
                    if pig['age_days'] != age_days:
                        cursor.execute("""
                            UPDATE pigs SET age_days = %s WHERE id = %s
                        """, (age_days, pig['id']))
                        updated_count += 1
                        
                except ValueError:
                    continue  # Skip invalid dates
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Updated ages for {updated_count} pigs'
        })
        
    except Exception as e:
        print(f"Error recalculating pig ages: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to recalculate ages: {str(e)}'})

@app.route('/api/pig/audit-trail/<int:pig_id>', methods=['GET'])
def get_pig_audit_trail(pig_id):
    """Get audit trail for a specific pig"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get pig basic info
        cursor.execute("""
            SELECT p.*, f.farm_name 
            FROM pigs p 
            LEFT JOIN farms f ON p.farm_id = f.id 
            WHERE p.id = %s
        """, (pig_id,))
        pig = cursor.fetchone()
        
        if not pig:
            return jsonify({'success': False, 'message': 'Pig not found'}), 404
        
        # Get activity log entries for this pig
        cursor.execute("""
            SELECT al.*, e.full_name, e.role
            FROM activity_log al
            LEFT JOIN employees e ON al.employee_id = e.id
            WHERE al.action IN ('PIG_REGISTRATION', 'PIG_UPDATE', 'PIG_STATUS_CHANGE', 'BREEDING_REGISTRATION', 'BREEDING_CANCELLATION', 'FARROWING_ACTIVITY_COMPLETED', 'BREEDING_CYCLE_COMPLETED', 'LITTER_REGISTRATION', 'LITTER_POSTPONED', 'LOGIN', 'LOGOUT')
            AND al.description LIKE %s
            ORDER BY al.created_at DESC
        """, (f'%{pig["tag_id"]}%',))
        
        audit_entries = cursor.fetchall()
        
        # Format audit entries
        formatted_entries = []
        for entry in audit_entries:
            formatted_entries.append({
                'timestamp': entry['created_at'].strftime('%Y-%m-%d %H:%M:%S') if entry['created_at'] else 'Unknown',
                'activity_type': entry['action'],
                'description': entry['description'],
                'employee_name': entry['full_name'] or 'Unknown',
                'employee_role': entry['role'] or 'Unknown'
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'pig': pig,
            'audit_trail': formatted_entries
        })
        
    except Exception as e:
        print(f"Error getting pig audit trail: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get audit trail: {str(e)}'})

@app.route('/api/pigs/sows', methods=['GET'])
def get_sows():
    """Get all sows (female pigs) for breeding"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all sows (female pigs with breeding purpose)
        cursor.execute("""
            SELECT p.id, p.tag_id, p.breed, p.farm_id, f.farm_name
            FROM pigs p
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE p.gender = 'female' 
            AND p.purpose = 'breeding'
            AND p.status = 'active'
            ORDER BY p.tag_id
        """)
        
        sows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'sows': sows
        })
        
    except Exception as e:
        print(f"Error getting sows: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get sows: {str(e)}'})

# Litter Management API Endpoints

@app.route('/api/litter/next-id', methods=['GET'])
def get_next_litter_id():
    """Get next available litter ID"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the highest existing litter ID
        cursor.execute("""
            SELECT litter_id FROM litters 
            ORDER BY CAST(SUBSTRING(litter_id, 2) AS UNSIGNED) DESC 
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        
        if result:
            # Extract number from existing ID (e.g., L001 -> 1)
            current_number = int(result['litter_id'][1:])
            next_number = current_number + 1
        else:
            # No existing litters, start with 1
            next_number = 1
        
        # Format as L001, L002, etc.
        next_litter_id = f"L{next_number:03d}"
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'next_litter_id': next_litter_id
        })
        
    except Exception as e:
        print(f"Error getting next litter ID: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get next litter ID: {str(e)}'})

@app.route('/api/litter/register', methods=['POST'])
def register_litter():
    """Register a new litter"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['sow_id', 'farrowing_record_id', 'litter_id', 'farrowing_date', 'total_piglets', 'alive_piglets']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if litter ID already exists
        cursor.execute("SELECT id FROM litters WHERE litter_id = %s", (data['litter_id'],))
        if cursor.fetchone():
            return jsonify({'success': False, 'message': 'Litter ID already exists'})
        
        # Get weaning data from farrowing activities if available
        weaning_weight = None
        weaning_date = None
        
        cursor.execute("""
            SELECT weaning_weight, weaning_date 
            FROM farrowing_activities 
            WHERE farrowing_record_id = %s AND activity_name = 'Weaning' AND completed = TRUE
        """, (data['farrowing_record_id'],))
        
        weaning_data = cursor.fetchone()
        if weaning_data:
            weaning_weight = weaning_data['weaning_weight']
            weaning_date = weaning_data['weaning_date']
        
        # Insert litter record
        cursor.execute("""
            INSERT INTO litters (
                litter_id, farrowing_record_id, sow_id, boar_id, farrowing_date,
                total_piglets, alive_piglets, still_births, avg_weight, 
                weaning_weight, weaning_date, notes, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['litter_id'], data['farrowing_record_id'], data['sow_id'], 
            data.get('boar_id'), data['farrowing_date'], data['total_piglets'],
            data['alive_piglets'], data.get('still_births', 0), data.get('avg_weight'),
            weaning_weight, weaning_date, data.get('notes'), session['employee_id']
        ))
        
        litter_id = cursor.lastrowid
        
        # Get breeding record ID from farrowing record
        cursor.execute("""
            SELECT breeding_id FROM farrowing_records WHERE id = %s
        """, (data['farrowing_record_id'],))
        
        breeding_record = cursor.fetchone()
        if not breeding_record:
            return jsonify({'success': False, 'message': 'Breeding record not found for this farrowing'})
        
        # Update breeding record status to completed
        cursor.execute("""
            UPDATE breeding_records 
            SET status = 'completed', completed_date = %s 
            WHERE id = %s
        """, (data['farrowing_date'], breeding_record['breeding_id']))
        
        # Update sow breeding status back to available
        cursor.execute("""
            UPDATE pigs 
            SET breeding_status = 'available' 
            WHERE id = %s
        """, (data['sow_id']))
        
        # Log activity
        log_activity(session['employee_id'], 'LITTER_REGISTRATION', 
                   f'Registered litter {data["litter_id"]} with {data["alive_piglets"]} alive piglets from sow {data["sow_id"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Litter {data["litter_id"]} registered successfully with {data["alive_piglets"]} alive piglets'
        })
        
    except Exception as e:
        print(f"Error registering litter: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to register litter: {str(e)}'})

@app.route('/api/litter/postpone', methods=['POST'])
def postpone_litter_registration():
    """Postpone litter registration with reason"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        if not data.get('reason'):
            return jsonify({'success': False, 'message': 'Reason for postponement is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding record ID from farrowing record
        cursor.execute("""
            SELECT breeding_id FROM farrowing_records WHERE id = %s
        """, (data['farrowing_record_id'],))
        
        breeding_record = cursor.fetchone()
        if not breeding_record:
            return jsonify({'success': False, 'message': 'Breeding record not found for this farrowing'})
        
        # Update breeding record status to postponed
        cursor.execute("""
            UPDATE breeding_records 
            SET status = 'postponed', notes = %s 
            WHERE id = %s
        """, (data['reason'], breeding_record['breeding_id']))
        
        # Log activity
        log_activity(session['employee_id'], 'LITTER_POSTPONED', 
                   f'Postponed litter registration for farrowing record {data["farrowing_record_id"]}. Reason: {data["reason"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Litter registration postponed successfully'
        })
        
    except Exception as e:
        print(f"Error postponing litter registration: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to postpone litter registration: {str(e)}'})

# Breeding Management API Endpoints

@app.route('/api/breeding/available-sows', methods=['GET'])
def get_available_sows():
    """Get all sows available for breeding (breeding_status = 'available')"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get sows that are available for breeding
        cursor.execute("""
            SELECT p.id, p.tag_id, p.breed, p.age_days, f.farm_name
            FROM pigs p 
            LEFT JOIN farms f ON p.farm_id = f.id 
            WHERE p.gender = 'female' 
            AND p.pig_type = 'grown_pig' 
            AND p.purpose = 'breeding' 
            AND p.breeding_status = 'available'
            AND p.status = 'active'
            ORDER BY p.tag_id
        """)
        sows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'sows': sows
        })
        
    except Exception as e:
        print(f"Error getting available sows: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/breeding/completed-cycles', methods=['GET'])
def get_completed_breeding_cycles():
    """Get breeding records that have completed their cycle and need litter registration"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding records that are ready for litter registration
        # This includes records that are past expected farrowing date and still marked as 'pregnant'
        cursor.execute("""
            SELECT br.*, 
                   sow.tag_id as sow_tag_id, sow.breed as sow_breed, sow.age_days as sow_age_days,
                   sow.farm_id as sow_farm_id, f.farm_name as sow_farm_name,
                   boar.tag_id as boar_tag_id
            FROM breeding_records br
            LEFT JOIN pigs sow ON br.sow_id = sow.id
            LEFT JOIN pigs boar ON br.boar_id = boar.id
            LEFT JOIN farms f ON sow.farm_id = f.id
            WHERE br.status = 'pregnant' 
            AND br.expected_farrowing <= CURDATE()
            AND br.expected_farrowing >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            AND NOT EXISTS (
                SELECT 1 FROM litters l WHERE l.farrowing_record_id = br.id
            )
            ORDER BY br.expected_farrowing ASC
        """)
        
        completed_cycles = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'completed_cycles': completed_cycles
        })
        
    except Exception as e:
        print(f"Error getting completed breeding cycles: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/breeding/available-boars', methods=['GET'])
def get_available_boars():
    """Get all boars available for breeding"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get boars that are available for breeding
        cursor.execute("""
            SELECT p.id, p.tag_id, p.breed, p.age_days, f.farm_name
            FROM pigs p 
            LEFT JOIN farms f ON p.farm_id = f.id 
            WHERE p.gender = 'male' 
            AND p.pig_type = 'grown_pig' 
            AND p.purpose = 'breeding' 
            AND p.status = 'active'
            ORDER BY p.tag_id
        """)
        boars = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'boars': boars
        })
        
    except Exception as e:
        print(f"Error getting available boars: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/breeding/register', methods=['POST'])
def register_breeding():
    """Register a new breeding record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        sow_id = data.get('sow_id')
        boar_id = data.get('boar_id')
        mating_date = data.get('mating_date')
        notes = data.get('notes', '')
        
        # Validation
        if not all([sow_id, boar_id, mating_date]):
            return jsonify({'success': False, 'message': 'All fields are required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verify sow is available for breeding
        cursor.execute("""
            SELECT id, tag_id, breeding_status 
            FROM pigs 
            WHERE id = %s AND gender = 'female' AND pig_type = 'grown_pig' 
            AND purpose = 'breeding' AND breeding_status = 'available' AND status = 'active'
        """, (sow_id,))
        sow = cursor.fetchone()
        
        if not sow:
            return jsonify({'success': False, 'message': 'Selected sow is not available for breeding'})
        
        # Verify boar exists and is male
        cursor.execute("""
            SELECT id, tag_id 
            FROM pigs 
            WHERE id = %s AND gender = 'male' AND pig_type = 'grown_pig' 
            AND purpose = 'breeding' AND status = 'active'
        """, (boar_id,))
        boar = cursor.fetchone()
        
        if not boar:
            return jsonify({'success': False, 'message': 'Selected boar is not valid'})
        
        # Calculate expected due date (114 days from mating)
        from datetime import datetime, timedelta
        mating_dt = datetime.strptime(mating_date, '%Y-%m-%d')
        expected_due_date = mating_dt + timedelta(days=114)
        
        # Insert breeding record
        cursor.execute("""
            INSERT INTO breeding_records (sow_id, boar_id, mating_date, expected_due_date, notes, created_by)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (sow_id, boar_id, mating_date, expected_due_date.date(), notes, session['employee_id']))
        
        breeding_id = cursor.lastrowid
        
        # Update sow's breeding status to 'served'
        cursor.execute("""
            UPDATE pigs 
            SET breeding_status = 'served' 
            WHERE id = %s
        """, (sow_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Log activity
        log_activity(session['employee_id'], 'BREEDING_REGISTRATION', 
                    f'Breeding record created: Sow {sow["tag_id"]} with Boar {boar["tag_id"]}')
        
        return jsonify({
            'success': True,
            'message': 'Breeding record registered successfully',
            'breeding_id': breeding_id,
            'expected_due_date': expected_due_date.date().isoformat()
        })
        
    except Exception as e:
        print(f"Breeding registration error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Failed to register breeding: {str(e)}'})

@app.route('/api/breeding/list', methods=['GET'])
def get_breeding_records():
    """Get all breeding records with calculated status and days to farrowing"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all breeding records with pig and farm information
        cursor.execute("""
            SELECT br.*, 
                   sow.tag_id as sow_tag_id, sow.breed as sow_breed, sow.breeding_status,
                   boar.tag_id as boar_tag_id, boar.breed as boar_breed,
                   e.full_name as created_by_name
            FROM breeding_records br
            LEFT JOIN pigs sow ON br.sow_id = sow.id
            LEFT JOIN pigs boar ON br.boar_id = boar.id
            LEFT JOIN employees e ON br.created_by = e.id
            ORDER BY br.created_at DESC
        """)
        records = cursor.fetchall()
        
        # Process records using actual breeding status from pigs table
        from datetime import datetime
        today = datetime.now().date()
        
        # Auto-update breeding statuses based on time
        for record in records:
            if record['mating_date'] and record['breeding_status'] == 'served':
                mating_date = record['mating_date']
                days_since_mating = (today - mating_date).days
                
                # If more than 25 days have passed since mating, update to pregnant
                if days_since_mating > 25:
                    cursor.execute("""
                        UPDATE pigs 
                        SET breeding_status = 'pregnant' 
                        WHERE id = %s
                    """, (record['sow_id'],))
                    record['breeding_status'] = 'pregnant'
        
        processed_records = []
        for record in records:
            record_dict = dict(record)
            
            # Use the actual breeding_status from the pigs table
            breeding_status = record['breeding_status']
            record_dict['breeding_status'] = breeding_status
            
            # Calculate days to farrowing (pregnancy is 114 days)
            if record['mating_date']:
                mating_date = record['mating_date']
                days_since_mating = (today - mating_date).days
                days_to_farrowing = 114 - days_since_mating
                
                # Calculate expected due date (114 days from mating)
                from datetime import timedelta
                expected_due_date = mating_date + timedelta(days=114)
                record_dict['expected_due_date'] = expected_due_date
                
                # Determine if can cancel based on breeding status and days
                # Can only cancel within 25 days of mating (served status)
                days_since_mating = (datetime.now().date() - record['mating_date']).days
                if breeding_status == 'served' and days_since_mating <= 25:
                    record_dict['can_cancel'] = True
                    record_dict['days_remaining_to_cancel'] = 25 - days_since_mating
                elif breeding_status == 'pregnant':
                    record_dict['can_cancel'] = False
                    record_dict['days_remaining_to_cancel'] = 0
                else:
                    record_dict['can_cancel'] = False
                    record_dict['days_remaining_to_cancel'] = 0
                
                record_dict['days_to_farrowing'] = max(0, days_to_farrowing)
            else:
                record_dict['can_cancel'] = False
                record_dict['days_to_farrowing'] = None
                record_dict['expected_due_date'] = None
            
            processed_records.append(record_dict)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': processed_records
        })
        
    except Exception as e:
        print(f"Error getting breeding records: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/breeding/cancel/<int:breeding_id>', methods=['POST'])
def cancel_breeding(breeding_id):
    """Cancel a breeding record (within 93 days to farrowing)"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        reason = data.get('reason', 'No reason provided')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding record
        cursor.execute("""
            SELECT br.*, sow.tag_id as sow_tag_id, sow.breeding_status
            FROM breeding_records br
            LEFT JOIN pigs sow ON br.sow_id = sow.id
            WHERE br.id = %s
        """, (breeding_id,))
        record = cursor.fetchone()
        
        if not record:
            return jsonify({'success': False, 'message': 'Breeding record not found'})
        
        # Check if within 25 days of mating
        from datetime import datetime
        mating_date = record['mating_date']
        days_since_mating = (datetime.now().date() - mating_date).days
        
        if days_since_mating > 25:  # More than 25 days since mating = pregnant
            return jsonify({'success': False, 'message': 'Cannot cancel breeding after 25 days from mating'})
        
        # Move breeding record to failed_conceptions table
        cursor.execute("""
            INSERT INTO failed_conceptions (sow_id, boar_id, mating_date, failure_date, failure_reason, notes, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (record['sow_id'], record['boar_id'], record['mating_date'], datetime.now().date(), reason, record['notes'] or '', session['employee_id']))
        
        # Remove the breeding record from breeding_records table
        cursor.execute("DELETE FROM breeding_records WHERE id = %s", (breeding_id,))
        
        # Update sow's breeding status back to available
        cursor.execute("""
            UPDATE pigs 
            SET breeding_status = 'available' 
            WHERE id = %s
        """, (record['sow_id'],))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Log activity
        log_activity(session['employee_id'], 'BREEDING_CANCELLATION', 
                    f'Breeding cancelled: Sow {record["sow_tag_id"]} - Reason: {reason}')
        
        return jsonify({
            'success': True,
            'message': 'Breeding record cancelled successfully'
        })
        
    except Exception as e:
        print(f"Breeding cancellation error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Failed to cancel breeding: {str(e)}'})

@app.route('/api/breeding/failed-conceptions', methods=['GET'])
def get_failed_conceptions():
    """Get all failed conception records"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all failed conception records with pig and farm information
        cursor.execute("""
            SELECT fc.*, 
                   sow.tag_id as sow_tag_id, sow.breed as sow_breed,
                   boar.tag_id as boar_tag_id, boar.breed as boar_breed,
                   e.full_name as created_by_name
            FROM failed_conceptions fc
            LEFT JOIN pigs sow ON fc.sow_id = sow.id
            LEFT JOIN pigs boar ON fc.boar_id = boar.id
            LEFT JOIN employees e ON fc.created_by = e.id
            ORDER BY fc.created_at DESC
        """)
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': records
        })
        
    except Exception as e:
        print(f"Error getting failed conceptions: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/breeding/<int:breeding_id>/edit', methods=['PUT'])
def edit_breeding_record(breeding_id):
    """Edit a breeding record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        mating_date = data.get('mating_date')
        expected_due_date = data.get('expected_due_date')
        breeding_status = data.get('breeding_status')
        
        if not mating_date:
            return jsonify({'success': False, 'message': 'Mating date is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Update breeding record
        cursor.execute("""
            UPDATE breeding_records 
            SET mating_date = %s, expected_due_date = %s, status = %s, updated_at = NOW()
            WHERE id = %s
        """, (mating_date, expected_due_date, breeding_status, breeding_id))
        
        # Update sow status based on breeding status
        if breeding_status == 'cancelled':
            cursor.execute("""
                UPDATE pigs 
                SET status = 'available' 
                WHERE id = (SELECT sow_id FROM breeding_records WHERE id = %s)
            """, (breeding_id,))
        elif breeding_status in ['served', 'pregnant']:
            cursor.execute("""
                UPDATE pigs 
                SET status = 'breeding' 
                WHERE id = (SELECT sow_id FROM breeding_records WHERE id = %s)
            """, (breeding_id,))
        elif breeding_status == 'completed':
            cursor.execute("""
                UPDATE pigs 
                SET status = 'available' 
                WHERE id = (SELECT sow_id FROM breeding_records WHERE id = %s)
            """, (breeding_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Log activity
        log_activity(session['employee_id'], 'BREEDING_EDIT', 
                    f'Breeding record {breeding_id} updated')
        
        return jsonify({
            'success': True,
            'message': 'Breeding record updated successfully'
        })
        
    except Exception as e:
        print(f"Error updating breeding record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to update breeding record: {str(e)}'})

@app.route('/api/breeding/<int:breeding_id>/delete', methods=['DELETE'])
def delete_breeding_record(breeding_id):
    """Delete a breeding record and set sow status to available"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding record details for logging
        cursor.execute("""
            SELECT br.sow_id, p.tag_id as sow_tag_id
            FROM breeding_records br
            LEFT JOIN pigs p ON br.sow_id = p.id
            WHERE br.id = %s
        """, (breeding_id,))
        
        record = cursor.fetchone()
        if not record:
            return jsonify({'success': False, 'message': 'Breeding record not found'})
        
        # Update sow status to available
        cursor.execute("""
            UPDATE pigs 
            SET status = 'available' 
            WHERE id = %s
        """, (record['sow_id'],))
        
        # Delete related litters first (they reference farrowing_records)
        cursor.execute("""
            DELETE l FROM litters l 
            JOIN farrowing_records fr ON l.farrowing_record_id = fr.id 
            WHERE fr.breeding_id = %s
        """, (breeding_id,))
        print(f"Deleted litters for breeding record {breeding_id}")
        
        # Delete related farrowing records (they reference breeding_records)
        cursor.execute("DELETE FROM farrowing_records WHERE breeding_id = %s", (breeding_id,))
        print(f"Deleted farrowing records for breeding record {breeding_id}")
        
        # Temporarily disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        try:
            # Delete breeding record
            cursor.execute("DELETE FROM breeding_records WHERE id = %s", (breeding_id,))
        finally:
            # Always re-enable foreign key checks, even if an error occurs
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Log activity
        log_activity(session['employee_id'], 'BREEDING_DELETE', 
                    f'Breeding record {breeding_id} deleted for sow {record["sow_tag_id"]}')
        
        return jsonify({
            'success': True,
            'message': 'Breeding record deleted successfully. Sow is now available for breeding.'
        })
        
    except Exception as e:
        print(f"Error deleting breeding record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to delete breeding record: {str(e)}'})

@app.route('/api/breeding/completed-farrowings', methods=['GET'])
def get_completed_farrowings():
    """Get all completed farrowing records with unweaned litters only"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get completed farrowing records with unweaned litters only
        # Only show pigs whose breeding status is 'farrowed' and have unweaned litters
        cursor.execute("""
            SELECT fr.id, fr.breeding_id, fr.farrowing_date, fr.alive_piglets, fr.still_births,
                   fr.dead_piglets, fr.weak_piglets, fr.avg_weight, fr.health_notes, fr.notes,
                   fr.created_by, fr.created_at, fr.updated_at,
                   br.mating_date,
                   sow.tag_id as sow_tag_id, sow.breed as sow_breed,
                   boar.tag_id as boar_tag_id, boar.breed as boar_breed,
                   e.full_name as created_by_name,
                   l.litter_id, l.total_piglets, l.alive_piglets, l.still_births,
                   l.avg_weight, l.weaning_weight, l.weaning_date, l.status as litter_status,
                   l.notes as litter_notes,
                   CASE WHEN EXISTS(SELECT 1 FROM farrowing_records_edit_history WHERE record_id = fr.id) THEN 1 ELSE 0 END as is_edited
            FROM farrowing_records fr
            JOIN breeding_records br ON fr.breeding_id = br.id
            LEFT JOIN pigs sow ON br.sow_id = sow.id
            LEFT JOIN pigs boar ON br.boar_id = boar.id
            LEFT JOIN employees e ON fr.created_by = e.id
            LEFT JOIN litters l ON fr.id = l.farrowing_record_id
            WHERE sow.breeding_status = 'farrowed' 
            AND l.status = 'unweaned'
            ORDER BY fr.farrowing_date DESC
        """)
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': records
        })
        
    except Exception as e:
        print(f"Error getting completed farrowings: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/breeding/statistics', methods=['GET'])
def get_breeding_statistics():
    """Get breeding statistics including success and failure rates"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding records count by status
        cursor.execute("""
            SELECT 
                COUNT(CASE WHEN status = 'served' THEN 1 END) as served_count,
                COUNT(CASE WHEN status = 'pregnant' THEN 1 END) as pregnant_count,
                COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_count
            FROM breeding_records
        """)
        breeding_stats = cursor.fetchone()
        
        # Get failed conceptions count
        cursor.execute("SELECT COUNT(*) as failed_count FROM failed_conceptions")
        failed_count = cursor.fetchone()['failed_count']
        
        # Calculate success rate
        total_attempts = (breeding_stats['served_count'] or 0) + (breeding_stats['pregnant_count'] or 0) + failed_count
        success_rate = 0
        if total_attempts > 0:
            success_rate = round(((breeding_stats['pregnant_count'] or 0) / total_attempts) * 100, 2)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'statistics': {
                'served_count': breeding_stats['served_count'] or 0,
                'pregnant_count': breeding_stats['pregnant_count'] or 0,
                'cancelled_count': breeding_stats['cancelled_count'] or 0,
                'failed_count': failed_count,
                'total_attempts': total_attempts,
                'success_rate': success_rate
            }
        })
        
    except Exception as e:
        print(f"Error getting breeding statistics: {str(e)}")
        return jsonify({'error': str(e)}), 500

def update_breeding_statuses():
    """Update breeding statuses based on time elapsed"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding records that need status updates
        cursor.execute("""
            SELECT br.id, br.sow_id, br.mating_date, br.status, sow.tag_id
            FROM breeding_records br
            LEFT JOIN pigs sow ON br.sow_id = sow.id
            WHERE br.status = 'served'
        """)
        records = cursor.fetchall()
        
        from datetime import datetime
        today = datetime.now().date()
        updated_count = 0
        
        for record in records:
            days_since_mating = (today - record['mating_date']).days
            
            # If more than 25 days have passed, change status to pregnant
            if days_since_mating > 25:
                cursor.execute("""
                    UPDATE breeding_records 
                    SET status = 'pregnant' 
                    WHERE id = %s
                """, (record['id'],))
                
                # Update sow's breeding status to pregnant
                cursor.execute("""
                    UPDATE pigs 
                    SET breeding_status = 'pregnant' 
                    WHERE id = %s
                """, (record['sow_id'],))
                
                updated_count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        if updated_count > 0:
            print(f"✅ Updated {updated_count} breeding statuses to pregnant")
        
    except Exception as e:
        print(f"❌ Error updating breeding statuses: {e}")
        import traceback
        traceback.print_exc()

@app.route('/api/breeding/register-farrowing/<int:breeding_id>', methods=['POST'])
def register_farrowing(breeding_id):
    """Register farrowing for a breeding record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        from datetime import timedelta
        data = request.get_json()
        farrowing_date = data.get('farrowing_date')
        alive_piglets = data.get('alive_piglets')
        still_births = data.get('still_births')
        avg_weight = data.get('avg_weight')
        health_notes = data.get('health_notes', '')
        
        if not all([farrowing_date, alive_piglets is not None, still_births is not None, avg_weight is not None]):
            return jsonify({'success': False, 'message': 'Missing required fields'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding record details
        cursor.execute("""
            SELECT br.*, p.tag_id as sow_tag_id, p.breed as sow_breed
            FROM breeding_records br
            JOIN pigs p ON br.sow_id = p.id
            WHERE br.id = %s
        """, (breeding_id,))
        
        breeding_record = cursor.fetchone()
        if not breeding_record:
            return jsonify({'success': False, 'message': 'Breeding record not found'})
        
        # Insert farrowing record
        cursor.execute("""
            INSERT INTO farrowing_records (
                breeding_id, farrowing_date, alive_piglets, still_births, 
                avg_weight, health_notes, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            breeding_id, farrowing_date, alive_piglets, still_births,
            avg_weight, health_notes, session['employee_id']
        ))
        
        farrowing_id = cursor.lastrowid
        
        # Create farrowing activities with due dates
        activities = [
            (1, 'Clear airways, ensure colostrum intake'),
            (1, 'Provide heat lamps'),
            (1, 'Remove afterbirth'),
            (2, 'Iron injections'),
            (2, 'Ear notching/tagging'),
            (3, 'Tail docking'),
            (3, 'Castration (males)'),
            (14, 'Start creep feed'),
            (21, 'Weaning')
        ]
        
        for due_day, activity_name in activities:
            # Convert farrowing_date string to date object for timedelta calculation
            farrowing_date_obj = datetime.strptime(farrowing_date, '%Y-%m-%d').date()
            due_date = farrowing_date_obj + timedelta(days=due_day)
            cursor.execute("""
                INSERT INTO farrowing_activities (
                    farrowing_record_id, activity_name, due_day, due_date
                ) VALUES (%s, %s, %s, %s)
            """, (farrowing_id, activity_name, due_day, due_date))
        
        # Update breeding record status to completed
        print(f"Updating breeding record {breeding_id} status to 'completed'")
        cursor.execute("""
            UPDATE breeding_records 
            SET status = 'completed', updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (breeding_id,))
        
        # Create litter record
        litter_id = generate_litter_id()
        total_piglets = alive_piglets + still_births
        
        cursor.execute("""
            INSERT INTO litters (
                litter_id, farrowing_record_id, sow_id, boar_id, farrowing_date,
                total_piglets, alive_piglets, still_births, avg_weight, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            litter_id, farrowing_id, breeding_record['sow_id'], breeding_record['boar_id'],
            farrowing_date, total_piglets, alive_piglets, still_births, avg_weight, session['employee_id']
        ))
        
        print(f"📝 Created litter record: {litter_id}")
        
        # Update sow's breeding status to farrowed
        print(f"Updating sow {breeding_record['sow_id']} breeding status to 'farrowed'")
        # Note: Sows with 'farrowed' status need recovery time before being bred again
        # This status can be manually changed back to 'available' by farm managers
        # or automatically after a set recovery period (typically 2-3 months)
        cursor.execute("""
            UPDATE pigs 
            SET breeding_status = 'farrowed' 
            WHERE id = %s
        """, (breeding_record['sow_id'],))
        
        # Verify the updates
        cursor.execute("SELECT status FROM breeding_records WHERE id = %s", (breeding_id,))
        updated_breeding = cursor.fetchone()
        print(f"✅ Breeding record status after update: {updated_breeding['status']}")
        
        cursor.execute("SELECT breeding_status FROM pigs WHERE id = %s", (breeding_record['sow_id'],))
        updated_pig = cursor.fetchone()
        print(f"✅ Pig breeding status after update: {updated_pig['breeding_status']}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Farrowing registration completed successfully for breeding record {breeding_id}")
        
        return jsonify({
            'success': True,
            'message': 'Farrowing registered successfully'
        })
        
    except Exception as e:
        print(f"Farrowing registration error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Failed to register farrowing: {str(e)}'})

@app.route('/api/farrowing/activities/<int:farrowing_id>', methods=['GET'])
def get_farrowing_activities(farrowing_id):
    """Get farrowing activities for a specific farrowing record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get farrowing activities with completion status
        cursor.execute("""
            SELECT fa.*, e.full_name as completed_by_name
            FROM farrowing_activities fa
            LEFT JOIN employees e ON fa.completed_by = e.id
            WHERE fa.farrowing_record_id = %s
            ORDER BY fa.due_day ASC
        """, (farrowing_id,))
        
        activities = cursor.fetchall()
        
        # Auto-complete activities that have reached their exact due day
        today = datetime.now().date()
        for activity in activities:
            if not activity['completed'] and activity['due_date'] == today:
                cursor.execute("""
                    UPDATE farrowing_activities 
                    SET completed = TRUE, completed_date = %s, 
                        notes = CONCAT(COALESCE(notes, ''), ' - Auto-completed on due day ', %s)
                    WHERE id = %s
                """, (today, today, activity['id']))
                activity['completed'] = True
                activity['completed_date'] = today
                activity['notes'] = f"Auto-completed on due day {today}"
        
        # Refresh activities after auto-completion
        if any(not a['completed'] and a['due_date'] == today for a in activities):
            cursor.execute("""
                SELECT fa.*, e.full_name as completed_by_name
                FROM farrowing_activities fa
                LEFT JOIN employees e ON fa.completed_by = e.id
                WHERE fa.farrowing_record_id = %s
                ORDER BY fa.due_day ASC
            """, (farrowing_id,))
            activities = cursor.fetchall()
        
        # Convert to list of dictionaries
        activities_list = []
        for activity in activities:
            activities_list.append({
                'id': activity['id'],
                'activity_name': activity['activity_name'],
                'due_day': activity['due_day'],
                'due_date': activity['due_date'].isoformat() if activity['due_date'] else None,
                'completed': bool(activity['completed']),
                'completed_date': activity['completed_date'].isoformat() if activity['completed_date'] else None,
                'completed_by': activity['completed_by_name'],
                'notes': activity['notes'],
                'weaning_weight': float(activity['weaning_weight']) if activity['weaning_weight'] else None,
                'weaning_date': activity['weaning_date'].isoformat() if activity['weaning_date'] else None
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'activities': activities_list
        })
        
    except Exception as e:
        print(f"Error getting farrowing activities: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get activities: {str(e)}'})

@app.route('/api/farrowing/activities/<int:activity_id>/complete', methods=['POST'])
def complete_farrowing_activity(activity_id):
    """Mark a farrowing activity as completed"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        notes = data.get('notes', '')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, get the activity details to check if it can be completed
        cursor.execute("""
            SELECT fa.*, fr.farrowing_date 
            FROM farrowing_activities fa
            JOIN farrowing_records fr ON fa.farrowing_record_id = fr.id
            WHERE fa.id = %s
        """, (activity_id,))
        
        activity = cursor.fetchone()
        if not activity:
            return jsonify({'success': False, 'message': 'Activity not found'})
        
        # Check if the activity is already completed
        if activity['completed']:
            return jsonify({'success': False, 'message': 'Activity is already completed'})
        
        # Check if the current date has reached the activity day
        from datetime import datetime, timedelta
        today = datetime.now().date()
        farrowing_date = activity['farrowing_date']
        activity_day = activity['due_day']
        due_date = farrowing_date + timedelta(days=activity_day)
        
        if today < due_date:
            return jsonify({
                'success': False, 
                'message': f'Cannot complete activity before Day {activity_day}. This activity is due on {due_date.strftime("%Y-%m-%d")}'
            })
        
        # Check if activity is overdue and add warning
        is_overdue = today > due_date
        overdue_warning = ""
        if is_overdue:
            days_overdue = (today - due_date).days
            overdue_warning = f" (Note: This activity is {days_overdue} day(s) overdue)"
        
        # Check if this is a weaning activity and requires additional data
        weaning_weight = None
        weaning_date = None
        
        if activity['activity_name'] == 'Weaning':
            weaning_weight = data.get('weaning_weight')
            weaning_date = data.get('weaning_date')
            
            if not weaning_weight:
                return jsonify({'success': False, 'message': 'Weaning weight is required for weaning activity'})
            
            if not weaning_date:
                return jsonify({'success': False, 'message': 'Weaning date and time is required for weaning activity'})
        
        # Mark activity as completed
        cursor.execute("""
            UPDATE farrowing_activities 
            SET completed = TRUE, completed_date = CURRENT_DATE, 
                completed_by = %s, notes = %s, weaning_weight = %s, weaning_date = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (session['employee_id'], notes, weaning_weight, weaning_date, activity_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Log activity
        log_activity(session['employee_id'], 'FARROWING_ACTIVITY_COMPLETED', 
                    f'Completed farrowing activity ID {activity_id} on {today}')
        
        # Check if all activities are completed and trigger recovery period
        if is_overdue:
            check_and_trigger_recovery_period(activity['farrowing_record_id'])
        
        # Check if all activities are completed and update litter status to weaned
        check_and_update_litter_status(activity['farrowing_record_id'])
        
        return jsonify({
            'success': True,
            'message': f'Activity "Day {activity_day}: {activity["activity_name"]}" marked as completed{overdue_warning}'
        })
        
    except Exception as e:
        print(f"Error completing farrowing activity: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to complete activity: {str(e)}'})

@app.route('/api/farrowing/complete-activity/<int:activity_id>', methods=['POST'])
def complete_farrowing_activity_simple(activity_id):
    """Simple complete farrowing activity endpoint for compatibility"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the farrowing record ID for this activity
        cursor.execute("""
            SELECT farrowing_record_id FROM farrowing_activities WHERE id = %s
        """, (activity_id,))
        
        activity = cursor.fetchone()
        if not activity:
            return jsonify({'success': False, 'message': 'Activity not found'})
        
        # Update the activity as completed
        cursor.execute("""
            UPDATE farrowing_activities 
            SET completed = TRUE, completed_date = NOW() 
            WHERE id = %s
        """, (activity_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Check if all activities are completed and update litter status to weaned
        check_and_update_litter_status(activity['farrowing_record_id'])
        
        return jsonify({
            'success': True,
            'message': 'Activity completed successfully'
        })
        
    except Exception as e:
        print(f"Error completing farrowing activity: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/farrowing/check-recovery/<int:farrowing_id>', methods=['GET'])
def check_sow_recovery_status(farrowing_id):
    """Check if sow is ready for next breeding (40 days after farrowing)"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get farrowing details and calculate recovery status
        cursor.execute("""
            SELECT fr.farrowing_date, fr.id, br.sow_id, p.tag_id as sow_tag_id,
                   p.breeding_status, p.breed
            FROM farrowing_records fr
            JOIN breeding_records br ON fr.breeding_id = br.id
            JOIN pigs p ON br.sow_id = p.id
            WHERE fr.id = %s
        """, (farrowing_id,))
        
        farrowing = cursor.fetchone()
        if not farrowing:
            return jsonify({'success': False, 'message': 'Farrowing record not found'})
        
        from datetime import datetime, timedelta
        today = datetime.now().date()
        farrowing_date = farrowing['farrowing_date']
        recovery_date = farrowing_date + timedelta(days=40)
        days_until_recovery = (recovery_date - today).days
        
        # Check if all activities are completed
        cursor.execute("""
            SELECT COUNT(*) as total_activities,
                   SUM(CASE WHEN completed = TRUE THEN 1 ELSE 0 END) as completed_activities
            FROM farrowing_activities 
            WHERE farrowing_record_id = %s
        """, (farrowing_id,))
        
        activities_result = cursor.fetchone()
        all_activities_completed = activities_result['total_activities'] == activities_result['completed_activities']
        
        # Check if sow is ready for next breeding
        sow_ready = today >= recovery_date and all_activities_completed
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'farrowing_date': farrowing_date.isoformat(),
                'recovery_date': recovery_date.isoformat(),
                'days_until_recovery': max(0, days_until_recovery),
                'all_activities_completed': all_activities_completed,
                'sow_ready': sow_ready,
                'sow_tag_id': farrowing['sow_tag_id'],
                'current_breeding_status': farrowing['breeding_status'],
                'sow_breed': farrowing['breed']
            }
        })
        
    except Exception as e:
        print(f"Error checking sow recovery status: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to check recovery status: {str(e)}'})

@app.route('/api/farrowing/mark-sow-available/<int:farrowing_id>', methods=['POST'])
def mark_sow_available_for_breeding(farrowing_id):
    """Mark sow as available for next breeding after recovery period"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get farrowing details
        cursor.execute("""
            SELECT fr.farrowing_date, fr.id, br.sow_id, br.id as breeding_id, p.tag_id as sow_tag_id
            FROM farrowing_records fr
            JOIN breeding_records br ON fr.breeding_id = br.id
            JOIN pigs p ON br.sow_id = p.id
            WHERE fr.id = %s
        """, (farrowing_id,))
        
        farrowing = cursor.fetchone()
        if not farrowing:
            return jsonify({'success': False, 'message': 'Farrowing record not found'})
        
        # Check if recovery period is complete
        from datetime import datetime, timedelta
        today = datetime.now().date()
        farrowing_date = farrowing['farrowing_date']
        recovery_date = farrowing_date + timedelta(days=40)
        
        if today < recovery_date:
            return jsonify({
                'success': False, 
                'message': f'Sow is not ready yet. Recovery period ends on {recovery_date.strftime("%Y-%m-%d")}'
            })
        
        # Check if all activities are completed
        cursor.execute("""
            SELECT COUNT(*) as total_activities,
                   SUM(CASE WHEN completed = TRUE THEN 1 ELSE 0 END) as completed_activities
            FROM farrowing_activities 
            WHERE farrowing_record_id = %s
        """, (farrowing_id,))
        
        activities_result = cursor.fetchone()
        if activities_result['total_activities'] != activities_result['completed_activities']:
            return jsonify({
                'success': False, 
                'message': 'Cannot mark sow as available until all farrowing activities are completed'
            })

        # Check if weaning activity is completed and update litter status
        cursor.execute("""
            SELECT fa.*, l.id as litter_id
            FROM farrowing_activities fa
            JOIN litters l ON l.farrowing_record_id = fa.farrowing_record_id
            WHERE fa.farrowing_record_id = %s AND fa.activity_name = 'Weaning'
        """, (farrowing_id,))
        
        weaning_activity = cursor.fetchone()
        if weaning_activity and weaning_activity['completed']:
            # Update litter status to weaned
            cursor.execute("""
                UPDATE litters 
                SET status = 'weaned', weaning_date = %s, weaning_weight = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (weaning_activity['weaning_date'], weaning_activity['weaning_weight'], weaning_activity['litter_id']))
            print(f"✅ Updated litter {weaning_activity['litter_id']} status to 'weaned'")
        else:
            # Check which activities are not completed
            cursor.execute("""
                SELECT activity_name, due_day, due_date
                FROM farrowing_activities 
                WHERE farrowing_record_id = %s AND completed = FALSE
                ORDER BY due_day ASC
            """, (farrowing_id,))
            
            incomplete_activities = cursor.fetchall()
            if incomplete_activities:
                incomplete_list = [f"Day {activity['due_day']}: {activity['activity_name']}" for activity in incomplete_activities]
                return jsonify({
                    'success': False, 
                    'message': 'Cannot mark sow as available. Incomplete farrowing activities:',
                    'incomplete_activities': incomplete_list
                })
        
        # Check if litter registration is required
        cursor.execute("""
            SELECT COUNT(*) as litter_count
            FROM litters 
            WHERE farrowing_record_id = %s
        """, (farrowing_id,))
        
        litter_result = cursor.fetchone()
        if litter_result['litter_count'] == 0:
            # Litter registration is required before marking sow as available
            return jsonify({
                'success': False, 
                'message': 'Litter registration required',
                'requires_litter_registration': True,
                'farrowing_data': {
                    'farrowing_id': farrowing_id,
                    'breeding_id': farrowing['breeding_id'],
                    'sow_id': farrowing['sow_id'],
                    'sow_tag_id': farrowing['sow_tag_id'],
                    'farrowing_date': farrowing['farrowing_date'].strftime('%Y-%m-%d')
                }
            })
        
        # Update sow's breeding status to available
        cursor.execute("""
            UPDATE pigs 
            SET breeding_status = 'available' 
            WHERE id = %s
        """, (farrowing['sow_id'],))
        
        # Log the successful breeding cycle completion
        log_activity(session['employee_id'], 'BREEDING_CYCLE_COMPLETED', 
                    f'Successful breeding cycle completed for sow {farrowing["sow_tag_id"]}. Ready for next breeding.')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Sow {farrowing["sow_tag_id"]} marked as available for next breeding. Successful breeding cycle completed!'
        })
        
    except Exception as e:
        print(f"Error marking sow as available: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to mark sow as available: {str(e)}'})

@app.route('/api/farrowing/litter-registration-data/<int:farrowing_id>', methods=['GET'])
def get_farrowing_litter_registration_data(farrowing_id):
    """Get farrowing data needed for litter registration"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get comprehensive farrowing data for litter registration
        cursor.execute("""
            SELECT fr.*, 
                   br.mating_date, br.expected_due_date,
                   sow.tag_id as sow_tag_id, sow.breed as sow_breed, sow.age_days as sow_age_days,
                   sow.farm_id as sow_farm_id, f.farm_name as sow_farm_name,
                   boar.tag_id as boar_tag_id, boar.breed as boar_breed,
                   e.full_name as created_by_name
            FROM farrowing_records fr
            JOIN breeding_records br ON fr.breeding_id = br.id
            LEFT JOIN pigs sow ON br.sow_id = sow.id
            LEFT JOIN pigs boar ON br.boar_id = boar.id
            LEFT JOIN farms f ON sow.farm_id = f.id
            LEFT JOIN employees e ON fr.created_by = e.id
            WHERE fr.id = %s
        """, (farrowing_id,))
        
        farrowing_data = cursor.fetchone()
        if not farrowing_data:
            return jsonify({'success': False, 'message': 'Farrowing record not found'})
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'farrowing_data': farrowing_data
        })
        
    except Exception as e:
        print(f"Error getting farrowing litter registration data: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get farrowing data: {str(e)}'})

@app.route('/api/farrowing/<int:farrowing_id>/edit', methods=['PUT'])
def edit_farrowing_record(farrowing_id):
    """Edit a farrowing record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        farrowing_date = data.get('farrowing_date')
        alive_piglets = data.get('alive_piglets')
        dead_piglets = data.get('dead_piglets', 0)
        weak_piglets = data.get('weak_piglets', 0)
        notes = data.get('notes', '')
        
        if not farrowing_date:
            return jsonify({'success': False, 'message': 'Farrowing date is required'})
        
        if alive_piglets is None or alive_piglets < 0:
            return jsonify({'success': False, 'message': 'Valid number of alive piglets is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get original data for comparison
        cursor.execute("""
            SELECT farrowing_date, alive_piglets, dead_piglets, weak_piglets, notes
            FROM farrowing_records WHERE id = %s
        """, (farrowing_id,))
        original_data = cursor.fetchone()
        
        if not original_data:
            return jsonify({'success': False, 'message': 'Farrowing record not found'})
        
        # Track changes
        changes = []
        if str(original_data['farrowing_date']) != farrowing_date:
            changes.append(('farrowing_date', str(original_data['farrowing_date']), farrowing_date))
        if str(original_data['alive_piglets']) != str(alive_piglets):
            changes.append(('alive_piglets', str(original_data['alive_piglets']), str(alive_piglets)))
        if str(original_data['dead_piglets']) != str(dead_piglets):
            changes.append(('dead_piglets', str(original_data['dead_piglets']), str(dead_piglets)))
        if str(original_data['weak_piglets']) != str(weak_piglets):
            changes.append(('weak_piglets', str(original_data['weak_piglets']), str(weak_piglets)))
        if str(original_data['notes'] or '') != str(notes):
            changes.append(('notes', str(original_data['notes'] or ''), str(notes)))
        
        # Update farrowing record
        cursor.execute("""
            UPDATE farrowing_records 
            SET farrowing_date = %s, alive_piglets = %s, dead_piglets = %s, 
                weak_piglets = %s, notes = %s, updated_at = NOW()
            WHERE id = %s
        """, (farrowing_date, alive_piglets, dead_piglets, weak_piglets, notes, farrowing_id))
        
        # Insert audit history
        for field_name, old_value, new_value in changes:
            cursor.execute("""
                INSERT INTO farrowing_records_edit_history 
                (record_id, field_name, old_value, new_value, edited_by)
                VALUES (%s, %s, %s, %s, %s)
            """, (farrowing_id, field_name, old_value, new_value, session['employee_id']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Log activity
        log_activity(session['employee_id'], 'FARROWING_EDIT', 
                    f'Farrowing record {farrowing_id} updated')
        
        return jsonify({
            'success': True,
            'message': 'Farrowing record updated successfully'
        })
        
    except Exception as e:
        print(f"Error updating farrowing record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to update farrowing record: {str(e)}'})

@app.route('/api/farrowing/<int:farrowing_id>/delete', methods=['DELETE'])
def delete_farrowing_record(farrowing_id):
    """Delete a farrowing record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get farrowing record details for logging
        cursor.execute("""
            SELECT br.sow_id, p.tag_id as sow_tag_id
            FROM farrowing_records fr
            JOIN breeding_records br ON fr.breeding_id = br.id
            LEFT JOIN pigs p ON br.sow_id = p.id
            WHERE fr.id = %s
        """, (farrowing_id,))
        
        record = cursor.fetchone()
        if not record:
            return jsonify({'success': False, 'message': 'Farrowing record not found'})
        
        # Update sow status to available
        cursor.execute("""
            UPDATE pigs 
            SET status = 'available', breeding_status = 'available' 
            WHERE id = %s
        """, (record['sow_id'],))
        
        # Update breeding record status to completed
        cursor.execute("""
            UPDATE breeding_records 
            SET status = 'completed' 
            WHERE sow_id = %s AND status = 'pregnant'
        """, (record['sow_id'],))
        
        # Delete related litters first to avoid foreign key constraint
        cursor.execute("DELETE FROM litters WHERE farrowing_record_id = %s", (farrowing_id,))
        print(f"Deleted litters for farrowing record {farrowing_id}")
        
        # Temporarily disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        try:
            # Delete farrowing record
            cursor.execute("DELETE FROM farrowing_records WHERE id = %s", (farrowing_id,))
            
            conn.commit()
        finally:
            # Always re-enable foreign key checks, even if an error occurs
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        cursor.close()
        conn.close()
        
        # Log activity
        log_activity(session['employee_id'], 'FARROWING_DELETE', 
                    f'Farrowing record {farrowing_id} deleted for sow {record["sow_tag_id"]}')
        
        return jsonify({
            'success': True,
            'message': 'Farrowing record deleted successfully. Sow is now available for breeding.'
        })
        
    except Exception as e:
        print(f"Error deleting farrowing record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to delete farrowing record: {str(e)}'})

@app.route('/api/farrowing/<int:farrowing_id>/audit', methods=['GET'])
def get_farrowing_audit_history(farrowing_id):
    """Get audit history for a farrowing record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT h.*, e.full_name as edited_by_name
            FROM farrowing_records_edit_history h
            LEFT JOIN employees e ON h.edited_by = e.id
            WHERE h.record_id = %s
            ORDER BY h.edited_at DESC
        """, (farrowing_id,))
        
        audit_records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'audit_records': audit_records
        })
        
    except Exception as e:
        print(f"Error getting farrowing audit history: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get audit history: {str(e)}'})

@app.route('/api/family-tree/sows', methods=['GET'])
def get_family_tree_sows():
    """Get all sows for family tree display"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all sows with their breeding status and farm information
        cursor.execute("""
            SELECT p.id, p.tag_id, p.breed, p.gender, p.birth_date, p.age_days,
                   p.breeding_status, p.status, p.purpose,
                   f.farm_name, f.farm_location,
                   COALESCE(COUNT(br.id), 0) as total_breedings,
                   COALESCE(COUNT(CASE WHEN br.status = 'completed' THEN 1 END), 0) as successful_breedings,
                   COALESCE(COUNT(CASE WHEN br.status = 'failed' THEN 1 END), 0) as failed_breedings
            FROM pigs p
            LEFT JOIN farms f ON p.farm_id = f.id
            LEFT JOIN breeding_records br ON p.id = br.sow_id
            WHERE p.gender = 'female' AND p.status = 'active'
            GROUP BY p.id, p.tag_id, p.breed, p.gender, p.birth_date, p.age_days,
                     p.breeding_status, p.status, p.purpose,
                     f.farm_name, f.farm_location
            ORDER BY p.tag_id
        """)
        sows = cursor.fetchall()
        
        print(f"Found {len(sows)} sows")
        for sow in sows:
            print(f"Sow: {sow}")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'sows': sows
        })
        
    except Exception as e:
        print(f"Error getting family tree sows: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/family-tree/boars', methods=['GET'])
def get_family_tree_boars():
    """Get all boars for family tree display"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all boars with their breeding status and farm information
        cursor.execute("""
            SELECT p.id, p.tag_id, p.breed, p.gender, p.birth_date, p.age_days,
                   p.breeding_status, p.status, p.purpose,
                   f.farm_name, f.farm_location,
                   COALESCE(COUNT(br.id), 0) as total_breedings,
                   COALESCE(COUNT(CASE WHEN br.status = 'completed' THEN 1 END), 0) as successful_breedings,
                   COALESCE(COUNT(CASE WHEN br.status = 'failed' THEN 1 END), 0) as failed_breedings
            FROM pigs p
            LEFT JOIN farms f ON p.farm_id = f.id
            LEFT JOIN breeding_records br ON p.id = br.boar_id
            WHERE p.gender = 'male' AND p.status = 'active'
            GROUP BY p.id, p.tag_id, p.breed, p.gender, p.birth_date, p.age_days,
                     p.breeding_status, p.status, p.purpose,
                     f.farm_name, f.farm_location
            ORDER BY p.tag_id
        """)
        boars = cursor.fetchall()
        
        print(f"Found {len(boars)} boars")
        for boar in boars:
            print(f"Boar: {boar}")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'boars': boars
        })
        
    except Exception as e:
        print(f"Error getting family tree boars: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Cow Management API endpoints
@app.route('/api/cow/generate-ear-tag', methods=['POST'])
def generate_cow_ear_tag():
    """Generate next available cow ear tag ID starting with C"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the highest existing ear tag number
        cursor.execute("""
            SELECT ear_tag FROM cows 
            WHERE ear_tag LIKE 'C%' 
            ORDER BY CAST(SUBSTRING(ear_tag, 2) AS UNSIGNED) DESC 
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            # Extract number from existing tag and increment
            current_number = int(result['ear_tag'][1:])
            next_number = current_number + 1
        else:
            # First cow
            next_number = 1
        
        # Format with leading zeros
        next_ear_tag = f"C{next_number:03d}"
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'next_ear_tag': next_ear_tag
        })
        
    except Exception as e:
        print(f"Error generating cow ear tag: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to generate ear tag: {str(e)}'})

@app.route('/api/cow/register', methods=['POST'])
def register_cow():
    """Register a new cow"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['ear_tag', 'breed', 'gender', 'source', 'birth_date']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        # Calculate age in days if birth date is provided
        age_days = None
        if data.get('birth_date'):
            try:
                birth_dt = datetime.strptime(data['birth_date'], '%Y-%m-%d')
                age_days = (datetime.now() - birth_dt).days
            except ValueError:
                return jsonify({'success': False, 'message': 'Invalid birth date format'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if ear tag already exists
        cursor.execute("SELECT id FROM cows WHERE ear_tag = %s", (data['ear_tag'],))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Ear tag already exists'})
        
        # Insert new cow
        cursor.execute("""
            INSERT INTO cows (
                ear_tag, name, breed, color_markings, gender, birth_date, age_days,
                source, purchase_date, purchase_place, sire_ear_tag, sire_details,
                dam_ear_tag, dam_details, registered_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['ear_tag'], data.get('name'), data['breed'], data.get('color_markings'),
            data['gender'], data['birth_date'], age_days, data['source'],
            data.get('purchase_date'), data.get('purchase_place'),
            data.get('sire_ear_tag'), data.get('sire_details'),
            data.get('dam_ear_tag'), data.get('dam_details'), session['employee_id']
        ))
        
        cow_id = cursor.lastrowid
        
        # Log activity
        log_activity(session['employee_id'], 'COW_REGISTRATION', 
                   f'New cow registered with ear tag {data["ear_tag"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Cow registered successfully',
            'cow_id': cow_id
        })
        
    except Exception as e:
        print(f"Error registering cow: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to register cow: {str(e)}'})

@app.route('/api/cow/production/stats', methods=['GET'])
def get_cow_production_stats():
    """Get cow production statistics"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get today's production
        today = datetime.now().date()
        cursor.execute("""
            SELECT COALESCE(SUM(milk_quantity), 0) as today_production
            FROM cow_milk_production 
            WHERE production_date = %s
        """, (today,))
        today_result = cursor.fetchone()
        
        # Get total cows
        cursor.execute("SELECT COUNT(*) as total_cows FROM cows WHERE status = 'active'")
        cows_result = cursor.fetchone()
        
        # Get average per cow
        cursor.execute("""
            SELECT COALESCE(AVG(milk_quantity), 0) as avg_per_cow
            FROM cow_milk_production 
            WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        """)
        avg_result = cursor.fetchone()
        
        # Get this week's total
        cursor.execute("""
            SELECT COALESCE(SUM(milk_quantity), 0) as week_total
            FROM cow_milk_production 
            WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        """)
        week_result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'today_production': today_result['today_production'],
            'total_cows': cows_result['total_cows'],
            'avg_per_cow': round(avg_result['avg_per_cow'], 2),
            'week_total': week_result['week_total']
        })
        
    except Exception as e:
        print(f"Error getting production stats: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get production stats: {str(e)}'})

@app.route('/api/cow/production/records', methods=['GET'])
def get_cow_production_records():
    """Get cow production records"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get production records with cow details
        cursor.execute("""
            SELECT cmp.id, cmp.cow_id, cmp.production_date, cmp.session, 
                   cmp.quantity, cmp.notes, cmp.created_at, cmp.updated_at,
                   c.ear_tag, c.name
            FROM cow_milk_production cmp
            JOIN cows c ON cmp.cow_id = c.id
            ORDER BY cmp.production_date DESC, cmp.created_at DESC
            LIMIT 50
        """)
        records = cursor.fetchall()
        
        # Convert records to JSON-serializable format
        serializable_records = []
        for record in records:
            serializable_record = {
                'id': record[0],
                'cow_id': record[1],
                'production_date': record[2].isoformat() if record[2] else None,
                'session': record[3],
                'quantity': record[4],
                'notes': record[5],
                'created_at': record[6].isoformat() if record[6] else None,
                'updated_at': record[7].isoformat() if record[7] else None,
                'ear_tag': record[8],
                'name': record[9]
            }
            serializable_records.append(serializable_record)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': serializable_records
        })
        
    except Exception as e:
        print(f"Error getting production records: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get production records: {str(e)}'})

@app.route('/api/cow/production/record', methods=['POST'])
def record_cow_milk_production():
    """Record cow milk production"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['cow_id', 'production_date', 'milking_session', 'milk_quantity']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create cow_milk_production table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cow_milk_production (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cow_id INT NOT NULL,
                production_date DATE NOT NULL,
                milking_session ENUM('morning', 'afternoon', 'evening') NOT NULL,
                milk_quantity DECIMAL(5,2) NOT NULL,
                notes TEXT,
                recorded_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (cow_id) REFERENCES cows(id),
                FOREIGN KEY (recorded_by) REFERENCES employees(id),
                INDEX idx_cow_id (cow_id),
                INDEX idx_production_date (production_date),
                INDEX idx_milking_session (milking_session)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Insert production record
        cursor.execute("""
            INSERT INTO cow_milk_production (
                cow_id, production_date, milking_session, milk_quantity, notes, recorded_by
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data['cow_id'], data['production_date'], data['milking_session'],
            data['milk_quantity'], data.get('notes'), session['employee_id']
        ))
        
        production_id = cursor.lastrowid
        
        # Log activity
        log_activity(session['employee_id'], 'COW_MILK_PRODUCTION', 
                   f'Milk production recorded: {data["milk_quantity"]}L for cow {data["cow_id"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Milk production recorded successfully',
            'production_id': production_id
        })
        
    except Exception as e:
        print(f"Error recording milk production: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to record milk production: {str(e)}'})

# Chicken Production API Endpoints
@app.route('/api/chicken/production/stats', methods=['GET'])
def get_chicken_production_stats():
    """Get chicken production statistics"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create chicken_production table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_production (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chicken_id INT NOT NULL,
                production_date DATE NOT NULL,
                production_type ENUM('eggs', 'meat') NOT NULL,
                quantity INT NOT NULL,
                notes TEXT,
                recorded_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (chicken_id) REFERENCES chickens(id),
                FOREIGN KEY (recorded_by) REFERENCES employees(id),
                INDEX idx_production_date (production_date),
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_production_type (production_type)
            )
        """)
        
        # Get today's production
        cursor.execute("""
            SELECT COALESCE(SUM(quantity), 0) as today_production
            FROM chicken_production 
            WHERE production_date = CURDATE()
        """)
        today_production = cursor.fetchone()[0]
        
        # Get total chickens
        cursor.execute("SELECT COUNT(*) FROM chickens WHERE current_status = 'active'")
        total_chickens = cursor.fetchone()[0]
        
        # Get average per chicken (last 7 days)
        cursor.execute("""
            SELECT COALESCE(AVG(quantity), 0) as avg_per_chicken
            FROM chicken_production 
            WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        """)
        avg_per_chicken = cursor.fetchone()[0]
        
        # Get week's total
        cursor.execute("""
            SELECT COALESCE(SUM(quantity), 0) as week_total
            FROM chicken_production 
            WHERE production_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        """)
        week_total = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'today_production': today_production,
            'total_chickens': total_chickens,
            'avg_per_chicken': round(avg_per_chicken, 1),
            'week_total': week_total
        })
        
    except Exception as e:
        print(f"Error getting chicken production stats: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get production stats: {str(e)}'})

@app.route('/api/chicken/production/records', methods=['GET'])
def get_chicken_production_records():
    """Get chicken production records"""
    print("=== CHICKEN PRODUCTION RECORDS API CALLED ===")
    print("Production records API function started successfully")
    
    try:
        print("Attempting database connection for production records...")
        conn = get_db_connection()
        print("Database connection successful for production records")
        print(f"Database connection object: {conn}")
        print(f"Database connection closed: {conn.closed}")
        cursor = conn.cursor()
        print("Database cursor created for production records")
        print(f"Database cursor object: {cursor}")
        
        # Create chicken_production table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_production (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chicken_id INT NOT NULL,
                production_date DATE NOT NULL,
                production_type ENUM('eggs', 'meat') NOT NULL,
                quantity INT NOT NULL,
                notes TEXT,
                created_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (chicken_id) REFERENCES chickens(id),
                FOREIGN KEY (created_by) REFERENCES employees(id),
                INDEX idx_production_date (production_date),
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_production_type (production_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Check if updated_at column exists, if not add it
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'chicken_production' 
            AND COLUMN_NAME = 'updated_at'
        """)
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE chicken_production ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
        
        # Check if production records exist, if not create sample data
        cursor.execute("SELECT COUNT(*) FROM chicken_production")
        production_count = cursor.fetchone()[0]
        
        if production_count == 0:
            print("No production records found, creating sample production records...")
            # Get first chicken ID
            cursor.execute("SELECT id FROM chickens WHERE current_status = 'active' LIMIT 1")
            chicken_result = cursor.fetchone()
            if chicken_result:
                chicken_id = chicken_result[0]
                # Create sample production records
                sample_productions = [
                    (chicken_id, '2024-01-15', 'eggs', 25, 'Daily egg collection from Batch A', 1),
                    (chicken_id, '2024-01-16', 'eggs', 22, 'Continued egg production', 1),
                    (chicken_id, '2024-01-17', 'meat', 3, 'Meat production from slaughtered chickens', 1),
                    (chicken_id, '2024-01-18', 'eggs', 28, 'High egg production day', 1),
                    (chicken_id, '2024-01-19', 'meat', 2, 'Additional meat production', 1)
                ]
                
                for production_data in sample_productions:
                    cursor.execute("""
                        INSERT INTO chicken_production (
                            chicken_id, production_date, production_type, quantity, notes, created_by
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, production_data)
                
                conn.commit()
                print("Sample production records created")
        
        # Get production records with chicken details
        cursor.execute("""
            SELECT cp.id, cp.chicken_id, cp.production_date, cp.production_type, 
                   cp.quantity, cp.notes, cp.created_at, cp.updated_at,
                   c.chicken_id as ear_tag, c.batch_name as name
            FROM chicken_production cp
            LEFT JOIN chickens c ON cp.chicken_id = c.id
            ORDER BY cp.production_date DESC, cp.created_at DESC
            LIMIT 50
        """)
        records = cursor.fetchall()
        
        print(f"Found {len(records)} production records in database")
        
        # Convert records to JSON-serializable format
        serializable_records = []
        for record in records:
            serializable_record = {
                'id': record[0],
                'chicken_id': record[1],
                'production_date': record[2].isoformat() if record[2] else None,
                'production_type': record[3],
                'quantity': record[4],
                'notes': record[5],
                'created_at': record[6].isoformat() if record[6] else None,
                'updated_at': record[7].isoformat() if record[7] else None,
                'ear_tag': record[8],
                'name': record[9]
            }
            serializable_records.append(serializable_record)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': serializable_records,
            'count': len(serializable_records)
        })
        
    except Exception as e:
        print(f"Error getting chicken production records: {str(e)}")
        print(f"Exception type: {type(e)}")
        print(f"Exception args: {e.args}")
        import traceback
        traceback.print_exc()
        error_message = f"Database error: {str(e)}" if str(e) else "Unknown database error"
        return jsonify({'success': False, 'message': f'Failed to get production records: {error_message}', 'error': str(e)})

@app.route('/api/chicken/production/record', methods=['POST'])
def record_chicken_production():
    """Record chicken production"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['chicken_id', 'production_date', 'production_type', 'quantity']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create chicken_production table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_production (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chicken_id INT NOT NULL,
                production_date DATE NOT NULL,
                production_type ENUM('eggs', 'meat') NOT NULL,
                quantity INT NOT NULL,
                notes TEXT,
                recorded_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (chicken_id) REFERENCES chickens(id),
                FOREIGN KEY (recorded_by) REFERENCES employees(id),
                INDEX idx_production_date (production_date),
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_production_type (production_type)
            )
        """)
        
        # Check if created_by column exists, if not add it
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'chicken_production' 
            AND COLUMN_NAME = 'created_by'
        """)
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE chicken_production ADD COLUMN created_by INT")
            cursor.execute("ALTER TABLE chicken_production ADD FOREIGN KEY (created_by) REFERENCES employees(id)")
        
        # Check if updated_at column exists, if not add it
        cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'chicken_production' 
            AND COLUMN_NAME = 'updated_at'
        """)
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE chicken_production ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
        
        # Insert production record
        cursor.execute("""
            INSERT INTO chicken_production (
                chicken_id, production_date, production_type, quantity, notes, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data['chicken_id'], data['production_date'], data['production_type'],
            data['quantity'], data.get('notes'), session['employee_id']
        ))
        
        production_id = cursor.lastrowid
        
        # If meat production, insert weight details and create chicken_meat_production table
        if data['production_type'] == 'meat' and data.get('alive_weights') and data.get('dead_weights'):
            # Create chicken_meat_production table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chicken_meat_production (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    production_id INT NOT NULL,
                    chicken_number INT NOT NULL,
                    alive_weight DECIMAL(10,3) NOT NULL,
                    dead_weight DECIMAL(10,3) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (production_id) REFERENCES chicken_production(id),
                    INDEX idx_production_id (production_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            alive_weights = data.get('alive_weights', [])
            dead_weights = data.get('dead_weights', [])
            
            for i, (alive_weight, dead_weight) in enumerate(zip(alive_weights, dead_weights)):
                cursor.execute("""
                    INSERT INTO chicken_meat_production 
                    (production_id, chicken_number, alive_weight, dead_weight)
                    VALUES (%s, %s, %s, %s)
                """, (production_id, i + 1, alive_weight, dead_weight))
        
        # Log activity
        log_activity(session['employee_id'], 'CHICKEN_PRODUCTION', 
                   f'Chicken production recorded: {data["quantity"]} {data["production_type"]} for chicken {data["chicken_id"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Chicken production recorded successfully',
            'production_id': production_id
        })
        
    except Exception as e:
        print(f"Error recording chicken production: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to record chicken production: {str(e)}'})

@app.route('/api/chicken/list', methods=['GET'])
def get_chickens_list():
    """Get all chickens for display"""
    print("=== CHICKEN LIST API CALLED ===")
    print("API function started successfully")
    
    try:
        print("Attempting database connection...")
        conn = get_db_connection()
        print("Database connection successful")
        print(f"Database connection object: {conn}")
        print(f"Database connection closed: {conn.closed}")
        cursor = conn.cursor()
        print("Database cursor created")
        print(f"Database cursor object: {cursor}")
        
        # Create chickens table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chickens (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chicken_id VARCHAR(20) UNIQUE NOT NULL,
                batch_name VARCHAR(100) NOT NULL,
                chicken_type ENUM('broiler', 'kienyeji', 'layer') NOT NULL,
                breed_name VARCHAR(100) NOT NULL,
                gender ENUM('male', 'female') NOT NULL,
                hatch_date DATE NOT NULL,
                age_days INT NOT NULL,
                source VARCHAR(100) NOT NULL,
                coop_number INT NOT NULL,
                quantity INT NOT NULL DEFAULT 1,
                current_status ENUM('active', 'sold', 'dead', 'culled') DEFAULT 'active',
                registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_chicken_type (chicken_type),
                INDEX idx_batch_name (batch_name),
                INDEX idx_coop_number (coop_number),
                INDEX idx_current_status (current_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Check if chickens exist, if not create sample data
        cursor.execute("SELECT COUNT(*) FROM chickens WHERE current_status = 'active'")
        chicken_count = cursor.fetchone()[0]
        
        if chicken_count == 0:
            print("No chickens found, creating sample chickens...")
            # Create sample chickens
            sample_chickens = [
                ('CHK001', 'Batch A', 'broiler', 'Cobb 500', 'female', '2024-01-01', 30, 'Hatchery', 1, 10, 'active', 1),
                ('CHK002', 'Batch B', 'layer', 'Lohmann Brown', 'female', '2024-01-15', 15, 'Hatchery', 2, 8, 'active', 1),
                ('CHK003', 'Batch C', 'kienyeji', 'Indigenous', 'male', '2024-02-01', 45, 'Local', 3, 5, 'active', 1)
            ]
            
            for chicken_data in sample_chickens:
                cursor.execute("""
                    INSERT INTO chickens (
                        chicken_id, batch_name, chicken_type, breed_name, gender, 
                        hatch_date, age_days, source, coop_number, quantity, 
                        current_status, created_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, chicken_data)
            
            conn.commit()
            print("Sample chickens created")
        
        # Get all active chickens
        cursor.execute("""
            SELECT c.id, c.chicken_id, c.batch_name, c.chicken_type, c.breed_name, 
                   c.gender, c.hatch_date, c.age_days, c.source, c.coop_number, 
                   c.quantity, c.current_status, c.registration_date, c.created_by
            FROM chickens c 
            WHERE c.current_status = 'active'
            ORDER BY c.registration_date DESC
        """)
        chickens = cursor.fetchall()
        
        print(f"Found {len(chickens)} active chickens in database")
        
        # Convert chickens to JSON-serializable format
        serializable_chickens = []
        for chicken in chickens:
            serializable_chicken = {
                'id': chicken[0],
                'chicken_id': chicken[1],
                'batch_name': chicken[2],
                'chicken_type': chicken[3],
                'breed_name': chicken[4],
                'gender': chicken[5],
                'hatch_date': chicken[6].isoformat() if chicken[6] else None,
                'age_days': chicken[7],
                'source': chicken[8],
                'coop_number': chicken[9],
                'quantity': chicken[10],
                'current_status': chicken[11],
                'registration_date': chicken[12].isoformat() if chicken[12] else None,
                'created_by': chicken[13],
                'created_by_name': 'Employee'
            }
            serializable_chickens.append(serializable_chicken)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'chickens': serializable_chickens
        })
        
    except Exception as e:
        print(f"Error getting chickens: {str(e)}")
        print(f"Exception type: {type(e)}")
        print(f"Exception args: {e.args}")
        import traceback
        traceback.print_exc()
        error_message = f"Database error: {str(e)}" if str(e) else "Unknown database error"
        return jsonify({'success': False, 'message': f'Failed to get chickens: {error_message}', 'error': str(e)})

# Employee Chicken Registration API
@app.route('/api/chicken/register', methods=['POST'])
def register_employee_chicken():
    """Register a new chicken (for employees)"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        # Get form data (like admin version)
        chicken_id = request.form.get('chicken_id')
        batch_name = request.form.get('batch_name')
        chicken_type = request.form.get('chicken_type')
        breed_name = request.form.get('breed_name')
        gender = request.form.get('gender')
        hatch_date = request.form.get('hatch_date')
        age_days = request.form.get('age_days')
        source = request.form.get('source')
        coop_number = request.form.get('coop_number')
        quantity = request.form.get('quantity')
        current_status = request.form.get('current_status', 'active')
        
        # Validate required fields
        if not all([chicken_id, batch_name, chicken_type, breed_name, gender, hatch_date, source, coop_number, quantity]):
            return jsonify({'success': False, 'message': 'All required fields must be filled'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create chickens table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chickens (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chicken_id VARCHAR(20) UNIQUE NOT NULL,
                batch_name VARCHAR(100) NOT NULL,
                chicken_type ENUM('broiler', 'kienyeji', 'layer') NOT NULL,
                breed_name VARCHAR(100) NOT NULL,
                gender ENUM('male', 'female') NOT NULL,
                hatch_date DATE NOT NULL,
                age_days INT NOT NULL,
                source VARCHAR(100) NOT NULL,
                coop_number INT NOT NULL,
                quantity INT NOT NULL DEFAULT 1,
                current_status ENUM('active', 'sold', 'dead', 'culled') DEFAULT 'active',
                registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_chicken_type (chicken_type),
                INDEX idx_batch_name (batch_name),
                INDEX idx_coop_number (coop_number),
                INDEX idx_current_status (current_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Insert chicken data (like admin version)
        cursor.execute("""
            INSERT INTO chickens (
                chicken_id, batch_name, chicken_type, breed_name, gender, 
                hatch_date, age_days, source, coop_number, quantity, 
                current_status, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            chicken_id, batch_name, chicken_type, breed_name, gender,
            hatch_date, age_days, source, coop_number, quantity,
            current_status, session['employee_id']
        ))
        
        chicken_id = cursor.lastrowid
        
        # Log activity
        log_activity(session['employee_id'], 'CHICKEN_REGISTRATION', 
                   f'Chicken registered: {chicken_id} - {batch_name}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Chicken registered successfully',
            'chicken_id': chicken_id
        })
        
    except Exception as e:
        print(f"Error registering chicken: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to register chicken: {str(e)}'})

@app.route('/api/cow/list', methods=['GET'])
def get_cows_list():
    """Get all cows for display"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all cows with registration information
        cursor.execute("""
            SELECT c.*, e.full_name as registered_by_name 
            FROM cows c 
            LEFT JOIN employees e ON c.registered_by = e.id 
            WHERE c.status = 'active'
            ORDER BY c.created_at DESC
        """)
        cows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'cows': cows
        })
        
    except Exception as e:
        print(f"Error getting cows list: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/calves/list', methods=['GET'])
def get_calves_list():
    """Get all calves for display"""
    # Temporarily disable authentication for testing
    # if 'employee_id' not in session or session.get('employee_role') != 'administrator':
    #     return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all calves with parent information
        cursor.execute("""
            SELECT c.*, 
                   e.full_name as recorded_by_name,
                   dam.ear_tag as dam_ear_tag,
                   dam.name as dam_name,
                   sire.ear_tag as sire_ear_tag,
                   sire.name as sire_name
            FROM calves c 
            LEFT JOIN employees e ON c.recorded_by = e.id 
            LEFT JOIN cows dam ON c.dam_id = dam.id
            LEFT JOIN cows sire ON c.sire_id = sire.id
            ORDER BY c.created_at DESC
        """)
        calves_data = cursor.fetchall()
        
        print(f"Found {len(calves_data)} calves in database")
        
        # Convert to list of dictionaries
        calves = []
        for calf_data in calves_data:
            # Handle dictionary format from MySQL connector
            calf = {
                'id': calf_data['id'],
                'calf_id': calf_data['calf_id'],
                'name': calf_data['name'],
                'breed': calf_data['breed'],
                'color_markings': calf_data['color_markings'],
                'gender': calf_data['gender'],
                'birth_date': calf_data['birth_date'],
                'dam_id': calf_data['dam_id'],
                'sire_id': calf_data['sire_id'],
                'status': calf_data['status'],
                'recorded_by': calf_data['recorded_by'],
                'created_at': calf_data['created_at'],
                'recorded_by_name': calf_data.get('recorded_by_name'),
                'dam_ear_tag': calf_data.get('dam_ear_tag'),
                'dam_name': calf_data.get('dam_name'),
                'sire_ear_tag': calf_data.get('sire_ear_tag'),
                'sire_name': calf_data.get('sire_name')
            }
            
            # Calculate age for each calf
            if calf['birth_date']:
                from datetime import date
                # Handle different date formats
                if isinstance(calf['birth_date'], str):
                    birth_date = datetime.strptime(calf['birth_date'], '%Y-%m-%d').date()
                else:
                    birth_date = calf['birth_date']
                today = date.today()
                age_days = (today - birth_date).days
                calf['age_days'] = age_days
                print(f"Calf {calf['calf_id']}: birth_date={birth_date}, today={today}, age_days={age_days}")
            else:
                calf['age_days'] = 0
                
            calves.append(calf)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'calves': calves
        })
        
    except Exception as e:
        print(f"Error getting calves list: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cow/<int:cow_id>', methods=['GET'])
def get_cow_details(cow_id):
    """Get specific cow details for editing"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT c.*, e.full_name as registered_by_name 
            FROM cows c 
            LEFT JOIN employees e ON c.registered_by = e.id 
            WHERE c.id = %s AND c.status = 'active'
        """, (cow_id,))
        cow = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not cow:
            return jsonify({'error': 'Cow not found'}), 404
        
        return jsonify({
            'success': True,
            'cow': cow
        })
        
    except Exception as e:
        print(f"Error getting cow details: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cow/<int:cow_id>/edit', methods=['PUT'])
def edit_cow(cow_id):
    """Edit cow details"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['ear_tag', 'breed', 'gender', 'source', 'birth_date']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        # Calculate age in days if birth date is provided
        age_days = None
        if data.get('birth_date'):
            try:
                birth_dt = datetime.strptime(data['birth_date'], '%Y-%m-%d')
                age_days = (datetime.now() - birth_dt).days
            except ValueError:
                return jsonify({'success': False, 'message': 'Invalid birth date format'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if new ear tag already exists (excluding current cow)
        cursor.execute("SELECT id FROM cows WHERE ear_tag = %s AND id != %s", (data['ear_tag'], cow_id))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Ear tag already exists'})
        
        # Get current cow data for comparison
        cursor.execute("""
            SELECT ear_tag, name, breed, color_markings, gender, birth_date, 
                   source, purchase_date, purchase_place, sire_ear_tag, 
                   sire_details, dam_ear_tag, dam_details
            FROM cows WHERE id = %s
        """, (cow_id,))
        current_cow = cursor.fetchone()
        
        # Track changes and log to history
        changes_logged = 0
        fields_to_check = [
            ('ear_tag', 'Ear Tag'),
            ('name', 'Name'),
            ('breed', 'Breed'),
            ('color_markings', 'Color/Markings'),
            ('gender', 'Gender'),
            ('birth_date', 'Birth Date'),
            ('source', 'Source'),
            ('purchase_date', 'Purchase Date'),
            ('purchase_place', 'Purchase Place'),
            ('sire_ear_tag', 'Sire Ear Tag'),
            ('sire_details', 'Sire Details'),
            ('dam_ear_tag', 'Dam Ear Tag'),
            ('dam_details', 'Dam Details')
        ]
        
        for field, display_name in fields_to_check:
            old_value = str(current_cow[field]) if current_cow[field] is not None else ''
            new_value = str(data.get(field, '')) if data.get(field) is not None else ''
            
            if old_value != new_value:
                cursor.execute("""
                    INSERT INTO cow_edit_history (cow_id, field_name, old_value, new_value, edited_by)
                    VALUES (%s, %s, %s, %s, %s)
                """, (cow_id, display_name, old_value, new_value, session['employee_id']))
                changes_logged += 1
        
        # Update cow
        cursor.execute("""
            UPDATE cows SET 
                ear_tag = %s, name = %s, breed = %s, color_markings = %s, 
                gender = %s, birth_date = %s, age_days = %s, source = %s, 
                purchase_date = %s, purchase_place = %s, sire_ear_tag = %s, 
                sire_details = %s, dam_ear_tag = %s, dam_details = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (
            data['ear_tag'], data.get('name'), data['breed'], data.get('color_markings'),
            data['gender'], data['birth_date'], age_days, data['source'],
            data.get('purchase_date'), data.get('purchase_place'),
            data.get('sire_ear_tag'), data.get('sire_details'),
            data.get('dam_ear_tag'), data.get('dam_details'), cow_id
        ))
        
        # Log activity
        log_activity(session['employee_id'], 'COW_EDIT', 
                   f'Cow details updated for ear tag {data["ear_tag"]} - {changes_logged} changes logged')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Cow details updated successfully. {changes_logged} changes logged.',
            'changes_logged': changes_logged
        })
        
    except Exception as e:
        print(f"Error editing cow: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to update cow: {str(e)}'})

@app.route('/api/cow/generate-edit-ear-tag', methods=['POST'])
def generate_edit_ear_tag():
    """Generate ear tag for edited cow with EC prefix"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the highest existing EC ear tag number
        cursor.execute("""
            SELECT ear_tag FROM cows 
            WHERE ear_tag LIKE 'EC%' 
            ORDER BY CAST(SUBSTRING(ear_tag, 3) AS UNSIGNED) DESC 
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            # Extract number from existing EC tag and increment
            current_number = int(result['ear_tag'][2:])  # Remove EC prefix
            next_number = current_number + 1
        else:
            # First EC cow
            next_number = 1
        
        # Format with leading zeros
        new_ear_tag = f"EC{next_number:03d}"
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'new_ear_tag': new_ear_tag
        })
        
    except Exception as e:
        print(f"Error generating edit ear tag: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to generate edit ear tag: {str(e)}'})

@app.route('/api/cow/<int:cow_id>/edit-history', methods=['GET'])
def get_cow_edit_history(cow_id):
    """Get edit history for a specific cow"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get cow basic info
        cursor.execute("""
            SELECT ear_tag, name FROM cows WHERE id = %s
        """, (cow_id,))
        cow = cursor.fetchone()
        
        if not cow:
            return jsonify({'error': 'Cow not found'}), 404
        
        # Get edit history
        cursor.execute("""
            SELECT ceh.*, e.full_name as edited_by_name
            FROM cow_edit_history ceh
            LEFT JOIN employees e ON ceh.edited_by = e.id
            WHERE ceh.cow_id = %s
            ORDER BY ceh.edited_at DESC
        """, (cow_id,))
        history = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'cow': cow,
            'history': history
        })
        
    except Exception as e:
        print(f"Error getting cow edit history: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/milk-production/record', methods=['POST'])
def record_milk_production():
    """Record milk production data"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['cow_id', 'production_date', 'milking_session', 'milk_quantity']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert milk production record
        cursor.execute("""
            INSERT INTO milk_production (
                cow_id, production_date, milking_session, milk_quantity,
                fat_percentage, protein_percentage, milk_grade,
                milk_quality_assessment, additional_notes, recorded_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['cow_id'], data['production_date'], data['milking_session'],
            data['milk_quantity'], data.get('fat_percentage'), data.get('protein_percentage'),
            data.get('milk_grade'), data.get('milk_quality_assessment'),
            data.get('additional_notes'), session['employee_id']
        ))
        
        production_id = cursor.lastrowid
        
        # Log activity
        log_activity(session['employee_id'], 'MILK_PRODUCTION', 
                   f'Milk production recorded: {data["milk_quantity"]}L for cow {data["cow_id"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Milk production recorded successfully',
            'production_id': production_id
        })
        
    except Exception as e:
        print(f"Error recording milk production: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to record milk production: {str(e)}'})

@app.route('/api/milk-production/<int:production_id>', methods=['GET'])
def get_milk_production_record(production_id):
    """Get a specific milk production record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get production record
        cursor.execute("""
            SELECT * FROM milk_production WHERE id = %s
        """, (production_id,))
        record = cursor.fetchone()
        
        if not record:
            return jsonify({'success': False, 'message': 'Production record not found'}), 404
        
        cursor.close()
        conn.close()
        
        # Convert to dictionary
        record_dict = {
            'id': record['id'],
            'cow_id': record['cow_id'],
            'production_date': record['production_date'].isoformat() if record['production_date'] else None,
            'milking_session': record['milking_session'],
            'milk_quantity': float(record['milk_quantity']) if record['milk_quantity'] is not None else None,
            'fat_percentage': float(record['fat_percentage']) if record['fat_percentage'] is not None else None,
            'protein_percentage': float(record['protein_percentage']) if record['protein_percentage'] is not None else None,
            'milk_grade': record['milk_grade'],
            'milk_quality_assessment': record['milk_quality_assessment'],
            'additional_notes': record['additional_notes'],
            'recorded_by': record['recorded_by'],
            'created_at': record['created_at'].isoformat() if record['created_at'] else None
        }
        
        return jsonify({
            'success': True,
            'record': record_dict
        })
        
    except Exception as e:
        print(f"Error getting milk production record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get production record: {str(e)}'})

@app.route('/api/milk-production/<int:production_id>/edit', methods=['PUT'])
def edit_milk_production_record(production_id):
    """Edit a milk production record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['production_date', 'milking_session', 'milk_quantity']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get original data for comparison
        cursor.execute("""
            SELECT production_date, milking_session, milk_quantity, fat_percentage, 
                   protein_percentage, milk_grade, milk_quality_assessment, additional_notes
            FROM milk_production WHERE id = %s
        """, (production_id,))
        original_data = cursor.fetchone()
        
        if not original_data:
            return jsonify({'success': False, 'message': 'Production record not found'}), 404
        
        # Track changes for audit trail
        changes = []
        
        # Compare each field and track changes
        if str(original_data['production_date']) != str(data.get('production_date')):
            changes.append(('production_date', str(original_data['production_date']), str(data.get('production_date'))))
        if str(original_data['milking_session']) != str(data.get('milking_session')):
            changes.append(('milking_session', str(original_data['milking_session']), str(data.get('milking_session'))))
        if str(original_data['milk_quantity']) != str(data.get('milk_quantity')):
            changes.append(('milk_quantity', str(original_data['milk_quantity']), str(data.get('milk_quantity'))))
        if str(original_data['fat_percentage'] or '') != str(data.get('fat_percentage') or ''):
            changes.append(('fat_percentage', str(original_data['fat_percentage'] or ''), str(data.get('fat_percentage') or '')))
        if str(original_data['protein_percentage'] or '') != str(data.get('protein_percentage') or ''):
            changes.append(('protein_percentage', str(original_data['protein_percentage'] or ''), str(data.get('protein_percentage') or '')))
        if str(original_data['milk_grade'] or '') != str(data.get('milk_grade') or ''):
            changes.append(('milk_grade', str(original_data['milk_grade'] or ''), str(data.get('milk_grade') or '')))
        if str(original_data['milk_quality_assessment'] or '') != str(data.get('milk_quality_assessment') or ''):
            changes.append(('milk_quality_assessment', str(original_data['milk_quality_assessment'] or ''), str(data.get('milk_quality_assessment') or '')))
        if str(original_data['additional_notes'] or '') != str(data.get('additional_notes') or ''):
            changes.append(('additional_notes', str(original_data['additional_notes'] or ''), str(data.get('additional_notes') or '')))
        
        # Update milk production record
        cursor.execute("""
            UPDATE milk_production SET 
                production_date = %s, milking_session = %s, milk_quantity = %s,
                fat_percentage = %s, protein_percentage = %s, milk_grade = %s,
                milk_quality_assessment = %s, additional_notes = %s
            WHERE id = %s
        """, (
            data['production_date'], data['milking_session'], data['milk_quantity'],
            data.get('fat_percentage'), data.get('protein_percentage'),
            data.get('milk_grade'), data.get('milk_quality_assessment'),
            data.get('additional_notes'), production_id
        ))
        
        # Insert audit records for each changed field
        for field_name, old_value, new_value in changes:
            cursor.execute("""
                INSERT INTO milk_production_edit_history 
                (production_id, field_name, old_value, new_value, edited_by)
                VALUES (%s, %s, %s, %s, %s)
            """, (production_id, field_name, old_value, new_value, session['employee_id']))
        
        # Log activity
        log_activity(session['employee_id'], 'MILK_PRODUCTION_EDIT', 
                   f'Milk production record {production_id} updated: {data["milk_quantity"]}L')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Milk production record updated successfully'
        })
        
    except Exception as e:
        print(f"Error updating milk production record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to update production record: {str(e)}'})

@app.route('/api/milk-production/<int:production_id>/delete', methods=['DELETE'])
def delete_milk_production_record(production_id):
    """Delete a milk production record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if record exists and get details for logging
        cursor.execute("""
            SELECT cow_id, milk_quantity, production_date 
            FROM milk_production WHERE id = %s
        """, (production_id,))
        record = cursor.fetchone()
        
        if not record:
            return jsonify({'success': False, 'message': 'Production record not found'}), 404
        
        # Check for any potential foreign key constraint issues
        # This is just for debugging - we'll try the delete anyway
        try:
            cursor.execute("""
                SELECT COUNT(*) as count FROM milk_production 
                WHERE cow_id = %s AND id != %s
            """, (record[0], production_id))
            related_records = cursor.fetchone()
            print(f"Found {related_records['count']} other production records for cow {record[0]}")
        except Exception as debug_e:
            print(f"Debug query failed: {debug_e}")
        
        # Delete the record
        print(f"Attempting to delete milk production record {production_id}")
        cursor.execute("DELETE FROM milk_production WHERE id = %s", (production_id,))
        
        # Check if any rows were affected
        rows_affected = cursor.rowcount
        print(f"Delete query affected {rows_affected} rows")
        
        if rows_affected == 0:
            return jsonify({'success': False, 'message': 'No record was deleted. Record may not exist.'})
        
        # Log activity (with error handling)
        try:
            log_activity(session['employee_id'], 'MILK_PRODUCTION_DELETE', 
                       f'Milk production record {production_id} deleted: {record[1]}L from cow {record[0]} on {record[2]}')
        except Exception as log_error:
            print(f"Warning: Failed to log activity: {log_error}")
            # Continue with the operation even if logging fails
        
        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Milk production record deleted successfully'
        })
        
    except Exception as e:
        if conn:
            conn.rollback()
        error_message = str(e)
        print(f"Error deleting milk production record: {error_message}")
        
        # Provide more specific error messages based on the error type
        if "foreign key constraint" in error_message.lower():
            return jsonify({'success': False, 'message': 'Cannot delete this record because it is referenced by other records. Please remove related records first.'})
        elif "cannot delete" in error_message.lower():
            return jsonify({'success': False, 'message': 'Cannot delete this record due to database constraints.'})
        elif "constraint" in error_message.lower():
            return jsonify({'success': False, 'message': 'Database constraint violation. The record cannot be deleted.'})
        else:
            return jsonify({'success': False, 'message': f'Failed to delete production record: {error_message}'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/milk-production/<int:production_id>/audit', methods=['GET'])
def get_milk_production_audit(production_id):
    """Get audit history for a milk production record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get audit history
        cursor.execute("""
            SELECT 
                mpeh.field_name,
                mpeh.old_value,
                mpeh.new_value,
                mpeh.edited_at,
                COALESCE(e.full_name, 'Unknown User') as edited_by_name
            FROM milk_production_edit_history mpeh
            LEFT JOIN employees e ON mpeh.edited_by = e.id
            WHERE mpeh.production_id = %s
            ORDER BY mpeh.edited_at DESC
        """, (production_id,))
        
        audit_records = cursor.fetchall()
        
        # Convert datetime to string for JSON serialization
        for record in audit_records:
            if record['edited_at']:
                record['edited_at'] = record['edited_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'success': True,
            'audit_records': audit_records
        })
        
    except Exception as e:
        print(f"Error fetching milk production audit history: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to fetch audit history: {str(e)}'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/milk-sales-usage/record', methods=['POST'])
def record_milk_sales_usage():
    """Record milk sales or usage data"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['transaction_type', 'transaction_date']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert sales/usage record
        cursor.execute("""
            INSERT INTO milk_sales_usage (
                transaction_type, transaction_date, buyer, quantity_sold,
                price_per_liter, total_amount, quantity_used, purpose_of_use, recorded_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['transaction_type'], data['transaction_date'], data.get('buyer'),
            data.get('quantity_sold'), data.get('price_per_liter'), data.get('total_amount'),
            data.get('quantity_used'), data.get('purpose_of_use'), session['employee_id']
        ))
        
        transaction_id = cursor.lastrowid
        
        # Log activity
        log_activity(session['employee_id'], 'MILK_TRANSACTION', 
                   f'{data["transaction_type"].title()} recorded: {data.get("quantity_sold", data.get("quantity_used", 0))}L')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'{data["transaction_type"].title()} recorded successfully',
            'transaction_id': transaction_id
        })
        
    except Exception as e:
        print(f"Error recording milk transaction: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to record transaction: {str(e)}'})

@app.route('/api/milk-analytics/production', methods=['GET'])
def get_milk_production_analytics():
    """Get milk production analytics data"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get current month production data
        cursor.execute("""
            SELECT 
                SUM(milk_quantity) as total_production,
                AVG(milk_quantity) as daily_average,
                MAX(milk_quantity) as peak_production,
                COUNT(*) as production_days
            FROM milk_production 
            WHERE MONTH(production_date) = MONTH(CURRENT_DATE()) 
            AND YEAR(production_date) = YEAR(CURRENT_DATE())
        """)
        current_month = cursor.fetchone()
        
        # Get last month production for growth calculation
        cursor.execute("""
            SELECT SUM(milk_quantity) as last_month_production
            FROM milk_production 
            WHERE MONTH(production_date) = MONTH(CURRENT_DATE() - INTERVAL 1 MONTH) 
            AND YEAR(production_date) = YEAR(CURRENT_DATE() - INTERVAL 1 MONTH)
        """)
        last_month = cursor.fetchone()
        
        # Get 30-day trend data
        cursor.execute("""
            SELECT 
                production_date,
                SUM(milk_quantity) as daily_production
            FROM milk_production 
            WHERE production_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY production_date
            ORDER BY production_date
        """)
        trend_data = cursor.fetchall()
        
        # Calculate growth rate
        current_total = float(current_month['total_production'] or 0)
        last_total = float(last_month['last_month_production'] or 0)
        growth_rate = ((current_total - last_total) / last_total * 100) if last_total > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'total_production': current_total,
                'daily_average': float(current_month['daily_average'] or 0),
                'peak_production': float(current_month['peak_production'] or 0),
                'growth_rate': round(growth_rate, 1),
                'trend_data': [{'date': str(record['production_date']), 'production': float(record['daily_production'])} for record in trend_data]
            }
        })
        
    except Exception as e:
        print(f"Error getting milk production analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/milk-analytics/usage', methods=['GET'])
def get_milk_usage_analytics():
    """Get milk usage analytics data"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get usage data by purpose for current month
        cursor.execute("""
            SELECT 
                purpose_of_use,
                SUM(quantity_used) as total_usage
            FROM milk_sales_usage 
            WHERE transaction_type = 'usage' 
            AND MONTH(transaction_date) = MONTH(CURRENT_DATE()) 
            AND YEAR(transaction_date) = YEAR(CURRENT_DATE())
            GROUP BY purpose_of_use
        """)
        usage_data = cursor.fetchall()
        
        # Format usage data
        usage_stats = {
            'calf_feeding': 0,
            'home_consumption': 0,
            'processing': 0,
            'wastage_spoiled': 0
        }
        
        for record in usage_data:
            if record['purpose_of_use']:
                usage_stats[record['purpose_of_use']] = float(record['total_usage'] or 0)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': usage_stats
        })
        
    except Exception as e:
        print(f"Error getting milk usage analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/milk-analytics/quality', methods=['GET'])
def get_milk_quality_analytics():
    """Get milk quality analytics data"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get quality distribution for current month
        cursor.execute("""
            SELECT 
                milk_quality_assessment,
                COUNT(*) as count,
                AVG(fat_percentage) as avg_fat,
                AVG(protein_percentage) as avg_protein
            FROM milk_production 
            WHERE MONTH(production_date) = MONTH(CURRENT_DATE()) 
            AND YEAR(production_date) = YEAR(CURRENT_DATE())
            GROUP BY milk_quality_assessment
        """)
        quality_data = cursor.fetchall()
        
        # Calculate quality percentages
        total_records = sum(record['count'] for record in quality_data)
        quality_stats = {
            'good_quality': 0,
            'moderate_quality': 0,
            'poor_quality': 0,
            'avg_fat_content': 0,
            'avg_protein_content': 0
        }
        
        for record in quality_data:
            percentage = (record['count'] / total_records * 100) if total_records > 0 else 0
            quality_stats[record['milk_quality_assessment']] = round(percentage, 1)
        
        # Calculate average composition
        cursor.execute("""
            SELECT 
                AVG(fat_percentage) as avg_fat,
                AVG(protein_percentage) as avg_protein
            FROM milk_production 
            WHERE MONTH(production_date) = MONTH(CURRENT_DATE()) 
            AND YEAR(production_date) = YEAR(CURRENT_DATE())
            AND fat_percentage IS NOT NULL 
            AND protein_percentage IS NOT NULL
        """)
        composition = cursor.fetchone()
        
        quality_stats['avg_fat_content'] = round(float(composition['avg_fat'] or 0), 1)
        quality_stats['avg_protein_content'] = round(float(composition['avg_protein'] or 0), 1)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': quality_stats
        })
        
    except Exception as e:
        print(f"Error getting milk quality analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/milk-analytics/sales', methods=['GET'])
def get_milk_sales_analytics():
    """Get milk sales analytics data"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get sales data for current month
        cursor.execute("""
            SELECT 
                SUM(quantity_sold) as total_sold,
                SUM(total_amount) as total_revenue,
                AVG(price_per_liter) as avg_price,
                COUNT(*) as sales_count
            FROM milk_sales_usage 
            WHERE transaction_type = 'sale' 
            AND MONTH(transaction_date) = MONTH(CURRENT_DATE()) 
            AND YEAR(transaction_date) = YEAR(CURRENT_DATE())
        """)
        sales_data = cursor.fetchone()
        
        # Get sales by buyer
        cursor.execute("""
            SELECT 
                buyer,
                SUM(quantity_sold) as quantity,
                SUM(total_amount) as revenue
            FROM milk_sales_usage 
            WHERE transaction_type = 'sale' 
            AND MONTH(transaction_date) = MONTH(CURRENT_DATE()) 
            AND YEAR(transaction_date) = YEAR(CURRENT_DATE())
            GROUP BY buyer
            ORDER BY revenue DESC
        """)
        buyers_data = cursor.fetchall()
        
        # Get daily sales trend
        cursor.execute("""
            SELECT 
                transaction_date,
                SUM(quantity_sold) as daily_quantity,
                SUM(total_amount) as daily_revenue
            FROM milk_sales_usage 
            WHERE transaction_type = 'sale' 
            AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY transaction_date
            ORDER BY transaction_date
        """)
        trend_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'total_sold': float(sales_data['total_sold'] or 0),
                'total_revenue': float(sales_data['total_revenue'] or 0),
                'avg_price': float(sales_data['avg_price'] or 0),
                'sales_count': sales_data['sales_count'] or 0,
                'buyers': [{'buyer': record['buyer'], 'quantity': float(record['quantity']), 'revenue': float(record['revenue'])} for record in buyers_data],
                'trend_data': [{'date': str(record['transaction_date']), 'quantity': float(record['daily_quantity']), 'revenue': float(record['daily_revenue'])} for record in trend_data]
            }
        })
        
    except Exception as e:
        print(f"Error getting milk sales analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/milk-analytics/animals', methods=['GET'])
def get_animal_production_analytics():
    """Get individual animal production analytics"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get production by individual animals for current month
        cursor.execute("""
            SELECT 
                c.id,
                c.ear_tag,
                c.name,
                SUM(mp.milk_quantity) as total_production,
                AVG(mp.milk_quantity) as avg_daily_production,
                COUNT(mp.id) as production_days,
                AVG(mp.fat_percentage) as avg_fat,
                AVG(mp.protein_percentage) as avg_protein
            FROM cows c
            LEFT JOIN milk_production mp ON c.id = mp.cow_id 
            AND MONTH(mp.production_date) = MONTH(CURRENT_DATE()) 
            AND YEAR(mp.production_date) = YEAR(CURRENT_DATE())
            WHERE c.status = 'active'
            GROUP BY c.id, c.ear_tag, c.name
            HAVING total_production > 0
            ORDER BY total_production DESC
        """)
        animals_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': [{
                'id': record['id'],
                'ear_tag': record['ear_tag'],
                'name': record['name'] or 'Unnamed',
                'total_production': float(record['total_production'] or 0),
                'avg_daily_production': float(record['avg_daily_production'] or 0),
                'production_days': record['production_days'] or 0,
                'avg_fat': float(record['avg_fat'] or 0),
                'avg_protein': float(record['avg_protein'] or 0)
            } for record in animals_data]
        })
        
    except Exception as e:
        print(f"Error getting animal production analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cow/<int:cow_id>/details', methods=['GET'])
def get_cow_detailed_info(cow_id):
    """Get detailed information for a specific cow"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get cow basic information
        cursor.execute("""
            SELECT * FROM cows WHERE id = %s
        """, (cow_id,))
        cow = cursor.fetchone()
        
        if not cow:
            return jsonify({'error': 'Cow not found'}), 404
        
        # Get milk production history for this cow
        cursor.execute("""
            SELECT 
                mp.id,
                mp.production_date,
                mp.milking_session,
                mp.milk_quantity,
                mp.fat_percentage,
                mp.protein_percentage,
                mp.milk_quality_assessment,
                mp.additional_notes,
                mp.created_at,
                CASE 
                    WHEN EXISTS(SELECT 1 FROM milk_production_edit_history mpeh WHERE mpeh.production_id = mp.id) 
                    THEN 1 
                    ELSE 0 
                END as is_edited
            FROM milk_production mp
            WHERE mp.cow_id = %s 
            ORDER BY mp.production_date DESC, mp.created_at DESC
            LIMIT 30
        """, (cow_id,))
        production_history = cursor.fetchall()
        
        # Get production statistics for this cow
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                SUM(milk_quantity) as total_production,
                AVG(milk_quantity) as avg_production,
                MAX(milk_quantity) as peak_production,
                AVG(fat_percentage) as avg_fat,
                AVG(protein_percentage) as avg_protein,
                COUNT(DISTINCT production_date) as production_days
            FROM milk_production 
            WHERE cow_id = %s
        """, (cow_id,))
        stats = cursor.fetchone()
        
        # Get recent production trend (last 7 days)
        cursor.execute("""
            SELECT 
                production_date,
                SUM(milk_quantity) as daily_total
            FROM milk_production 
            WHERE cow_id = %s 
            AND production_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            GROUP BY production_date
            ORDER BY production_date
        """, (cow_id,))
        trend_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'cow': {
                'id': cow['id'],
                'ear_tag': cow['ear_tag'],
                'name': cow['name'],
                'breed': cow['breed'],
                'color_markings': cow['color_markings'],
                'gender': cow['gender'],
                'birth_date': str(cow['birth_date']) if cow['birth_date'] else None,
                'age_days': cow['age_days'],
                'source': cow['source'],
                'purchase_date': str(cow['purchase_date']) if cow['purchase_date'] else None,
                'purchase_place': cow['purchase_place'],
                'sire_ear_tag': cow['sire_ear_tag'],
                'sire_details': cow['sire_details'],
                'dam_ear_tag': cow['dam_ear_tag'],
                'dam_details': cow['dam_details'],
                'status': cow['status'],
                'created_at': str(cow['created_at'])
            },
            'production_history': [{
                'id': record['id'],
                'production_date': str(record['production_date']),
                'milking_session': record['milking_session'],
                'milk_quantity': float(record['milk_quantity']),
                'fat_percentage': float(record['fat_percentage']) if record['fat_percentage'] else None,
                'protein_percentage': float(record['protein_percentage']) if record['protein_percentage'] else None,
                'milk_quality_assessment': record['milk_quality_assessment'],
                'additional_notes': record['additional_notes'],
                'created_at': str(record['created_at']),
                'is_edited': bool(record['is_edited'])
            } for record in production_history],
            'statistics': {
                'total_records': stats['total_records'] or 0,
                'total_production': float(stats['total_production'] or 0),
                'avg_production': float(stats['avg_production'] or 0),
                'peak_production': float(stats['peak_production'] or 0),
                'avg_fat': float(stats['avg_fat'] or 0),
                'avg_protein': float(stats['avg_protein'] or 0),
                'production_days': stats['production_days'] or 0
            },
            'trend_data': [{
                'date': str(record['production_date']),
                'daily_total': float(record['daily_total'])
            } for record in trend_data]
        })
        
    except Exception as e:
        print(f"Error getting cow details: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cow-breeding/available-dams', methods=['GET'])
def get_available_dams():
    """Get all available female cows for breeding"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get available female cows (not currently pregnant)
        cursor.execute("""
            SELECT c.id, c.ear_tag, c.name, c.breed, c.age_days, c.status
            FROM cows c
            LEFT JOIN cow_breeding cb ON c.id = cb.dam_id 
            AND cb.pregnancy_status IN ('served', 'conceived')
            AND cb.conception_cancelled = FALSE
            WHERE c.gender = 'female' 
            AND c.status = 'active'
            AND cb.id IS NULL
            ORDER BY c.ear_tag
        """)
        dams = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'dams': [{
                'id': record['id'],
                'ear_tag': record['ear_tag'],
                'name': record['name'] or 'Unnamed',
                'breed': record['breed'],
                'age_days': record['age_days'],
                'age_months': record['age_days'] // 30 if record['age_days'] else 0
            } for record in dams]
        })
        
    except Exception as e:
        print(f"Error getting available dams: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cow-breeding/available-sires', methods=['GET'])
def get_available_sires():
    """Get all available male cows for breeding"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get available male cows
        cursor.execute("""
            SELECT c.id, c.ear_tag, c.name, c.breed, c.age_days, c.status
            FROM cows c
            WHERE c.gender = 'male' 
            AND c.status = 'active'
            ORDER BY c.ear_tag
        """)
        sires = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'sires': [{
                'id': record['id'],
                'ear_tag': record['ear_tag'],
                'name': record['name'] or 'Unnamed',
                'breed': record['breed'],
                'age_days': record['age_days'],
                'age_months': record['age_days'] // 30 if record['age_days'] else 0
            } for record in sires]
        })
        
    except Exception as e:
        print(f"Error getting available sires: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cow-breeding/register', methods=['POST'])
def register_cow_breeding():
    """Register a new breeding record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['dam_id', 'sire_id', 'breeding_date']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        # Calculate expected calving date (279 days after breeding)
        from datetime import datetime, timedelta
        breeding_date = datetime.strptime(data['breeding_date'], '%Y-%m-%d').date()
        expected_calving_date = breeding_date + timedelta(days=279)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if dam is already pregnant
        cursor.execute("""
            SELECT id FROM cow_breeding 
            WHERE dam_id = %s 
            AND pregnancy_status IN ('served', 'conceived')
            AND conception_cancelled = FALSE
        """, (data['dam_id'],))
        
        if cursor.fetchone():
            return jsonify({'success': False, 'message': 'This cow is already pregnant'})
        
        # Register breeding
        cursor.execute("""
            INSERT INTO cow_breeding (dam_id, sire_id, breeding_date, expected_calving_date, recorded_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (data['dam_id'], data['sire_id'], data['breeding_date'], expected_calving_date, session['employee_id']))
        
        breeding_id = cursor.lastrowid
        
        # Log activity
        log_activity(session['employee_id'], 'COW_BREEDING', 
                   f'Breeding registered: Dam {data["dam_id"]} x Sire {data["sire_id"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Breeding registered successfully',
            'breeding_id': breeding_id,
            'expected_calving_date': str(expected_calving_date)
        })
        
    except Exception as e:
        print(f"Error registering breeding: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to register breeding: {str(e)}'})

@app.route('/api/cow-breeding/list', methods=['GET'])
def get_breeding_list():
    """Get all breeding records"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding records with cow details - show all dams except those with 'available' status
        cursor.execute("""
            SELECT 
                cb.id,
                cb.breeding_date,
                cb.expected_calving_date,
                cb.birth_date,
                cb.pregnancy_status,
                cb.conception_cancelled,
                cb.cancellation_reason,
                cb.cancellation_date,
                cb.created_at,
                dam.ear_tag as dam_ear_tag,
                dam.name as dam_name,
                dam.breed as dam_breed,
                sire.ear_tag as sire_ear_tag,
                sire.name as sire_name,
                sire.breed as sire_breed,
                e.full_name as recorded_by_name
            FROM cow_breeding cb
            JOIN cows dam ON cb.dam_id = dam.id
            JOIN cows sire ON cb.sire_id = sire.id
            LEFT JOIN employees e ON cb.recorded_by = e.id
            WHERE cb.pregnancy_status != 'available'
            ORDER BY cb.breeding_date DESC
        """)
        breeding_records = cursor.fetchall()
        
        # Calculate lactation days for lactating cows
        from datetime import date
        today = date.today()
        
        processed_records = []
        for record in breeding_records:
            lactation_days = 0
            if record['pregnancy_status'] == 'lactating' and record['birth_date']:
                # Calculate lactation days from birth date
                birth_date = record['birth_date']
                lactation_days = (today - birth_date).days
            
            processed_record = {
                'id': record['id'],
                'breeding_date': str(record['breeding_date']),
                'expected_calving_date': str(record['expected_calving_date']),
                'birth_date': str(record['birth_date']) if record['birth_date'] else None,
                'pregnancy_status': record['pregnancy_status'],
                'conception_cancelled': record['conception_cancelled'],
                'cancellation_reason': record['cancellation_reason'],
                'cancellation_date': str(record['cancellation_date']) if record['cancellation_date'] else None,
                'created_at': str(record['created_at']),
                'lactation_days': lactation_days,
                'dam': {
                    'ear_tag': record['dam_ear_tag'],
                    'name': record['dam_name'],
                    'breed': record['dam_breed']
                },
                'sire': {
                    'ear_tag': record['sire_ear_tag'],
                    'name': record['sire_name'],
                    'breed': record['sire_breed']
                },
                'recorded_by': record['recorded_by_name']
            }
            processed_records.append(processed_record)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'breeding_records': processed_records
        })
        
    except Exception as e:
        print(f"Error getting breeding list: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cow-breeding/cancel-conception', methods=['POST'])
def cancel_conception():
    """Cancel conception for a breeding record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get('breeding_id') or not data.get('cancellation_reason'):
            return jsonify({'success': False, 'message': 'Breeding ID and cancellation reason are required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if breeding record exists and is within 30 days
        cursor.execute("""
            SELECT breeding_date, pregnancy_status, conception_cancelled
            FROM cow_breeding 
            WHERE id = %s
        """, (data['breeding_id'],))
        
        breeding = cursor.fetchone()
        if not breeding:
            return jsonify({'success': False, 'message': 'Breeding record not found'})
        
        if breeding['conception_cancelled']:
            return jsonify({'success': False, 'message': 'Conception already cancelled'})
        
        # Check if within 30 days
        from datetime import datetime, timedelta
        breeding_date = breeding['breeding_date']
        days_since_breeding = (datetime.now().date() - breeding_date).days
        
        if days_since_breeding > 30:
            return jsonify({'success': False, 'message': 'Cannot cancel conception after 30 days'})
        
        # Cancel conception
        cursor.execute("""
            UPDATE cow_breeding 
            SET conception_cancelled = TRUE, 
                cancellation_reason = %s, 
                cancellation_date = CURDATE(),
                pregnancy_status = 'available'
            WHERE id = %s
        """, (data['cancellation_reason'], data['breeding_id']))
        
        # Log activity
        log_activity(session['employee_id'], 'COW_BREEDING_CANCEL', 
                   f'Conception cancelled for breeding record {data["breeding_id"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Conception cancelled successfully'
        })
        
    except Exception as e:
        print(f"Error cancelling conception: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to cancel conception: {str(e)}'})

@app.route('/api/cow-breeding/end-lactation', methods=['POST'])
def end_lactation():
    """End lactation and change pregnancy status to available"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        breeding_id = data.get('breeding_id')
        
        if not breeding_id:
            return jsonify({'success': False, 'message': 'Breeding ID is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if breeding record exists and is lactating
        cursor.execute("""
            SELECT pregnancy_status, dam_id FROM cow_breeding 
            WHERE id = %s
        """, (breeding_id,))
        
        breeding = cursor.fetchone()
        if not breeding:
            return jsonify({'success': False, 'message': 'Breeding record not found'})
        
        if breeding['pregnancy_status'] != 'lactating':
            return jsonify({'success': False, 'message': 'Can only end lactation for lactating cows'})
        
        # Update breeding record to available
        cursor.execute("""
            UPDATE cow_breeding 
            SET pregnancy_status = 'available'
            WHERE id = %s
        """, (breeding_id,))
        
        # Log activity
        log_activity(session['employee_id'], 'LACTATION_ENDED', 
                   f'Lactation ended for breeding ID {breeding_id}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Lactation ended successfully'})
        
    except Exception as e:
        print(f"Error ending lactation: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to end lactation: {str(e)}'})

@app.route('/api/cow-breeding/calve', methods=['POST'])
def register_calving():
    """Register calving and create calf record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['breeding_id', 'birth_date', 'calf_gender', 'calf_breed']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get breeding record details
        cursor.execute("""
            SELECT cb.*, dam.ear_tag as dam_ear_tag, sire.ear_tag as sire_ear_tag
            FROM cow_breeding cb
            JOIN cows dam ON cb.dam_id = dam.id
            JOIN cows sire ON cb.sire_id = sire.id
            WHERE cb.id = %s
        """, (data['breeding_id'],))
        
        breeding = cursor.fetchone()
        if not breeding:
            return jsonify({'success': False, 'message': 'Breeding record not found'})
        
        # Check if already calved
        if breeding['birth_date']:
            return jsonify({'success': False, 'message': 'This cow has already calved'})
        
        # Generate calf ID based on dam's ear tag
        dam_ear_tag = breeding['dam_ear_tag']
        calf_id = f"CALF-{dam_ear_tag}-{datetime.now().strftime('%Y%m%d')}"
        
        # Calculate lactation dates
        from datetime import timedelta
        calving_date = datetime.strptime(data['birth_date'], '%Y-%m-%d').date()
        lactation_start_date = calving_date
        lactation_end_date = calving_date + timedelta(days=305)
        
        # Start transaction
        cursor.execute("START TRANSACTION")
        
        try:
            # Create calf record
            cursor.execute("""
                INSERT INTO calves (calf_id, name, breed, color_markings, gender, birth_date, dam_id, sire_id, recorded_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                calf_id,
                data.get('calf_name'),
                data['calf_breed'],
                data.get('calf_color_markings'),
                data['calf_gender'],
                data['birth_date'],
                breeding['dam_id'],
                breeding['sire_id'],
                session['employee_id']
            ))
            
            calf_record_id = cursor.lastrowid
            
            # Update breeding record with calving information
            cursor.execute("""
                UPDATE cow_breeding 
                SET birth_date = %s, 
                    lactation_start_date = %s, 
                    lactation_end_date = %s,
                    pregnancy_status = 'lactating'
                WHERE id = %s
            """, (data['birth_date'], lactation_start_date, lactation_end_date, data['breeding_id']))
            
            # Log activity
            log_activity(session['employee_id'], 'COW_CALVING', 
                       f'Calving registered: {calf_id} born to {breeding["dam_ear_tag"]}')
            
            cursor.execute("COMMIT")
            
            return jsonify({
                'success': True,
                'message': 'Calving registered successfully',
                'calf_id': calf_id,
                'lactation_end_date': str(lactation_end_date)
            })
            
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        
        finally:
            cursor.close()
            conn.close()
        
    except Exception as e:
        print(f"Error registering calving: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to register calving: {str(e)}'})

@app.route('/api/cow-breeding/ready-to-calve', methods=['GET'])
def get_ready_to_calve():
    """Get cows ready to calve (within 7 days)"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get cows ready to calve (within 7 days, not already calved)
        cursor.execute("""
            SELECT 
                cb.id as breeding_id,
                cb.breeding_date,
                cb.expected_calving_date,
                cb.pregnancy_status,
                dam.id as dam_id,
                dam.ear_tag as dam_ear_tag,
                dam.name as dam_name,
                dam.breed as dam_breed,
                sire.ear_tag as sire_ear_tag,
                sire.name as sire_name,
                sire.breed as sire_breed
            FROM cow_breeding cb
            JOIN cows dam ON cb.dam_id = dam.id
            JOIN cows sire ON cb.sire_id = sire.id
            WHERE cb.pregnancy_status IN ('served', 'conceived')
            AND cb.conception_cancelled = FALSE
            AND cb.birth_date IS NULL
            AND cb.expected_calving_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
            AND cb.expected_calving_date >= CURRENT_DATE()
            ORDER BY cb.expected_calving_date ASC
        """)
        
        ready_cows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'ready_cows': [{
                'breeding_id': record['breeding_id'],
                'breeding_date': str(record['breeding_date']),
                'expected_calving_date': str(record['expected_calving_date']),
                'pregnancy_status': record['pregnancy_status'],
                'dam': {
                    'id': record['dam_id'],
                    'ear_tag': record['dam_ear_tag'],
                    'name': record['dam_name'],
                    'breed': record['dam_breed']
                },
                'sire': {
                    'ear_tag': record['sire_ear_tag'],
                    'name': record['sire_name'],
                    'breed': record['sire_breed']
                }
            } for record in ready_cows]
        })
        
    except Exception as e:
        print(f"Error getting ready to calve cows: {str(e)}")
        return jsonify({'error': str(e)}), 500

def update_pregnancy_status():
    """Background job to update pregnancy status from 'served' to 'conceived' after 30 days"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Update pregnancy status from 'served' to 'conceived' for records older than 30 days
        cursor.execute("""
            UPDATE cow_breeding 
            SET pregnancy_status = 'conceived'
            WHERE pregnancy_status = 'served' 
            AND breeding_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            AND conception_cancelled = FALSE
        """)
        
        updated_count = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if updated_count > 0:
            print(f"Updated {updated_count} pregnancy statuses from 'served' to 'conceived'")
        
    except Exception as e:
        print(f"Error updating pregnancy status: {str(e)}")

def update_lactation_status():
    """Background job to update lactating cows back to available after 305 days"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Update pregnancy status from 'lactating' to 'available' for records past lactation period
        cursor.execute("""
            UPDATE cow_breeding 
            SET pregnancy_status = 'available'
            WHERE pregnancy_status = 'lactating' 
            AND lactation_end_date <= CURRENT_DATE()
        """)
        
        updated_count = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if updated_count > 0:
            print(f"Updated {updated_count} cows from 'lactating' to 'available'")
        
    except Exception as e:
        print(f"Error updating lactation status: {str(e)}")

@app.route('/api/farrowing/active-litters', methods=['GET'])
def get_farrowing_active_litters():
    """Get all active litters (alias for /api/litter/active)"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get unweaned litters with sow information
        cursor.execute("""
            SELECT l.*, 
                   p.tag_id as sow_tag_id, p.breed as sow_breed,
                   f.farm_name as farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE l.status = 'unweaned'
            ORDER BY l.farrowing_date DESC
        """)
        
        litters = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'litters': litters
        })
        
    except Exception as e:
        print(f"Error getting active litters: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get active litters: {str(e)}'})

@app.route('/api/farrowing/weaned-litters', methods=['GET'])
def get_farrowing_weaned_litters():
    """Get all weaned litters (alias for /api/litter/weaned)"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get weaned litters with sow information
        cursor.execute("""
            SELECT l.*, 
                   p.tag_id as sow_tag_id, p.breed as sow_breed,
                   f.farm_name as farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE l.status = 'weaned'
            ORDER BY l.weaning_date DESC
        """)
        
        litters = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'litters': litters
        })
        
    except Exception as e:
        print(f"Error getting weaned litters: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get weaned litters: {str(e)}'})

@app.route('/api/litter/active', methods=['GET'])
def get_active_litters():
    """Get all active litters"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get unweaned litters with sow information
        cursor.execute("""
            SELECT l.*, 
                   p.tag_id as sow_tag_id, p.breed as sow_breed,
                   f.farm_name as farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE l.status = 'unweaned'
            ORDER BY l.farrowing_date DESC
        """)
        
        litters = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'litters': litters
        })
        
    except Exception as e:
        print(f"Error getting active litters: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get active litters: {str(e)}'})

@app.route('/api/litter/<litter_id>/wean', methods=['POST'])
def mark_litter_weaned(litter_id):
    """Mark a litter as weaned"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get litter details
        cursor.execute("""
            SELECT l.*, p.tag_id as sow_tag_id
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            WHERE l.litter_id = %s
        """, (litter_id,))
        
        litter = cursor.fetchone()
        if not litter:
            return jsonify({'success': False, 'message': 'Litter not found'})
        
        # Check if litter is already weaned
        if litter['weaning_date']:
            return jsonify({'success': False, 'message': 'Litter is already marked as weaned'})
        
        # Check if all farrowing activities are completed
        cursor.execute("""
            SELECT COUNT(*) as total_activities,
                   SUM(CASE WHEN completed = TRUE THEN 1 ELSE 0 END) as completed_activities
            FROM farrowing_activities fa
            JOIN litters l ON l.farrowing_record_id = fa.farrowing_record_id
            WHERE l.litter_id = %s
        """, (litter_id,))
        
        activities_result = cursor.fetchone()
        if activities_result['total_activities'] != activities_result['completed_activities']:
            return jsonify({
                'success': False, 
                'message': 'Cannot mark litter as weaned until all farrowing activities are completed'
            })
        
        # Update litter status to weaned
        cursor.execute("""
            UPDATE litters 
            SET status = 'weaned', updated_at = CURRENT_TIMESTAMP
            WHERE litter_id = %s
        """, (litter_id,))
        
        # Log activity
        log_activity(session['employee_id'], 'LITTER_WEANED', 
                    f'Marked litter {litter_id} as weaned for sow {litter["sow_tag_id"]}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Litter {litter_id} marked as weaned successfully'
        })
        
    except Exception as e:
        print(f"Error marking litter as weaned: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to mark litter as weaned: {str(e)}'})

@app.route('/api/litter/weaned', methods=['GET'])
def get_weaned_litters():
    """Get all weaned litters"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get weaned litters with sow information
        cursor.execute("""
            SELECT l.*, 
                   p.tag_id as sow_tag_id, p.breed as sow_breed,
                   f.farm_name as farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE l.status = 'weaned'
            ORDER BY l.weaning_date DESC
        """)
        
        litters = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'litters': litters
        })
        
    except Exception as e:
        print(f"Error getting weaned litters: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get weaned litters: {str(e)}'})

@app.route('/api/litter/all', methods=['GET'])
def get_all_litters():
    """Get all litters (both unweaned and weaned)"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all litters with sow information
        cursor.execute("""
            SELECT l.*, 
                   p.tag_id as sow_tag_id, p.breed as sow_breed,
                   f.farm_name as farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            ORDER BY l.farrowing_date DESC
        """)
        
        litters = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'litters': litters
        })
        
    except Exception as e:
        print(f"Error getting all litters: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get all litters: {str(e)}'})

@app.route('/api/animal/<int:animal_id>/weights', methods=['GET'])
def get_animal_weights(animal_id):
    """Get weight records for a specific animal"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get weight records for the specific animal
        cursor.execute("""
            SELECT w.id, w.animal_id, w.litter_id, w.weight, w.expected_weight, w.weight_type, w.weighing_date, 
                   w.weighing_time, w.notes, 
                   w.created_at, w.updated_at,
                   p.tag_id, p.name, p.breed
            FROM weight_records w
            LEFT JOIN pigs p ON w.animal_id = p.id
            WHERE w.animal_id = %s
            ORDER BY w.weighing_date DESC, w.created_at DESC
        """, (animal_id,))
        
        weights = cursor.fetchall()
        
        # Convert datetime objects to strings for JSON serialization
        for weight in weights:
            if weight.get('weighing_date'):
                weight['weighing_date'] = str(weight['weighing_date'])
            if weight.get('weighing_time'):
                # Convert timedelta to HH:MM format
                if hasattr(weight['weighing_time'], 'total_seconds'):
                    # It's a timedelta object
                    total_seconds = int(weight['weighing_time'].total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    weight['weighing_time'] = f"{hours:02d}:{minutes:02d}"
                else:
                    # It's already a string or time object
                    weight['weighing_time'] = str(weight['weighing_time'])
            if weight.get('created_at'):
                weight['created_at'] = str(weight['created_at'])
            if weight.get('updated_at'):
                weight['updated_at'] = str(weight['updated_at'])
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'weights': weights
        })
        
    except Exception as e:
        print(f"Error getting animal weights: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get weight records: {str(e)}'})

@app.route('/api/animal/record-weight', methods=['POST'])
def record_animal_weight():
    """Record a new weight for an animal"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        animal_id = data.get('animal_id')
        actual_weight = data.get('actual_weight') or data.get('weight')  # Support both field names
        expected_weight = data.get('expected_weight')
        weighing_date = data.get('weighing_date')
        weighing_time = data.get('weighing_time')
        notes = data.get('notes', '')
        weight_category = data.get('weight_category', '')
        daily_gain = data.get('daily_gain', 0)
        age_days = data.get('age_days', 0)
        
        if not animal_id or not actual_weight or not weighing_date:
            return jsonify({'success': False, 'message': 'Missing required fields'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Use provided expected weight or calculate it
        if not expected_weight:
            from datetime import datetime
            weighing_date_obj = datetime.strptime(weighing_date, '%Y-%m-%d').date()
            expected_weight = calculate_expected_weight(animal_id=animal_id, weighing_date=weighing_date_obj)
        
        # Insert single weight record with both actual and expected weights
        cursor.execute("""
            INSERT INTO weight_records (animal_id, weight, expected_weight, weight_type, weighing_date, weighing_time, notes, created_at, updated_at)
            VALUES (%s, %s, %s, 'actual', %s, %s, %s, NOW(), NOW())
        """, (animal_id, actual_weight, expected_weight, weighing_date, weighing_time, notes))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Weight recorded successfully'
        })
        
    except Exception as e:
        print(f"Error recording weight: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to record weight: {str(e)}'})

@app.route('/api/animal/<int:animal_id>/details', methods=['GET'])
def get_animal_details(animal_id):
    """Get animal details including birth date for age calculation"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get animal details with birth date and status
        cursor.execute("""
            SELECT p.id, p.tag_id, p.name, p.breed, p.gender, p.birth_date, p.farm_id, p.status,
                   f.farm_name, DATEDIFF(CURDATE(), p.birth_date) as age_days
            FROM pigs p
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE p.id = %s
        """, (animal_id,))
        
        animal = cursor.fetchone()
        if not animal:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Animal not found'})
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'animal': animal
        })
        
    except Exception as e:
        print(f"Error getting animal details: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get animal details: {str(e)}'})

@app.route('/api/litter/<int:litter_id>/details', methods=['GET'])
def get_litter_details(litter_id):
    """Get litter details including farrowing date for age calculation"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get litter details with farrowing date
        cursor.execute("""
            SELECT l.id, l.litter_id, l.farrowing_date, l.sow_id, l.boar_id,
                   l.total_piglets, l.alive_piglets, l.status,
                   p.tag_id as sow_tag_id, p.breed as sow_breed,
                   f.farm_name, DATEDIFF(CURDATE(), l.farrowing_date) as age_days
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE l.id = %s
        """, (litter_id,))
        
        litter = cursor.fetchone()
        if not litter:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Litter not found'})
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'litter': litter
        })
        
    except Exception as e:
        print(f"Error getting litter details: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get litter details: {str(e)}'})

@app.route('/api/litter/<int:litter_id>/weights', methods=['GET'])
def get_litter_weights(litter_id):
    """Get weight records for a specific litter"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get weight records for the specific litter
        cursor.execute("""
            SELECT w.id, w.animal_id, w.litter_id, w.weight, w.expected_weight, w.weight_type, w.weighing_date, 
                   w.weighing_time, w.notes, 
                   w.created_at, w.updated_at,
                   l.litter_id, p.tag_id as sow_tag_id, p.breed as sow_breed
            FROM weight_records w
            LEFT JOIN litters l ON w.litter_id = l.id
            LEFT JOIN pigs p ON l.sow_id = p.id
            WHERE w.litter_id = %s
            ORDER BY w.weighing_date DESC, w.created_at DESC
        """, (litter_id,))
        
        weights = cursor.fetchall()
        
        # Convert datetime objects to strings for JSON serialization
        for weight in weights:
            if weight.get('weighing_date'):
                weight['weighing_date'] = str(weight['weighing_date'])
            if weight.get('weighing_time'):
                # Convert timedelta to HH:MM format
                if hasattr(weight['weighing_time'], 'total_seconds'):
                    # It's a timedelta object
                    total_seconds = int(weight['weighing_time'].total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    weight['weighing_time'] = f"{hours:02d}:{minutes:02d}"
                else:
                    # It's already a string or time object
                    weight['weighing_time'] = str(weight['weighing_time'])
            if weight.get('created_at'):
                weight['created_at'] = str(weight['created_at'])
            if weight.get('updated_at'):
                weight['updated_at'] = str(weight['updated_at'])
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'weights': weights
        })
        
    except Exception as e:
        print(f"Error getting litter weights: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get weight records: {str(e)}'})

@app.route('/api/litter/record-weight', methods=['POST'])
def record_litter_weight():
    """Record a new weight for a litter"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        litter_id = data.get('litter_id')
        actual_weight = data.get('actual_weight') or data.get('weight')  # Support both field names
        expected_weight = data.get('expected_weight')
        weighing_date = data.get('weighing_date')
        weighing_time = data.get('weighing_time')
        notes = data.get('notes', '')
        weight_category = data.get('weight_category', '')
        daily_gain = data.get('daily_gain', 0)
        age_days = data.get('age_days', 0)
        
        if not litter_id or not actual_weight or not weighing_date:
            return jsonify({'success': False, 'message': 'Missing required fields'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Use provided expected weight or calculate it
        if not expected_weight:
            from datetime import datetime
            weighing_date_obj = datetime.strptime(weighing_date, '%Y-%m-%d').date()
            expected_weight = calculate_expected_weight(litter_id=litter_id, weighing_date=weighing_date_obj)
        
        # Insert single weight record with both actual and expected weights
        cursor.execute("""
            INSERT INTO weight_records (litter_id, weight, expected_weight, weight_type, weighing_date, weighing_time, notes, created_at, updated_at)
            VALUES (%s, %s, %s, 'actual', %s, %s, %s, NOW(), NOW())
        """, (litter_id, actual_weight, expected_weight, weighing_date, weighing_time, notes))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Weight recorded successfully'
        })
        
    except Exception as e:
        print(f"Error recording litter weight: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to record weight: {str(e)}'})

@app.route('/api/litter/check-id/<litter_id>', methods=['GET'])
def check_litter_id(litter_id):
    """Check if a litter ID already exists"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get exclude parameter if provided
        exclude_id = request.args.get('exclude', None)
        
        # Check if litter ID exists
        if exclude_id:
            cursor.execute("SELECT id FROM litters WHERE litter_id = %s AND id != %s", (litter_id, exclude_id))
        else:
            cursor.execute("SELECT id FROM litters WHERE litter_id = %s", (litter_id,))
        
        existing_litter = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'exists': existing_litter is not None
        })
        
    except Exception as e:
        print(f"Error checking litter ID: {str(e)}")
        return jsonify({'exists': False, 'error': str(e)})

@app.route('/api/litter/update/<int:litter_id>', methods=['PUT'])
def update_litter(litter_id):
    """Update litter details"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['litter_id', 'sow_id', 'farrowing_date', 'alive_piglets', 'status']
        for field in required_fields:
            if field not in data or data[field] is None:
                return jsonify({'success': False, 'message': f'Missing required field: {field}'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if litter exists
        cursor.execute("SELECT * FROM litters WHERE id = %s", (litter_id,))
        existing_litter = cursor.fetchone()
        
        if not existing_litter:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Litter not found'})
        
        # Check if new litter_id already exists (excluding current litter)
        if data['litter_id'] != existing_litter['litter_id']:
            cursor.execute("SELECT id FROM litters WHERE litter_id = %s AND id != %s", (data['litter_id'], litter_id))
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'message': 'Litter ID already exists'})
        
        # Validate farrowing date is not in the future
        farrowing_date = datetime.strptime(data['farrowing_date'], '%Y-%m-%d').date()
        if farrowing_date > datetime.now().date():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'message': 'Farrowing date cannot be in the future'})
        
        # Validate weaning date if provided
        weaning_date = None
        if data.get('weaning_date'):
            weaning_date = datetime.strptime(data['weaning_date'], '%Y-%m-%d').date()
            if weaning_date < farrowing_date:
                cursor.close()
                conn.close()
                return jsonify({'success': False, 'message': 'Weaning date cannot be before farrowing date'})
        
        # Prepare the new litter ID with "E" prefix if it's being changed
        new_litter_id = data['litter_id']
        if new_litter_id != existing_litter['litter_id'] and not new_litter_id.startswith('E'):
            new_litter_id = f"E{data['litter_id']}"
        
        # Update the litter
        update_query = """
            UPDATE litters 
            SET litter_id = %s, sow_id = %s, farrowing_date = %s, 
                weaning_date = %s, alive_piglets = %s, weaning_weight = %s, 
                status = %s, updated_at = NOW()
            WHERE id = %s
        """
        
        cursor.execute(update_query, (
            new_litter_id,
            data['sow_id'],
            data['farrowing_date'],
            data.get('weaning_date'),
            data['alive_piglets'],
            data.get('weaning_weight'),
            data['status'],
            litter_id
        ))
        
        # Log the activity
        activity_description = f"Litter details updated: ID changed to {new_litter_id}, Sow: {data['sow_id']}, Farrowing: {data['farrowing_date']}, Alive piglets: {data['alive_piglets']}, Status: {data['status']}"
        
        cursor.execute("""
            INSERT INTO litter_activities (litter_id, activity_type, description, created_at)
            VALUES (%s, %s, %s, NOW())
        """, (litter_id, 'updated', activity_description))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Litter {new_litter_id} updated successfully',
            'litter_id': new_litter_id
        })
        
    except Exception as e:
        print(f"Error updating litter: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to update litter: {str(e)}'})

@app.route('/api/litter/<litter_id>/activities', methods=['GET'])
def get_litter_activities(litter_id):
    """Get all activities for a specific litter"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print(f"🔍 Looking for litter: {litter_id}")
        
        # First, check if the litter exists
        cursor.execute("SELECT * FROM litters WHERE litter_id = %s", (litter_id,))
        litter_check = cursor.fetchone()
        
        if not litter_check:
            print(f"❌ Litter {litter_id} not found in litters table")
            return jsonify({'success': False, 'message': f'Litter {litter_id} not found in database'})
        
        print(f"✅ Found litter: {litter_check}")
        
        # Check if there's a farrowing record
        if not litter_check.get('farrowing_record_id'):
            print(f"❌ Litter {litter_id} has no farrowing_record_id")
            return jsonify({'success': False, 'message': f'Litter {litter_id} has no associated farrowing record'})
        
        # Get farrowing record details
        cursor.execute("""
            SELECT fr.*
            FROM farrowing_records fr
            WHERE fr.id = %s
        """, (litter_check['farrowing_record_id'],))
        
        farrowing_record = cursor.fetchone()
        if not farrowing_record:
            print(f"❌ Farrowing record {litter_check['farrowing_record_id']} not found")
            return jsonify({'success': False, 'message': f'Farrowing record not found for litter {litter_id}'})
        
        print(f"✅ Found farrowing record: {farrowing_record}")
        
        # Use litter_check instead of litter
        litter = litter_check
        
        # Get all farrowing activities for this litter
        cursor.execute("""
            SELECT fa.*, 
                   CASE 
                       WHEN fa.completed = TRUE THEN 'Completed'
                       WHEN fa.due_date < CURRENT_DATE THEN 'Overdue'
                       WHEN fa.due_date = CURRENT_DATE THEN 'Due Today'
                       ELSE 'Upcoming'
                   END as status_category
            FROM farrowing_activities fa
            WHERE fa.farrowing_record_id = %s
            ORDER BY fa.due_day ASC
        """, (litter['farrowing_record_id'],))
        
        activities = cursor.fetchall()
        
        print(f"✅ Found {len(activities)} activities for litter {litter_id} (farrowing_record_id: {litter['farrowing_record_id']})")
        
        # Get sow information from the litter record BEFORE closing cursor
        cursor.execute("""
            SELECT p.tag_id as sow_tag_id
            FROM pigs p
            WHERE p.id = %s
        """, (litter_check['sow_id'],))
        
        sow_info = cursor.fetchone()
        sow_tag_id = sow_info['sow_tag_id'] if sow_info else 'N/A'
        
        cursor.close()
        conn.close()
        
        # Combine litter and farrowing data
        litter_data = {
            'id': litter_check['id'],
            'litter_id': litter_check['litter_id'],
            'farrowing_record_id': litter_check['farrowing_record_id'],
            'sow_id': litter_check['sow_id'],
            'farrowing_date': litter_check['farrowing_date'],
            'alive_piglets': litter_check['alive_piglets'],
            'status': litter_check['status'],
            'sow_tag_id': sow_tag_id
        }
        
        return jsonify({
            'success': True,
            'activities': activities,
            'litter': litter_data
        })
        
    except Exception as e:
        print(f"Error getting litter activities: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get litter activities: {str(e)}'})

def check_and_trigger_recovery_period(farrowing_record_id):
    """Check if all farrowing activities are completed and trigger 40-day recovery period"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if all activities for this farrowing are completed
        cursor.execute("""
            SELECT COUNT(*) as total_activities,
                   SUM(CASE WHEN completed = TRUE THEN 1 ELSE 0 END) as completed_activities
            FROM farrowing_activities 
            WHERE farrowing_record_id = %s
        """, (farrowing_record_id,))
        
        result = cursor.fetchone()
        if result['total_activities'] == result['completed_activities'] and result['total_activities'] > 0:
            print(f"All activities completed for farrowing {farrowing_record_id}, starting 40-day recovery period")
            
            # Get farrowing details
            cursor.execute("""
                SELECT fr.farrowing_date, fr.id, br.sow_id
                FROM farrowing_records fr
                JOIN breeding_records br ON fr.breeding_id = br.id
                WHERE fr.id = %s
            """, (farrowing_record_id,))
            
            farrowing = cursor.fetchone()
            if farrowing:
                # Calculate recovery date (40 days from farrowing)
                from datetime import timedelta
                recovery_date = farrowing['farrowing_date'] + timedelta(days=40)
                
                # Log the recovery period start
                log_activity(session.get('employee_id', 1), 'RECOVERY_PERIOD_STARTED', 
                           f'40-day recovery period started for farrowing {farrowing_record_id}, sow ready on {recovery_date}')
                
                print(f"📅 Sow will be ready for next breeding on: {recovery_date}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error checking recovery period: {str(e)}")

def check_and_update_litter_status(farrowing_record_id):
    """Check if all farrowing activities are completed and update litter status to weaned"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if all activities for this farrowing are completed
        cursor.execute("""
            SELECT COUNT(*) as total_activities,
                   SUM(CASE WHEN completed = TRUE THEN 1 ELSE 0 END) as completed_activities
            FROM farrowing_activities 
            WHERE farrowing_record_id = %s
        """, (farrowing_record_id,))
        
        result = cursor.fetchone()
        if result['total_activities'] == result['completed_activities'] and result['total_activities'] > 0:
            print(f"All activities completed for farrowing {farrowing_record_id}, updating litter status to weaned")
            
            # Get the weaning activity details
            cursor.execute("""
                SELECT weaning_weight, weaning_date
                FROM farrowing_activities 
                WHERE farrowing_record_id = %s AND activity_name = 'Weaning' AND completed = TRUE
            """, (farrowing_record_id,))
            
            weaning_activity = cursor.fetchone()
            
            # Get the litter associated with this farrowing
            cursor.execute("""
                SELECT l.id, l.litter_id, l.status
                FROM litters l
                WHERE l.farrowing_record_id = %s
            """, (farrowing_record_id,))
            
            litter = cursor.fetchone()
            
            if litter and litter['status'] != 'weaned':
                # Update litter status to weaned
                weaning_weight = weaning_activity['weaning_weight'] if weaning_activity else None
                weaning_date = weaning_activity['weaning_date'] if weaning_activity else None
                
                cursor.execute("""
                    UPDATE litters 
                    SET status = 'weaned', weaning_date = %s, weaning_weight = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (weaning_date, weaning_weight, litter['id']))
                
                print(f"✅ Updated litter {litter['litter_id']} status to 'weaned'")
                
                # Log the activity
                log_activity(session.get('employee_id', 1), 'LITTER_WEANED_AUTO', 
                           f'Litter {litter["litter_id"]} automatically marked as weaned after all activities completed')
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error checking and updating litter status: {str(e)}")

# Weight Settings API Endpoints
@app.route('/api/weight/settings', methods=['GET'])
def get_weight_settings():
    """Get all weight settings"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT setting_name, setting_value, setting_type, description 
            FROM weight_settings 
            ORDER BY setting_name
        """)
        
        settings = cursor.fetchall()
        settings_dict = {}
        for setting in settings:
            settings_dict[setting['setting_name']] = {
                'value': setting['setting_value'],
                'type': setting['setting_type'],
                'description': setting['description']
            }
        
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'settings': settings_dict})
        
    except Exception as e:
        print(f"Error fetching weight settings: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/weight/categories', methods=['GET'])
def get_weight_categories():
    """Get all weight categories"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, category_name, min_weight, max_weight, daily_gain, start_age, end_age
            FROM weight_categories
            ORDER BY start_age ASC
        """)
        
        categories = cursor.fetchall()
        print(f"Found {len(categories)} weight categories")
        for cat in categories:
            print(f"Category: {cat}")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'categories': categories
        })
        
    except Exception as e:
        print(f"Error getting weight categories: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get weight categories: {str(e)}'})

@app.route('/api/test/weight-categories', methods=['GET'])
def test_weight_categories():
    """Test route to check weight categories table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SHOW TABLES LIKE 'weight_categories'")
        table_exists = cursor.fetchone()
        print(f"Weight categories table exists: {table_exists is not None}")
        
        if table_exists:
            # Check table structure
            cursor.execute("DESCRIBE weight_categories")
            columns = cursor.fetchall()
            print(f"Table columns: {columns}")
            
            # Check if there's any data
            cursor.execute("SELECT COUNT(*) as count FROM weight_categories")
            count = cursor.fetchone()
            print(f"Number of records: {count['count'] if count else 0}")
            
            # Get sample data
            cursor.execute("SELECT * FROM weight_categories LIMIT 3")
            sample_data = cursor.fetchall()
            print(f"Sample data: {sample_data}")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'table_exists': table_exists is not None,
            'columns': columns if table_exists else [],
            'count': count['count'] if table_exists and count else 0,
            'sample_data': sample_data if table_exists else []
        })
        
    except Exception as e:
        print(f"Error testing weight categories: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to test weight categories: {str(e)}'})

@app.route('/api/analytics/weight-comparison', methods=['GET'])
def get_weight_comparison_data():
    """Get actual vs expected weight comparison data for analytics"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get weight records with actual vs expected comparison
        cursor.execute("""
            SELECT 
                w.id,
                w.animal_id,
                w.litter_id,
                w.weight as actual_weight,
                w.expected_weight,
                w.weighing_date,
                p.tag_id as animal_tag,
                p.name as animal_name,
                p.breed,
                l.litter_id as litter_identifier,
                CASE 
                    WHEN w.animal_id IS NOT NULL THEN 'animal'
                    WHEN w.litter_id IS NOT NULL THEN 'litter'
                    ELSE 'unknown'
                END as record_type,
                ROUND(((w.expected_weight - w.weight) / w.expected_weight * 100), 2) as weight_deficit_percentage,
                CASE 
                    WHEN w.weight >= w.expected_weight THEN 'meeting_target'
                    WHEN w.weight >= (w.expected_weight * 0.9) THEN 'close_to_target'
                    ELSE 'below_target'
                END as performance_status
            FROM weight_records w
            LEFT JOIN pigs p ON w.animal_id = p.id
            LEFT JOIN litters l ON w.litter_id = l.id
            WHERE w.weight IS NOT NULL 
            AND w.expected_weight IS NOT NULL
            ORDER BY w.weighing_date DESC
            LIMIT 100
        """)
        
        weight_records = cursor.fetchall()
        
        # Convert datetime objects to strings for JSON serialization
        for record in weight_records:
            if record.get('weighing_date'):
                record['weighing_date'] = str(record['weighing_date'])
        
        # Calculate summary statistics
        total_records = len(weight_records)
        meeting_target = len([r for r in weight_records if r['performance_status'] == 'meeting_target'])
        close_to_target = len([r for r in weight_records if r['performance_status'] == 'close_to_target'])
        below_target = len([r for r in weight_records if r['performance_status'] == 'below_target'])
        
        # Calculate average deficit
        avg_deficit = 0
        if weight_records:
            total_deficit = sum([r['weight_deficit_percentage'] for r in weight_records if r['weight_deficit_percentage'] > 0])
            deficit_count = len([r for r in weight_records if r['weight_deficit_percentage'] > 0])
            avg_deficit = total_deficit / deficit_count if deficit_count > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'weight_records': weight_records,
            'summary': {
                'total_records': total_records,
                'meeting_target': meeting_target,
                'close_to_target': close_to_target,
                'below_target': below_target,
                'avg_deficit_percentage': round(avg_deficit, 2)
            }
        })
        
    except Exception as e:
        print(f"Error getting weight comparison data: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get weight comparison data: {str(e)}'})

@app.route('/api/analytics/underweight-animals', methods=['GET'])
def get_underweight_animals():
    """Get animals whose last actual weight is below expected weight"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get animals with their latest weight records and expected weights
        cursor.execute("""
            SELECT p.id, p.tag_id, p.name, p.breed, p.gender, p.birth_date,
                   f.farm_name, DATEDIFF(CURDATE(), p.birth_date) as age_days,
                   w.weight as actual_weight, w.expected_weight, w.weighing_date,
                   ROUND(((w.expected_weight - w.weight) / w.expected_weight * 100), 2) as weight_deficit_percentage
            FROM pigs p
            LEFT JOIN farms f ON p.farm_id = f.id
            LEFT JOIN (
                SELECT animal_id, weight, expected_weight, weighing_date,
                       ROW_NUMBER() OVER (PARTITION BY animal_id ORDER BY weighing_date DESC) as rn
                FROM weight_records
                WHERE weight_type = 'actual'
            ) w ON p.id = w.animal_id AND w.rn = 1
            WHERE w.weight IS NOT NULL 
            AND w.expected_weight IS NOT NULL 
            AND w.weight < w.expected_weight
            ORDER BY weight_deficit_percentage DESC
        """)
        
        underweight_animals = cursor.fetchall()
        
        # Convert datetime objects to strings for JSON serialization
        for animal in underweight_animals:
            if animal.get('birth_date'):
                animal['birth_date'] = str(animal['birth_date'])
            if animal.get('weighing_date'):
                animal['weighing_date'] = str(animal['weighing_date'])
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'animals': underweight_animals
        })
        
    except Exception as e:
        print(f"Error getting underweight animals: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get underweight animals: {str(e)}'})

@app.route('/api/analytics/farm-overview', methods=['GET'])
def get_farm_overview():
    """Get farm overview statistics"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total animals
        cursor.execute("SELECT COUNT(*) as total_animals FROM pigs")
        total_animals = cursor.fetchone()['total_animals']
        
        # Get total litters
        cursor.execute("SELECT COUNT(*) as total_litters FROM litters")
        total_litters = cursor.fetchone()['total_litters']
        
        # Get average weight from latest weight records
        cursor.execute("""
            SELECT AVG(w.weight) as avg_weight
            FROM (
                SELECT animal_id, weight,
                       ROW_NUMBER() OVER (PARTITION BY animal_id ORDER BY weighing_date DESC) as rn
                FROM weight_records
                WHERE weight_type = 'actual'
            ) w
            WHERE w.rn = 1
        """)
        avg_weight_result = cursor.fetchone()
        avg_weight = avg_weight_result['avg_weight'] if avg_weight_result['avg_weight'] else 0
        
        # Get performance status based on weight gain trends
        cursor.execute("""
            SELECT 
                COUNT(CASE WHEN w.weight >= w.expected_weight THEN 1 END) as meeting_target,
                COUNT(*) as total_with_expected
            FROM (
                SELECT animal_id, weight, expected_weight,
                       ROW_NUMBER() OVER (PARTITION BY animal_id ORDER BY weighing_date DESC) as rn
                FROM weight_records
                WHERE weight_type = 'actual' AND expected_weight IS NOT NULL
            ) w
            WHERE w.rn = 1
        """)
        performance_result = cursor.fetchone()
        
        performance_ratio = 0
        if performance_result['total_with_expected'] > 0:
            performance_ratio = performance_result['meeting_target'] / performance_result['total_with_expected']
        
        performance_status = 'Good'
        if performance_ratio < 0.5:
            performance_status = 'Poor'
        elif performance_ratio < 0.7:
            performance_status = 'Needs Attention'
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'overview': {
                'total_animals': total_animals,
                'total_litters': total_litters,
                'avg_weight': round(avg_weight, 1),
                'performance_status': performance_status,
                'performance_ratio': round(performance_ratio * 100, 1)
            }
        })
        
    except Exception as e:
        print(f"Error getting farm overview: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to get farm overview: {str(e)}'})

@app.route('/api/weight/settings', methods=['POST'])
def save_weight_settings():
    """Save weight settings"""
    try:
        data = request.get_json()
        employee_id = session.get('employee_id')
        
        if not employee_id:
            return jsonify({'success': False, 'error': 'Not authenticated'}), 401
        
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Save each setting
        for setting_name, setting_data in data.items():
            setting_value = setting_data.get('value', '')
            setting_type = setting_data.get('type', 'text')
            description = setting_data.get('description', '')
            
            # Insert or update setting
            cursor.execute("""
                INSERT INTO weight_settings (setting_name, setting_value, setting_type, description, created_by)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                setting_value = VALUES(setting_value),
                setting_type = VALUES(setting_type),
                description = VALUES(description),
                updated_at = CURRENT_TIMESTAMP
            """, (setting_name, setting_value, setting_type, description, employee_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Settings saved successfully'})
        
    except Exception as e:
        print(f"Error saving weight settings: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/weight/categories', methods=['POST'])
def save_weight_category():
    """Save a new weight category"""
    try:
        data = request.get_json()
        employee_id = session.get('employee_id')
        
        if not employee_id:
            return jsonify({'success': False, 'error': 'Not authenticated'}), 401
        
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO weight_categories (start_age, end_age, category_name, min_weight, max_weight, daily_gain, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data['start_age'], data['end_age'], data['category_name'],
            data['min_weight'], data['max_weight'], data['daily_gain'], employee_id
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Category saved successfully'})
        
    except Exception as e:
        print(f"Error saving weight category: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/weight/categories/<int:category_id>', methods=['PUT'])
def update_weight_category(category_id):
    """Update a weight category"""
    try:
        data = request.get_json()
        employee_id = session.get('employee_id')
        
        if not employee_id:
            return jsonify({'success': False, 'error': 'Not authenticated'}), 401
        
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE weight_categories 
            SET start_age = %s, end_age = %s, category_name = %s, 
                min_weight = %s, max_weight = %s, daily_gain = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (
            data['start_age'], data['end_age'], data['category_name'],
            data['min_weight'], data['max_weight'], data['daily_gain'], category_id
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Category updated successfully'})
        
    except Exception as e:
        print(f"Error updating weight category: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/weight/categories/<int:category_id>', methods=['DELETE'])
def delete_weight_category(category_id):
    """Delete a weight category"""
    try:
        employee_id = session.get('employee_id')
        
        if not employee_id:
            return jsonify({'success': False, 'error': 'Not authenticated'}), 401
        
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM weight_categories WHERE id = %s", (category_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Category deleted successfully'})
        
    except Exception as e:
        print(f"Error deleting weight category: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Vaccination API endpoints
@app.route('/api/vaccination/animals', methods=['GET'])
def get_vaccination_animals():
    """Get all animals (pigs and litters) for vaccination tracking"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all pigs - using basic columns first
        cursor.execute("""
            SELECT p.id, p.tag_id, p.farm_id, p.pig_type, p.pig_source, p.breed, 
                   p.purpose, p.breeding_status, p.birth_date, p.purchase_date, p.age_days,
                   p.registered_by, p.status, p.created_at, p.updated_at, f.farm_name 
            FROM pigs p 
            LEFT JOIN farms f ON p.farm_id = f.id 
            WHERE p.status = 'active' AND p.pig_type IN ('grown_pig', 'piglet', 'batch')
            ORDER BY p.created_at DESC
        """)
        pigs = cursor.fetchall()
        
        # Get all litters - using basic columns first
        cursor.execute("""
            SELECT l.id, l.litter_id, l.farrowing_record_id, l.sow_id, l.boar_id,
                   l.total_piglets, l.alive_piglets, l.still_births,
                   l.avg_weight, l.weaning_weight, l.weaning_date, l.farrowing_date,
                   l.status, l.created_at, l.updated_at,
                   p.tag_id as sow_tag, p.breed as sow_breed,
                   f.farm_name
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            LEFT JOIN farms f ON p.farm_id = f.id
            WHERE l.status IN ('unweaned', 'weaned')
            ORDER BY l.created_at DESC
        """)
        litters = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'pigs': pigs,
            'litters': litters
        })
        
    except Exception as e:
        print(f"Error getting vaccination animals: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Slaughter Management API endpoints
@app.route('/api/slaughter/records', methods=['GET'])
def get_slaughter_records():
    """Get all slaughter records"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all slaughter records with pig/litter and employee information
        cursor.execute("""
            SELECT sr.*, 
                   p.tag_id as pig_tag_id, p.breed as pig_breed, p.gender as pig_gender,
                   l.litter_id,
                   sow.breed as litter_breed,
                   e.full_name as created_by_name,
                   CASE 
                       WHEN EXISTS(SELECT 1 FROM slaughter_records_edit_history sreh WHERE sreh.record_id = sr.id) 
                       THEN 1 
                       ELSE 0 
                   END as is_edited
            FROM slaughter_records sr
            LEFT JOIN pigs p ON sr.pig_id = p.id
            LEFT JOIN litters l ON sr.litter_id = l.id
            LEFT JOIN pigs sow ON l.sow_id = sow.id
            LEFT JOIN employees e ON sr.created_by = e.id
            ORDER BY sr.slaughter_date DESC, sr.created_at DESC
        """)
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': records
        })
        
    except Exception as e:
        print(f"Error fetching slaughter records: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/slaughter/record', methods=['POST'])
def create_slaughter_record():
    """Create a new slaughter record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        print(f"Slaughter record data received: {data}")
        
        # Validate required fields
        required_fields = ['pig_type', 'slaughter_date', 'live_weight', 'carcass_weight', 
                          'meat_grade', 'price_per_kg', 'buyer_name']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Calculate dressing percentage
        live_weight = float(data['live_weight'])
        carcass_weight = float(data['carcass_weight'])
        dressing_percentage = (carcass_weight / live_weight) * 100
        
        # Calculate total revenue
        price_per_kg = float(data['price_per_kg'])
        pigs_count = int(data.get('pigs_count', 1))
        total_revenue = carcass_weight * price_per_kg * pigs_count
        
        # Insert slaughter record
        cursor.execute("""
            INSERT INTO slaughter_records (
                pig_id, litter_id, pig_type, slaughter_date, live_weight, carcass_weight,
                dressing_percentage, meat_grade, price_per_kg, total_revenue, buyer_name,
                pigs_count, notes, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get('pig_id'), data.get('litter_id'), data['pig_type'],
            data['slaughter_date'], live_weight, carcass_weight, dressing_percentage,
            data['meat_grade'], price_per_kg, total_revenue, data['buyer_name'],
            pigs_count, data.get('notes', ''), session['employee_id']
        ))
        
        record_id = cursor.lastrowid
        
        # Update pig status to slaughtered
        if data['pig_type'] == 'grown_pig' and data.get('pig_id'):
            print(f"Updating pig {data['pig_id']} status to 'slaughtered'")
            cursor.execute("""
                UPDATE pigs 
                SET status = 'slaughtered', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (data['pig_id'],))
            print(f"Pig status update affected {cursor.rowcount} rows")
            
            # Verify the status update
            cursor.execute("SELECT status FROM pigs WHERE id = %s", (data['pig_id'],))
            updated_pig = cursor.fetchone()
            print(f"Pig {data['pig_id']} status after update: {updated_pig['status'] if updated_pig else 'NOT FOUND'}")
            
        elif data['pig_type'] == 'litter' and data.get('litter_id'):
            # For litters, reduce the number of available pigs
            pigs_to_slaughter = int(data.get('pigs_count', 1))
            
            # Update litter to reduce available pigs
            cursor.execute("""
                UPDATE litters 
                SET alive_piglets = alive_piglets - %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND alive_piglets >= %s
            """, (pigs_to_slaughter, data['litter_id'], pigs_to_slaughter))
            
            # Check if update was successful
            if cursor.rowcount == 0:
                raise Exception("Not enough pigs available in this litter for slaughter")
            
            # Get updated litter info
            cursor.execute("""
                SELECT alive_piglets, total_piglets FROM litters WHERE id = %s
            """, (data['litter_id'],))
            litter_info = cursor.fetchone()
            
            # If no pigs left, update litter status to slaughtered
            if litter_info and litter_info['alive_piglets'] <= 0:
                print(f"Updating litter {data['litter_id']} status to 'slaughtered'")
                cursor.execute("""
                    UPDATE litters 
                    SET status = 'slaughtered', updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (data['litter_id'],))
                print(f"Litter status update affected {cursor.rowcount} rows")
                
                # Verify the litter status update
                cursor.execute("SELECT status, alive_piglets FROM litters WHERE id = %s", (data['litter_id'],))
                updated_litter = cursor.fetchone()
                print(f"Litter {data['litter_id']} status after update: {updated_litter['status'] if updated_litter else 'NOT FOUND'}, alive_piglets: {updated_litter['alive_piglets'] if updated_litter else 'N/A'}")
                
                # Check if all litters from the parent sow are slaughtered
                cursor.execute("""
                    SELECT sow_id FROM litters WHERE id = %s
                """, (data['litter_id'],))
                sow_result = cursor.fetchone()
                
                if sow_result:
                    sow_id = sow_result['sow_id']
                    cursor.execute("""
                        SELECT COUNT(*) as total_litters,
                               SUM(CASE WHEN status = 'slaughtered' THEN 1 ELSE 0 END) as slaughtered_litters
                        FROM litters 
                        WHERE sow_id = %s
                    """, (sow_id,))
                    
                    litter_stats = cursor.fetchone()
                    if litter_stats and litter_stats['total_litters'] == litter_stats['slaughtered_litters']:
                        # All litters are slaughtered, update sow status
                        cursor.execute("""
                            UPDATE pigs 
                            SET status = 'slaughtered', updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (sow_id,))
        
        # Log activity
        if data['pig_type'] == 'grown_pig':
            log_activity(session['employee_id'], 'SLAUGHTER_RECORD', 
                       f'Slaughter record created: Pig {data.get("pig_id")} - Status changed to slaughtered - ${total_revenue} revenue')
        else:
            pigs_count = int(data.get('pigs_count', 1))
            log_activity(session['employee_id'], 'SLAUGHTER_RECORD', 
                       f'Slaughter record created: Litter {data.get("litter_id")} - {pigs_count} pigs slaughtered - ${total_revenue} revenue')
        
        conn.commit()
        
        # Final verification after commit
        if data['pig_type'] == 'grown_pig' and data.get('pig_id'):
            cursor.execute("SELECT status FROM pigs WHERE id = %s", (data['pig_id'],))
            final_pig = cursor.fetchone()
            print(f"FINAL: Pig {data['pig_id']} status after commit: {final_pig['status'] if final_pig else 'NOT FOUND'}")
        elif data['pig_type'] == 'litter' and data.get('litter_id'):
            cursor.execute("SELECT status, alive_piglets FROM litters WHERE id = %s", (data['litter_id'],))
            final_litter = cursor.fetchone()
            print(f"FINAL: Litter {data['litter_id']} status after commit: {final_litter['status'] if final_litter else 'NOT FOUND'}, alive_piglets: {final_litter['alive_piglets'] if final_litter else 'N/A'}")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Slaughter record created successfully',
            'record_id': record_id
        })
        
    except Exception as e:
        print(f"Error creating slaughter record: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/slaughter/record/<int:record_id>/edit', methods=['PUT'])
def edit_slaughter_record(record_id):
    """Edit a slaughter record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['slaughter_date', 'live_weight', 'carcass_weight', 'buyer_name', 'price_per_kg', 'total_revenue']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get original data for comparison
        cursor.execute("""
            SELECT slaughter_date, live_weight, carcass_weight, buyer_name, 
                   price_per_kg, total_revenue, meat_grade, notes
            FROM slaughter_records WHERE id = %s
        """, (record_id,))
        original_data = cursor.fetchone()
        
        if not original_data:
            return jsonify({'success': False, 'message': 'Slaughter record not found'}), 404
        
        # Track changes for audit trail
        changes = []
        
        # Compare each field and track changes
        if str(original_data['slaughter_date']) != str(data.get('slaughter_date')):
            changes.append(('slaughter_date', str(original_data['slaughter_date']), str(data.get('slaughter_date'))))
        if str(original_data['live_weight']) != str(data.get('live_weight')):
            changes.append(('live_weight', str(original_data['live_weight']), str(data.get('live_weight'))))
        if str(original_data['carcass_weight']) != str(data.get('carcass_weight')):
            changes.append(('carcass_weight', str(original_data['carcass_weight']), str(data.get('carcass_weight'))))
        if str(original_data['buyer_name'] or '') != str(data.get('buyer_name') or ''):
            changes.append(('buyer_name', str(original_data['buyer_name'] or ''), str(data.get('buyer_name') or '')))
        if str(original_data['price_per_kg']) != str(data.get('price_per_kg')):
            changes.append(('price_per_kg', str(original_data['price_per_kg']), str(data.get('price_per_kg'))))
        if str(original_data['total_revenue']) != str(data.get('total_revenue')):
            changes.append(('total_revenue', str(original_data['total_revenue']), str(data.get('total_revenue'))))
        if str(original_data['meat_grade'] or '') != str(data.get('meat_grade') or ''):
            changes.append(('meat_grade', str(original_data['meat_grade'] or ''), str(data.get('meat_grade') or '')))
        if str(original_data['notes'] or '') != str(data.get('notes') or ''):
            changes.append(('notes', str(original_data['notes'] or ''), str(data.get('notes') or '')))
        
        # Update slaughter record
        cursor.execute("""
            UPDATE slaughter_records SET 
                slaughter_date = %s, live_weight = %s, carcass_weight = %s,
                buyer_name = %s, price_per_kg = %s, total_revenue = %s,
                meat_grade = %s, notes = %s
            WHERE id = %s
        """, (
            data['slaughter_date'], data['live_weight'], data['carcass_weight'],
            data.get('buyer_name'), data['price_per_kg'], data['total_revenue'],
            data.get('meat_grade'), data.get('notes'), record_id
        ))
        
        # Insert audit records for each changed field
        for field_name, old_value, new_value in changes:
            cursor.execute("""
                INSERT INTO slaughter_records_edit_history 
                (record_id, field_name, old_value, new_value, edited_by)
                VALUES (%s, %s, %s, %s, %s)
            """, (record_id, field_name, old_value, new_value, session['employee_id']))
        
        # Log activity
        log_activity(session['employee_id'], 'SLAUGHTER_RECORD_EDIT', 
                   f'Slaughter record {record_id} updated: {data["live_weight"]}kg live weight')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Slaughter record updated successfully'
        })
        
    except Exception as e:
        print(f"Error updating slaughter record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to update slaughter record: {str(e)}'})

@app.route('/api/slaughter/record/<int:record_id>/delete', methods=['DELETE'])
def delete_slaughter_record(record_id):
    """Delete a slaughter record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, let's see what slaughter records exist
        cursor.execute("SELECT id, live_weight, buyer_name, slaughter_date FROM slaughter_records ORDER BY id")
        all_records = cursor.fetchall()
        print(f"All slaughter records in database: {all_records}")
        
        # Check if record exists and get details for logging
        cursor.execute("""
            SELECT live_weight, buyer_name, slaughter_date 
            FROM slaughter_records WHERE id = %s
        """, (record_id,))
        record = cursor.fetchone()
        
        if not record:
            print(f"Slaughter record {record_id} not found")
            return jsonify({'success': False, 'message': 'Slaughter record not found'}), 404
        
        print(f"Found slaughter record {record_id}: {record[0]}kg to {record[1]} on {record[2]}")
        
        # First, delete any audit history records
        cursor.execute("DELETE FROM slaughter_records_edit_history WHERE record_id = %s", (record_id,))
        audit_deleted = cursor.rowcount
        print(f"Deleted {audit_deleted} audit history records")
        
        # Try to delete the main record with explicit transaction handling
        try:
            # Disable foreign key checks temporarily
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute("DELETE FROM slaughter_records WHERE id = %s", (record_id,))
            rows_affected = cursor.rowcount
            print(f"Delete query affected {rows_affected} rows")
            # Re-enable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        except Exception as delete_error:
            print(f"Delete error: {delete_error}")
            # Re-enable foreign key checks in case of error
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            raise delete_error
        
        # Check if any rows were affected
        if rows_affected == 0:
            print(f"No rows were affected by delete operation for record {record_id}")
            return jsonify({'success': False, 'message': 'No record was deleted. Record may not exist.'})
        
        # Log activity
        log_activity(session['employee_id'], 'SLAUGHTER_RECORD_DELETE', 
                   f'Slaughter record {record_id} deleted: {record[0]}kg to {record[1]} on {record[2]}')
        
        conn.commit()
        print(f"Slaughter record {record_id} deleted successfully")
        return jsonify({
            'success': True,
            'message': 'Slaughter record deleted successfully'
        })
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error deleting slaughter record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to delete slaughter record: {str(e)}'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/slaughter/record/<int:record_id>/audit', methods=['GET'])
def get_slaughter_record_audit(record_id):
    """Get audit history for a slaughter record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get audit history
        cursor.execute("""
            SELECT 
                sreh.field_name,
                sreh.old_value,
                sreh.new_value,
                sreh.edited_at,
                COALESCE(e.full_name, 'Unknown User') as edited_by_name
            FROM slaughter_records_edit_history sreh
            LEFT JOIN employees e ON sreh.edited_by = e.id
            WHERE sreh.record_id = %s
            ORDER BY sreh.edited_at DESC
        """, (record_id,))
        
        audit_records = cursor.fetchall()
        
        # Convert datetime to string for JSON serialization
        for record in audit_records:
            if record['edited_at']:
                record['edited_at'] = record['edited_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'success': True,
            'audit_records': audit_records
        })
        
    except Exception as e:
        print(f"Error fetching slaughter record audit history: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to fetch audit history: {str(e)}'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/slaughter/statistics', methods=['GET'])
def get_slaughter_statistics():
    """Get slaughter statistics"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_slaughtered,
                SUM(pigs_count) as total_pigs,
                SUM(carcass_weight * pigs_count) as total_meat_kg,
                SUM(total_revenue) as total_revenue,
                COUNT(CASE WHEN slaughter_date >= DATE_SUB(CURDATE(), INTERVAL DAY(CURDATE())-1 DAY) THEN 1 END) as this_month_count
            FROM slaughter_records
        """)
        stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'statistics': {
                'total_slaughtered': stats['total_slaughtered'] or 0,
                'total_pigs': stats['total_pigs'] or 0,
                'total_meat_kg': float(stats['total_meat_kg'] or 0),
                'total_revenue': float(stats['total_revenue'] or 0),
                'this_month_count': stats['this_month_count'] or 0
            }
        })
        
    except Exception as e:
        print(f"Error fetching slaughter statistics: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/death/record', methods=['POST'])
def create_death_record():
    """Create a death record"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        print(f"Death record data received: {data}")
        
        # Validate required fields
        required_fields = ['pig_type', 'death_date', 'cause_of_death', 'weight_at_death']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({'success': False, 'message': f'{field} is required'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create death record
        cursor.execute("""
            INSERT INTO dead_pigs (
                pig_id, litter_id, pig_type, death_date, cause_of_death, 
                weight_at_death, age_at_death, additional_details, 
                pigs_count, created_by, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
            )
        """, (
            data.get('pig_id'),
            data.get('litter_id'),
            data['pig_type'],
            data['death_date'],
            data['cause_of_death'],
            data['weight_at_death'],
            data.get('age_at_death'),
            data.get('additional_details', ''),
            data.get('pigs_count', 1),
            session['employee_id']
        ))
        
        record_id = cursor.lastrowid
        
        # Update pig/litter status
        if data['pig_type'] == 'grown_pig' and data.get('pig_id'):
            # Update pig status to dead
            print(f"Updating pig {data['pig_id']} status to 'dead'")
            cursor.execute("""
                UPDATE pigs 
                SET status = 'dead', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (data['pig_id'],))
            print(f"Pig status update affected {cursor.rowcount} rows")
            
            # Verify the status update
            cursor.execute("SELECT status FROM pigs WHERE id = %s", (data['pig_id'],))
            updated_pig = cursor.fetchone()
            print(f"Pig {data['pig_id']} status after update: {updated_pig['status'] if updated_pig else 'NOT FOUND'}")
            
        elif data['pig_type'] == 'litter' and data.get('litter_id'):
            # For litters, reduce the number of available pigs
            pigs_to_remove = int(data.get('pigs_count', 1))
            
            # Update litter to reduce available pigs
            cursor.execute("""
                UPDATE litters 
                SET alive_piglets = alive_piglets - %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND alive_piglets >= %s
            """, (pigs_to_remove, data['litter_id'], pigs_to_remove))
            
            # Check if update was successful
            if cursor.rowcount == 0:
                raise Exception("Not enough pigs available in this litter for death record")
            
            # Get updated litter info
            cursor.execute("""
                SELECT alive_piglets, total_piglets FROM litters WHERE id = %s
            """, (data['litter_id'],))
            litter_info = cursor.fetchone()
            
            # If no pigs left, update litter status to dead
            if litter_info and litter_info['alive_piglets'] <= 0:
                print(f"Updating litter {data['litter_id']} status to 'dead'")
                cursor.execute("""
                    UPDATE litters 
                    SET status = 'dead', updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (data['litter_id'],))
                print(f"Litter status update affected {cursor.rowcount} rows")
                
                # Verify the litter status update
                cursor.execute("SELECT status, alive_piglets FROM litters WHERE id = %s", (data['litter_id'],))
                updated_litter = cursor.fetchone()
                print(f"Litter {data['litter_id']} status after update: {updated_litter['status'] if updated_litter else 'NOT FOUND'}, alive_piglets: {updated_litter['alive_piglets'] if updated_litter else 'N/A'}")
                
                # Check if all litters from the parent sow are dead
                cursor.execute("""
                    SELECT sow_id FROM litters WHERE id = %s
                """, (data['litter_id'],))
                sow_result = cursor.fetchone()
                
                if sow_result:
                    sow_id = sow_result['sow_id']
                    cursor.execute("""
                        SELECT COUNT(*) as total_litters,
                               SUM(CASE WHEN status = 'dead' THEN 1 ELSE 0 END) as dead_litters
                        FROM litters 
                        WHERE sow_id = %s
                    """, (sow_id,))
                    
                    litter_stats = cursor.fetchone()
                    if litter_stats and litter_stats['total_litters'] == litter_stats['dead_litters']:
                        # All litters are dead, update sow status
                        cursor.execute("""
                            UPDATE pigs 
                            SET status = 'dead', updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (sow_id,))
        
        # Log activity
        if data['pig_type'] == 'grown_pig':
            log_activity(session['employee_id'], 'DEATH_RECORD', 
                       f'Death record created: Pig {data.get("pig_id")} - Status changed to dead - Cause: {data["cause_of_death"]}')
        else:
            pigs_count = int(data.get('pigs_count', 1))
            log_activity(session['employee_id'], 'DEATH_RECORD', 
                       f'Death record created: Litter {data.get("litter_id")} - {pigs_count} pigs marked as dead - Cause: {data["cause_of_death"]}')
        
        conn.commit()
        
        # Final verification after commit
        if data['pig_type'] == 'grown_pig' and data.get('pig_id'):
            cursor.execute("SELECT status FROM pigs WHERE id = %s", (data['pig_id'],))
            final_pig = cursor.fetchone()
            print(f"FINAL: Pig {data['pig_id']} status after commit: {final_pig['status'] if final_pig else 'NOT FOUND'}")
        elif data['pig_type'] == 'litter' and data.get('litter_id'):
            cursor.execute("SELECT status, alive_piglets FROM litters WHERE id = %s", (data['litter_id'],))
            final_litter = cursor.fetchone()
            print(f"FINAL: Litter {data['litter_id']} status after commit: {final_litter['status'] if final_litter else 'NOT FOUND'}, alive_piglets: {final_litter['alive_piglets'] if final_litter else 'N/A'}")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Death record created successfully',
            'record_id': record_id
        })
        
    except Exception as e:
        print(f"Error creating death record: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/death/record/<int:record_id>/edit', methods=['PUT'])
def edit_death_record(record_id):
    """Edit a death record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['death_date', 'cause_of_death', 'weight_at_death']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get original data for comparison
        cursor.execute("""
            SELECT death_date, cause_of_death, weight_at_death, age_at_death, additional_details
            FROM dead_pigs WHERE id = %s
        """, (record_id,))
        original_data = cursor.fetchone()
        
        if not original_data:
            return jsonify({'success': False, 'message': 'Death record not found'}), 404
        
        # Track changes for audit trail
        changes = []
        
        # Compare each field and track changes
        if str(original_data['death_date']) != str(data.get('death_date')):
            changes.append(('death_date', str(original_data['death_date']), str(data.get('death_date'))))
        if str(original_data['cause_of_death'] or '') != str(data.get('cause_of_death') or ''):
            changes.append(('cause_of_death', str(original_data['cause_of_death'] or ''), str(data.get('cause_of_death') or '')))
        if str(original_data['weight_at_death']) != str(data.get('weight_at_death')):
            changes.append(('weight_at_death', str(original_data['weight_at_death']), str(data.get('weight_at_death'))))
        if str(original_data['age_at_death'] or '') != str(data.get('age_at_death') or ''):
            changes.append(('age_at_death', str(original_data['age_at_death'] or ''), str(data.get('age_at_death') or '')))
        if str(original_data['additional_details'] or '') != str(data.get('additional_details') or ''):
            changes.append(('additional_details', str(original_data['additional_details'] or ''), str(data.get('additional_details') or '')))
        
        # Update death record
        cursor.execute("""
            UPDATE dead_pigs SET 
                death_date = %s, cause_of_death = %s, weight_at_death = %s,
                age_at_death = %s, additional_details = %s
            WHERE id = %s
        """, (
            data['death_date'], data['cause_of_death'], data['weight_at_death'],
            data.get('age_at_death'), data.get('additional_details'), record_id
        ))
        
        # Insert audit records for each changed field
        for field_name, old_value, new_value in changes:
            cursor.execute("""
                INSERT INTO death_records_edit_history 
                (record_id, field_name, old_value, new_value, edited_by)
                VALUES (%s, %s, %s, %s, %s)
            """, (record_id, field_name, old_value, new_value, session['employee_id']))
        
        # Log activity
        log_activity(session['employee_id'], 'DEATH_RECORD_EDIT', 
                   f'Death record {record_id} updated: {data["weight_at_death"]}kg weight')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Death record updated successfully'
        })
        
    except Exception as e:
        print(f"Error updating death record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to update death record: {str(e)}'})

@app.route('/api/death/record/<int:record_id>/delete', methods=['DELETE'])
def delete_death_record(record_id):
    """Delete a death record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, let's see what death records exist
        cursor.execute("SELECT id, weight_at_death, cause_of_death, death_date FROM dead_pigs ORDER BY id")
        all_records = cursor.fetchall()
        print(f"All death records in database: {all_records}")
        
        # Check if record exists and get details for logging
        cursor.execute("""
            SELECT weight_at_death, cause_of_death, death_date 
            FROM dead_pigs WHERE id = %s
        """, (record_id,))
        record = cursor.fetchone()
        
        if not record:
            print(f"Death record {record_id} not found")
            return jsonify({'success': False, 'message': 'Death record not found'}), 404
        
        print(f"Found death record {record_id}: {record[0]}kg - {record[1]} on {record[2]}")
        
        # First, delete any audit history records
        cursor.execute("DELETE FROM death_records_edit_history WHERE record_id = %s", (record_id,))
        audit_deleted = cursor.rowcount
        print(f"Deleted {audit_deleted} audit history records")
        
        # Try to delete the main record with explicit transaction handling
        try:
            # Check if record still exists before deletion
            cursor.execute("SELECT COUNT(*) FROM dead_pigs WHERE id = %s", (record_id,))
            count_before = cursor.fetchone()[0]
            print(f"Records with ID {record_id} before deletion: {count_before}")
            
            # Disable foreign key checks temporarily
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            print("Foreign key checks disabled")
            
            # Try the delete operation
            cursor.execute("DELETE FROM dead_pigs WHERE id = %s", (record_id,))
            rows_affected = cursor.rowcount
            print(f"Delete query affected {rows_affected} rows")
            
            # Check if record still exists after deletion
            cursor.execute("SELECT COUNT(*) FROM dead_pigs WHERE id = %s", (record_id,))
            count_after = cursor.fetchone()[0]
            print(f"Records with ID {record_id} after deletion: {count_after}")
            
            # Re-enable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            print("Foreign key checks re-enabled")
            
        except Exception as delete_error:
            print(f"Delete error: {delete_error}")
            # Re-enable foreign key checks in case of error
            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            except:
                pass
            raise delete_error
        
        # Check if any rows were affected
        if rows_affected == 0:
            print(f"No rows were affected by delete operation for record {record_id}")
            # Try a simple test deletion
            print("Testing if we can delete any record...")
            cursor.execute("SELECT id FROM dead_pigs LIMIT 1")
            test_record = cursor.fetchone()
            if test_record:
                test_id = test_record[0]
                print(f"Trying to delete test record {test_id}")
                cursor.execute("DELETE FROM dead_pigs WHERE id = %s", (test_id,))
                test_rows = cursor.rowcount
                print(f"Test deletion affected {test_rows} rows")
                if test_rows > 0:
                    print("Test deletion worked - there might be a specific issue with record", record_id)
                else:
                    print("Test deletion also failed - there's a general deletion issue")
            return jsonify({'success': False, 'message': 'No record was deleted. Record may not exist.'})
        
        # Log activity
        log_activity(session['employee_id'], 'DEATH_RECORD_DELETE', 
                   f'Death record {record_id} deleted: {record[0]}kg - {record[1]} on {record[2]}')
        
        conn.commit()
        print(f"Death record {record_id} deleted successfully")
        return jsonify({
            'success': True,
            'message': 'Death record deleted successfully'
        })
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error deleting death record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to delete death record: {str(e)}'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/death/record/<int:record_id>/audit', methods=['GET'])
def get_death_record_audit(record_id):
    """Get audit history for a death record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get audit history
        cursor.execute("""
            SELECT 
                dreh.field_name,
                dreh.old_value,
                dreh.new_value,
                dreh.edited_at,
                COALESCE(e.full_name, 'Unknown User') as edited_by_name
            FROM death_records_edit_history dreh
            LEFT JOIN employees e ON dreh.edited_by = e.id
            WHERE dreh.record_id = %s
            ORDER BY dreh.edited_at DESC
        """, (record_id,))
        
        audit_records = cursor.fetchall()
        
        # Convert datetime to string for JSON serialization
        for record in audit_records:
            if record['edited_at']:
                record['edited_at'] = record['edited_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'success': True,
            'audit_records': audit_records
        })
        
    except Exception as e:
        print(f"Error fetching death record audit history: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to fetch audit history: {str(e)}'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/pigs/available', methods=['GET'])
def get_available_pigs():
    """Get available pigs and litters for slaughter"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        pig_type = request.args.get('type', 'grown_pig')
        
        if pig_type == 'grown_pig':
            # Get available grown pigs with latest weight
            cursor.execute("""
                SELECT p.id, p.tag_id, p.breed, p.gender, p.status, p.created_at,
                       COALESCE(w.weight, 0) as weight
                FROM pigs p 
                LEFT JOIN (
                    SELECT animal_id, weight,
                           ROW_NUMBER() OVER (PARTITION BY animal_id ORDER BY weighing_date DESC, created_at DESC) as rn
                    FROM weight_records 
                    WHERE animal_id IS NOT NULL
                ) w ON p.id = w.animal_id AND w.rn = 1
                WHERE p.status = 'active' 
                AND p.pig_type = 'grown_pig'
                ORDER BY p.tag_id ASC
            """)
            pigs = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return jsonify({
                'success': True,
                'pigs': pigs
            })
            
        elif pig_type == 'litter':
            # Get available litters with sow breed information
            cursor.execute("""
                SELECT l.id, l.litter_id, l.total_piglets as total_pigs, l.alive_piglets as available_pigs, 
                       l.avg_weight, l.status, l.created_at, l.farrowing_date,
                       p.breed, p.tag_id as sow_tag_id
                FROM litters l
                LEFT JOIN pigs p ON l.sow_id = p.id
                WHERE l.status IN ('unweaned', 'weaned') 
                AND l.alive_piglets > 0
                ORDER BY l.litter_id ASC
            """)
            litters = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return jsonify({
                'success': True,
                'litters': litters
            })
        else:
            return jsonify({'success': False, 'message': 'Invalid type parameter'}), 400
            
    except Exception as e:
        print(f"Error fetching available pigs/litters: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/litters/available', methods=['GET'])
def get_available_litters():
    """Get available litters for death records"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get available litters with sow breed information
        cursor.execute("""
            SELECT l.id, l.litter_id, l.total_piglets as total_pigs, l.alive_piglets as available_pigs, 
                   l.avg_weight, l.status, l.created_at, l.farrowing_date,
                   p.breed, p.tag_id as sow_tag_id
            FROM litters l
            LEFT JOIN pigs p ON l.sow_id = p.id
            WHERE l.status IN ('unweaned', 'weaned') 
            AND l.alive_piglets > 0
            ORDER BY l.litter_id ASC
        """)
        litters = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'litters': litters
        })
        
    except Exception as e:
        print(f"Error fetching available litters: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/pigs/death-details/<int:pig_id>', methods=['GET'])
def get_pig_death_details(pig_id):
    """Get pig details for death record age calculation"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, tag_id, birth_date, breed, gender, status
            FROM pigs 
            WHERE id = %s
        """, (pig_id,))
        pig = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if pig:
            return jsonify({
                'success': True,
                'pig': pig
            })
        else:
            return jsonify({'success': False, 'message': 'Pig not found'}), 404
        
    except Exception as e:
        print(f"Error fetching pig details: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/litters/death-details/<int:litter_id>', methods=['GET'])
def get_litter_death_details(litter_id):
    """Get litter details for death record age calculation"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, litter_id, farrowing_date, total_piglets, alive_piglets, status
            FROM litters 
            WHERE id = %s
        """, (litter_id,))
        litter = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if litter:
            return jsonify({
                'success': True,
                'litter': litter
            })
        else:
            return jsonify({'success': False, 'message': 'Litter not found'}), 404
        
    except Exception as e:
        print(f"Error fetching litter details: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/sale/record', methods=['POST'])
def create_sale_record():
    """Create a sale record"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        print(f"Sale record data received: {data}")
        
        # Validate required fields
        required_fields = ['pig_type', 'sale_date', 'buyer_name', 'sale_price']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({'success': False, 'message': f'{field} is required'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create sale record
        cursor.execute("""
            INSERT INTO sale_records (
                pig_id, litter_id, pig_type, sale_date, buyer_name, buyer_contact,
                sale_price, total_revenue, notes, 
                pigs_count, created_by, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
            )
        """, (
            data.get('pig_id'),
            data.get('litter_id'),
            data['pig_type'],
            data['sale_date'],
            data['buyer_name'],
            data.get('buyer_contact', ''),
            data['sale_price'],
            data['total_revenue'],
            data.get('notes', ''),
            data.get('pigs_count', 1),
            session['employee_id']
        ))
        
        record_id = cursor.lastrowid
        
        # Update pig/litter status
        if data['pig_type'] == 'grown_pig' and data.get('pig_id'):
            # Update pig status to sold
            print(f"Updating pig {data['pig_id']} status to 'sold'")
            cursor.execute("""
                UPDATE pigs 
                SET status = 'sold', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (data['pig_id'],))
            print(f"Pig status update affected {cursor.rowcount} rows")
            
            # Verify the status update
            cursor.execute("SELECT status FROM pigs WHERE id = %s", (data['pig_id'],))
            updated_pig = cursor.fetchone()
            print(f"Pig {data['pig_id']} status after update: {updated_pig['status'] if updated_pig else 'NOT FOUND'}")
            
        elif data['pig_type'] == 'litter' and data.get('litter_id'):
            # For litters, reduce the number of available pigs
            pigs_to_sell = int(data.get('pigs_count', 1))
            
            # Update litter to reduce available pigs
            cursor.execute("""
                UPDATE litters 
                SET alive_piglets = alive_piglets - %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND alive_piglets >= %s
            """, (pigs_to_sell, data['litter_id'], pigs_to_sell))
            
            # Check if update was successful
            if cursor.rowcount == 0:
                raise Exception("Not enough pigs available in this litter for sale record")
            
            # Get updated litter info
            cursor.execute("""
                SELECT alive_piglets, total_piglets FROM litters WHERE id = %s
            """, (data['litter_id'],))
            litter_info = cursor.fetchone()
            
            # If no pigs left, update litter status to sold
            if litter_info and litter_info['alive_piglets'] <= 0:
                print(f"Updating litter {data['litter_id']} status to 'sold'")
                cursor.execute("""
                    UPDATE litters 
                    SET status = 'sold', updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (data['litter_id'],))
                print(f"Litter status update affected {cursor.rowcount} rows")
                
                # Verify the litter status update
                cursor.execute("SELECT status, alive_piglets FROM litters WHERE id = %s", (data['litter_id'],))
                updated_litter = cursor.fetchone()
                print(f"Litter {data['litter_id']} status after update: {updated_litter['status'] if updated_litter else 'NOT FOUND'}, alive_piglets: {updated_litter['alive_piglets'] if updated_litter else 'N/A'}")
        
        # Log activity
        if data['pig_type'] == 'grown_pig':
            log_activity(session['employee_id'], 'SALE_RECORD', 
                       f'Sale record created: Pig {data.get("pig_id")} - Status changed to sold - Revenue: ${data["total_revenue"]}')
        else:
            pigs_count = int(data.get('pigs_count', 1))
            log_activity(session['employee_id'], 'SALE_RECORD', 
                       f'Sale record created: Litter {data.get("litter_id")} - {pigs_count} pigs sold - Revenue: ${data["total_revenue"]}')
        
        conn.commit()
        
        # Final verification after commit
        if data['pig_type'] == 'grown_pig' and data.get('pig_id'):
            cursor.execute("SELECT status FROM pigs WHERE id = %s", (data['pig_id'],))
            final_pig = cursor.fetchone()
            print(f"FINAL: Pig {data['pig_id']} status after commit: {final_pig['status'] if final_pig else 'NOT FOUND'}")
        elif data['pig_type'] == 'litter' and data.get('litter_id'):
            cursor.execute("SELECT status, alive_piglets FROM litters WHERE id = %s", (data['litter_id'],))
            final_litter = cursor.fetchone()
            print(f"FINAL: Litter {data['litter_id']} status after commit: {final_litter['status'] if final_litter else 'NOT FOUND'}, alive_piglets: {final_litter['alive_piglets'] if final_litter else 'N/A'}")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Sale record created successfully',
            'record_id': record_id
        })
        
    except Exception as e:
        print(f"Error creating sale record: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/sale/record/<int:record_id>/edit', methods=['PUT'])
def edit_sale_record(record_id):
    """Edit a sale record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['sale_date', 'buyer_name', 'sale_price', 'total_revenue']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get original data for comparison
        cursor.execute("""
            SELECT sale_date, buyer_name, buyer_contact, sale_price, total_revenue, notes
            FROM sale_records WHERE id = %s
        """, (record_id,))
        original_data = cursor.fetchone()
        
        if not original_data:
            return jsonify({'success': False, 'message': 'Sale record not found'}), 404
        
        # Track changes for audit trail
        changes = []
        
        # Compare each field and track changes
        if str(original_data['sale_date']) != str(data.get('sale_date')):
            changes.append(('sale_date', str(original_data['sale_date']), str(data.get('sale_date'))))
        if str(original_data['buyer_name'] or '') != str(data.get('buyer_name') or ''):
            changes.append(('buyer_name', str(original_data['buyer_name'] or ''), str(data.get('buyer_name') or '')))
        if str(original_data['buyer_contact'] or '') != str(data.get('buyer_contact') or ''):
            changes.append(('buyer_contact', str(original_data['buyer_contact'] or ''), str(data.get('buyer_contact') or '')))
        if str(original_data['sale_price']) != str(data.get('sale_price')):
            changes.append(('sale_price', str(original_data['sale_price']), str(data.get('sale_price'))))
        if str(original_data['total_revenue']) != str(data.get('total_revenue')):
            changes.append(('total_revenue', str(original_data['total_revenue']), str(data.get('total_revenue'))))
        if str(original_data['notes'] or '') != str(data.get('notes') or ''):
            changes.append(('notes', str(original_data['notes'] or ''), str(data.get('notes') or '')))
        
        # Update sale record
        cursor.execute("""
            UPDATE sale_records SET 
                sale_date = %s, buyer_name = %s, buyer_contact = %s,
                sale_price = %s, total_revenue = %s, notes = %s
            WHERE id = %s
        """, (
            data['sale_date'], data.get('buyer_name'), data.get('buyer_contact'),
            data['sale_price'], data['total_revenue'], data.get('notes'), record_id
        ))
        
        # Insert audit records for each changed field
        for field_name, old_value, new_value in changes:
            cursor.execute("""
                INSERT INTO sale_records_edit_history 
                (record_id, field_name, old_value, new_value, edited_by)
                VALUES (%s, %s, %s, %s, %s)
            """, (record_id, field_name, old_value, new_value, session['employee_id']))
        
        # Log activity
        log_activity(session['employee_id'], 'SALE_RECORD_EDIT', 
                   f'Sale record {record_id} updated: ${data["total_revenue"]} revenue')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Sale record updated successfully'
        })
        
    except Exception as e:
        print(f"Error updating sale record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to update sale record: {str(e)}'})

@app.route('/api/sale/record/<int:record_id>/delete', methods=['DELETE'])
def delete_sale_record(record_id):
    """Delete a sale record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if record exists and get details for logging
        cursor.execute("""
            SELECT total_revenue, buyer_name, sale_date 
            FROM sale_records WHERE id = %s
        """, (record_id,))
        record = cursor.fetchone()
        
        if not record:
            return jsonify({'success': False, 'message': 'Sale record not found'}), 404
        
        # Delete the record
        cursor.execute("DELETE FROM sale_records WHERE id = %s", (record_id,))
        
        # Check if any rows were affected
        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'No record was deleted. Record may not exist.'})
        
        # Log activity
        log_activity(session['employee_id'], 'SALE_RECORD_DELETE', 
                   f'Sale record {record_id} deleted: ${record[0]} to {record[1]} on {record[2]}')
        
        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Sale record deleted successfully'
        })
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error deleting sale record: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to delete sale record: {str(e)}'})
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/sale/record/<int:record_id>/audit', methods=['GET'])
def get_sale_record_audit(record_id):
    """Get audit history for a sale record"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get audit history
        cursor.execute("""
            SELECT 
                sreh.field_name,
                sreh.old_value,
                sreh.new_value,
                sreh.edited_at,
                COALESCE(e.full_name, 'Unknown User') as edited_by_name
            FROM sale_records_edit_history sreh
            LEFT JOIN employees e ON sreh.edited_by = e.id
            WHERE sreh.record_id = %s
            ORDER BY sreh.edited_at DESC
        """, (record_id,))
        
        audit_records = cursor.fetchall()
        
        # Convert datetime to string for JSON serialization
        for record in audit_records:
            if record['edited_at']:
                record['edited_at'] = record['edited_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'success': True,
            'audit_records': audit_records
        })
        
    except Exception as e:
        print(f"Error fetching sale record audit history: {str(e)}")
        return jsonify({'success': False, 'message': f'Failed to fetch audit history: {str(e)}'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/death/records', methods=['GET'])
def get_death_records():
    """Get all death records"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all death records with pig/litter and employee information
        cursor.execute("""
            SELECT dp.*, 
                   p.tag_id as pig_tag_id, p.breed as pig_breed, p.gender as pig_gender,
                   l.litter_id,
                   sow.breed as litter_breed,
                   e.full_name as created_by_name,
                   CASE 
                       WHEN EXISTS(SELECT 1 FROM death_records_edit_history dreh WHERE dreh.record_id = dp.id) 
                       THEN 1 
                       ELSE 0 
                   END as is_edited
            FROM dead_pigs dp
            LEFT JOIN pigs p ON dp.pig_id = p.id
            LEFT JOIN litters l ON dp.litter_id = l.id
            LEFT JOIN pigs sow ON l.sow_id = sow.id
            LEFT JOIN employees e ON dp.created_by = e.id
            ORDER BY dp.death_date DESC, dp.created_at DESC
        """)
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': records
        })
        
    except Exception as e:
        print(f"Error fetching death records: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/sale/records', methods=['GET'])
def get_sale_records():
    """Get all sale records"""
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all sale records with pig/litter and employee information
        cursor.execute("""
            SELECT sr.*, 
                   p.tag_id as pig_tag_id, p.breed as pig_breed, p.gender as pig_gender,
                   l.litter_id,
                   sow.breed as litter_breed,
                   e.full_name as created_by_name,
                   CASE 
                       WHEN EXISTS(SELECT 1 FROM sale_records_edit_history sreh WHERE sreh.record_id = sr.id) 
                       THEN 1 
                       ELSE 0 
                   END as is_edited
            FROM sale_records sr
            LEFT JOIN pigs p ON sr.pig_id = p.id
            LEFT JOIN litters l ON sr.litter_id = l.id
            LEFT JOIN pigs sow ON l.sow_id = sow.id
            LEFT JOIN employees e ON sr.created_by = e.id
            ORDER BY sr.sale_date DESC, sr.created_at DESC
        """)
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': records
        })
        
    except Exception as e:
        print(f"Error fetching sale records: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Vaccination Schedule API endpoints
@app.route('/api/vaccination/schedule', methods=['GET'])
def get_vaccination_schedule():
    """Get all vaccination schedule entries"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, day_number, day_description, reason, medicine_activity, 
                   dosage_amount, interval_duration, additional_notes, 
                   medicine_image, animal_image, created_at, updated_at
            FROM vaccination_schedule 
            ORDER BY day_number ASC
        """)
        
        schedule_entries = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'schedule': schedule_entries
        })
        
    except Exception as e:
        print(f"Error getting vaccination schedule: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/vaccination/schedule/<int:schedule_id>', methods=['GET'])
def get_individual_vaccination_schedule(schedule_id):
    """Get individual vaccination schedule entry"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, day_number, day_description, reason, medicine_activity, 
                   dosage_amount, interval_duration, additional_notes, 
                   medicine_image, animal_image, created_at, updated_at
            FROM vaccination_schedule 
            WHERE id = %s
        """, (schedule_id,))
        
        schedule_entry = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not schedule_entry:
            return jsonify({'success': False, 'error': 'Schedule entry not found'}), 404
        
        return jsonify({
            'success': True,
            'schedule': schedule_entry
        })
        
    except Exception as e:
        print(f"Error getting vaccination schedule entry: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/vaccination/schedule', methods=['POST'])
def add_vaccination_schedule():
    """Add new vaccination schedule entry"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        # Handle both JSON and FormData
        if request.content_type == 'application/json':
            data = request.get_json()
        else:
            # Handle FormData
            data = {
                'day_number': request.form.get('day_number'),
                'day_description': request.form.get('day_description', ''),
                'reason': request.form.get('reason'),
                'medicine_activity': request.form.get('medicine_activity'),
                'dosage_amount': request.form.get('dosage_amount', ''),
                'interval_duration': request.form.get('interval_duration', ''),
                'additional_notes': request.form.get('additional_notes', ''),
                'medicine_image': request.form.get('medicine_image', ''),
                'animal_image': request.form.get('animal_image', '')
            }
        
        # Validate required fields
        required_fields = ['day_number', 'reason', 'medicine_activity']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if day number already exists
        cursor.execute("SELECT id FROM vaccination_schedule WHERE day_number = %s", (data['day_number'],))
        if cursor.fetchone():
            return jsonify({'success': False, 'error': 'Day number already exists'}), 400
        
        # Insert new vaccination schedule entry
        cursor.execute("""
            INSERT INTO vaccination_schedule (day_number, day_description, reason, medicine_activity, 
                                            dosage_amount, interval_duration, additional_notes, 
                                            medicine_image, animal_image, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['day_number'],
            data.get('day_description', ''),
            data['reason'],
            data['medicine_activity'],
            data.get('dosage_amount', ''),
            data.get('interval_duration', ''),
            data.get('additional_notes', ''),
            data.get('medicine_image', ''),
            data.get('animal_image', ''),
            session['employee_id']
        ))
        
        # Log activity
        log_activity(session['employee_id'], 'CREATE', 
                    f'Added vaccination schedule for day {data["day_number"]}', 
                    'vaccination_schedule', cursor.lastrowid)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Vaccination schedule entry added successfully'
        })
        
    except Exception as e:
        print(f"Error adding vaccination schedule: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/vaccination/schedule/<int:schedule_id>', methods=['PUT'])
def update_vaccination_schedule(schedule_id):
    """Update vaccination schedule entry"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        # Handle both JSON and FormData
        if request.content_type == 'application/json':
            data = request.get_json()
        else:
            # Handle FormData
            data = {
                'day_number': request.form.get('day_number'),
                'day_description': request.form.get('day_description', ''),
                'reason': request.form.get('reason'),
                'medicine_activity': request.form.get('medicine_activity'),
                'dosage_amount': request.form.get('dosage_amount', ''),
                'interval_duration': request.form.get('interval_duration', ''),
                'additional_notes': request.form.get('additional_notes', ''),
                'medicine_image': request.form.get('medicine_image', ''),
                'animal_image': request.form.get('animal_image', '')
            }
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if schedule entry exists
        cursor.execute("SELECT id FROM vaccination_schedule WHERE id = %s", (schedule_id,))
        if not cursor.fetchone():
            return jsonify({'success': False, 'error': 'Schedule entry not found'}), 404
        
        # Update vaccination schedule entry
        cursor.execute("""
            UPDATE vaccination_schedule 
            SET day_number = %s, day_description = %s, reason = %s, medicine_activity = %s,
                dosage_amount = %s, interval_duration = %s, additional_notes = %s,
                medicine_image = %s, animal_image = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (
            data.get('day_number'),
            data.get('day_description', ''),
            data.get('reason'),
            data.get('medicine_activity'),
            data.get('dosage_amount', ''),
            data.get('interval_duration', ''),
            data.get('additional_notes', ''),
            data.get('medicine_image', ''),
            data.get('animal_image', ''),
            schedule_id
        ))
        
        # Log activity
        log_activity(session['employee_id'], 'UPDATE', 
                    f'Updated vaccination schedule for day {data.get("day_number")}', 
                    'vaccination_schedule', schedule_id)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Vaccination schedule entry updated successfully'
        })
        
    except Exception as e:
        print(f"Error updating vaccination schedule: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/vaccination/schedule/<int:schedule_id>', methods=['DELETE'])
def delete_vaccination_schedule(schedule_id):
    """Delete vaccination schedule entry"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if schedule entry exists
        cursor.execute("SELECT day_number FROM vaccination_schedule WHERE id = %s", (schedule_id,))
        entry = cursor.fetchone()
        if not entry:
            return jsonify({'success': False, 'error': 'Schedule entry not found'}), 404
        
        # Delete vaccination schedule entry
        cursor.execute("DELETE FROM vaccination_schedule WHERE id = %s", (schedule_id,))
        
        # Log activity
        log_activity(session['employee_id'], 'DELETE', 
                    f'Deleted vaccination schedule for day {entry["day_number"]}', 
                    'vaccination_schedule', schedule_id)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Vaccination schedule entry deleted successfully'
        })
        
    except Exception as e:
        print(f"Error deleting vaccination schedule: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Vaccination Records API endpoints
@app.route('/api/vaccination/complete', methods=['POST'])
def mark_vaccination_completed():
    """Mark a vaccination as completed for an animal"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        data = request.get_json()
        print(f"Received vaccination completion data: {data}")
        
        # Validate required fields
        required_fields = ['animal_id', 'animal_type', 'schedule_id', 'completion_notes']
        for field in required_fields:
            if field not in data:
                print(f"Missing required field: {field}")
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if vaccination is already completed
        cursor.execute("""
            SELECT id FROM vaccination_records 
            WHERE animal_id = %s AND animal_type = %s AND schedule_id = %s
        """, (data['animal_id'], data['animal_type'], data['schedule_id']))
        
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Vaccination already completed'}), 400
        
        # Insert vaccination completion record
        print(f"Inserting vaccination record: animal_id={data['animal_id']}, animal_type={data['animal_type']}, schedule_id={data['schedule_id']}")
        cursor.execute("""
            INSERT INTO vaccination_records (animal_id, animal_type, schedule_id, completed_date, completion_notes, completed_by)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            int(data['animal_id']),
            data['animal_type'],
            int(data['schedule_id']),
            datetime.now().date(),
            data['completion_notes'],
            session['employee_id']
        ))
        
        # Log the activity
        log_activity(
            session['employee_id'],
            'Marked vaccination as completed',
            f'Marked vaccination as completed for {data["animal_type"]} ID {data["animal_id"]}',
            'vaccination_records', 
            cursor.lastrowid
        )
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Vaccination marked as completed successfully'
        })
        
    except Exception as e:
        print(f"Error marking vaccination as completed: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/vaccination/records/<int:animal_id>/<animal_type>', methods=['GET'])
def get_vaccination_records(animal_id, animal_type):
    """Get vaccination records for a specific animal"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get vaccination records for the animal
        cursor.execute("""
            SELECT vr.*, vs.day_number, vs.day_description, vs.medicine_activity, vs.reason,
                   e.full_name as completed_by_name
            FROM vaccination_records vr
            JOIN vaccination_schedule vs ON vr.schedule_id = vs.id
            JOIN employees e ON vr.completed_by = e.id
            WHERE vr.animal_id = %s AND vr.animal_type = %s
            ORDER BY vr.completed_date DESC
        """, (animal_id, animal_type))
        
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'records': records
        })
        
    except Exception as e:
        print(f"Error getting vaccination records: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/logout', methods=['POST'])
def api_logout():
    if 'employee_id' in session:
        # Log logout activity
        log_activity(session['employee_id'], 'LOGOUT', f'Employee {session["employee_name"]} logged out')
        session.clear()
    return {'success': True, 'redirect': url_for('landing')}

@app.route('/admin/farm/breeding-records')
def breeding_records_page():
    """Breeding Records page - Active breeding records only"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_breeding_records.html')

@app.route('/admin/farm/litters')
def admin_farm_litters():
    """Admin farm litters management page"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_litters.html')

@app.route('/admin/farm/failed-conceptions')
def failed_conceptions_page():
    """Failed Conceptions page - Failed breeding attempts only"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_failed_conceptions.html')

@app.route('/admin/farm/completed-farrowings')
def completed_farrowings_page():
    """Completed Farrowings page - Successful farrowings only"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_completed_farrowings.html')

@app.route('/admin/farm/family-tree')
def family_tree_page():
    """Family Tree page - Display all sows and boars separately"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_family_tree.html')

@app.route('/admin/farm/register-cows')
def register_cows_page():
    """Cow Registration page"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_register_cows.html')

@app.route('/admin/farm/cow-milk')
def cow_milk_page():
    """Cow Milk Management page"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_cow_milk.html')

@app.route('/admin/farm/milk-production-analytics')
def milk_production_analytics_page():
    """Milk Production Analytics page"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_milk_production_analytics.html')

@app.route('/admin/farm/cow/<int:cow_id>')
def cow_detail_page(cow_id):
    """Individual cow detail page"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_cow_detail.html', cow_id=cow_id)

@app.route('/admin/farm/animal-details/<int:animal_id>')
def animal_details_page(animal_id):
    """Animal details page for pigs and litters"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    # Get the type parameter from query string
    animal_type = request.args.get('type', 'grown_pig')
    
    return render_template('admin_farm_animal_details.html', 
                         animal_id=animal_id, 
                         animal_type=animal_type)

@app.route('/admin/farm/vaccination-analytics')
def vaccination_analytics_page():
    """Vaccination Analytics page"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_vaccination_analytics.html')

@app.route('/admin/farm/cow-breeding')
def cow_breeding_page():
    """Cow Breeding Management page"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_login'))
    
    if session.get('employee_role') != 'administrator':
        flash('Access denied. Administrator privileges required.', 'error')
        return redirect(url_for('admin_dashboard'))
    
    return render_template('admin_farm_cow_breeding.html')

@app.route('/admin/farm/chicken-weight-check')
def admin_farm_chicken_weight_check():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return redirect(url_for('employee_login'))
    
    # Get user data from session
    user_data = {
        'id': session['employee_id'],
        'name': session['employee_name'],
        'role': session['employee_role'],
        'status': session['employee_status'],
        'email': f"{session['employee_name'].lower().replace(' ', '.')}@farm.com"
    }
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all active chickens
        cursor.execute("""
            SELECT 
                chicken_id,
                batch_name,
                chicken_type,
                breed_name,
                gender,
                hatch_date,
                age_days,
                source,
                coop_number,
                quantity,
                current_status,
                registration_date
            FROM chickens 
            WHERE current_status = 'active'
            ORDER BY chicken_type, age_days
        """)
        chickens = cursor.fetchall()
        
        # Get all weight standards
        cursor.execute("""
            SELECT 
                id,
                category,
                age_days,
                expected_weight,
                description,
                created_at
            FROM chicken_weight_standards 
            ORDER BY category, age_days
        """)
        weight_standards = cursor.fetchall()
        
        # Create weight tracking table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chicken_weight_tracking (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chicken_id VARCHAR(50) NOT NULL,
                weight_standard_id INT NOT NULL,
                actual_weight DECIMAL(6,3) NOT NULL,
                expected_weight DECIMAL(6,3) NOT NULL,
                weight_percentage DECIMAL(5,2) NOT NULL,
                weight_category ENUM('healthy', 'underweight', 'overweight') NOT NULL,
                checked_by INT NOT NULL,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_chicken_id (chicken_id),
                INDEX idx_weight_standard_id (weight_standard_id),
                INDEX idx_checked_at (checked_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Get existing weight tracking records
        cursor.execute("""
            SELECT 
                cwt.chicken_id,
                cwt.weight_standard_id,
                cwt.actual_weight,
                cwt.expected_weight,
                cwt.weight_percentage,
                cwt.weight_category,
                cwt.checked_at
            FROM chicken_weight_tracking cwt
            ORDER BY cwt.checked_at DESC
        """)
        weight_tracking = cursor.fetchall()
        
        # Create a mapping of chicken_id + weight_standard_id to tracking record
        tracking_map = {}
        for record in weight_tracking:
            key = f"{record['chicken_id']}_{record['weight_standard_id']}"
            tracking_map[key] = record
        
        # Match chickens with their applicable weight standards (only incomplete ones)
        chickens_with_standards = []
        for chicken in chickens:
            chicken_standards = []
            for standard in weight_standards:
                if standard['category'] == chicken['chicken_type'] and chicken['age_days'] >= standard['age_days']:
                    # Check if this weight check has already been completed
                    key = f"{chicken['chicken_id']}_{standard['id']}"
                    tracking_record = tracking_map.get(key)
                    
                    # Only include incomplete weight checks
                    if tracking_record is None:
                        chicken_standards.append({
                            'standard': standard,
                            'completed': False,
                            'tracking_record': None
                        })
            
            if chicken_standards:  # Only include chickens that have incomplete weight standards
                chickens_with_standards.append({
                    'chicken': chicken,
                    'standards': chicken_standards
                })
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error fetching chicken weight check data: {str(e)}")
        chickens_with_standards = []
    
    return render_template('admin_farm_chicken_weight_check.html',
                         user=user_data,
                         chickens_with_standards=chickens_with_standards)

@app.route('/admin/farm/chicken-weight-submit', methods=['POST'])
def submit_chicken_weight():
    if 'employee_id' not in session or session.get('employee_role') != 'administrator':
        return jsonify({'success': False, 'message': 'Unauthorized access'})
    
    try:
        chicken_id = request.form.get('chicken_id')
        weight_standard_id = request.form.get('weight_standard_id')
        actual_weight = float(request.form.get('actual_weight'))
        
        if not chicken_id or not weight_standard_id or not actual_weight:
            return jsonify({'success': False, 'message': 'All fields are required'})
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the weight standard details
        cursor.execute("""
            SELECT expected_weight FROM chicken_weight_standards 
            WHERE id = %s
        """, (weight_standard_id,))
        
        standard = cursor.fetchone()
        if not standard:
            return jsonify({'success': False, 'message': 'Weight standard not found'})
        
        expected_weight = float(standard['expected_weight'])
        
        # Calculate weight percentage
        weight_percentage = (actual_weight / expected_weight) * 100
        
        # Determine weight category
        if weight_percentage >= 90 and weight_percentage <= 110:
            weight_category = 'healthy'
        elif weight_percentage < 90:
            weight_category = 'underweight'
        else:
            weight_category = 'overweight'
        
        # Insert weight tracking record
        cursor.execute("""
            INSERT INTO chicken_weight_tracking 
            (chicken_id, weight_standard_id, actual_weight, expected_weight, weight_percentage, weight_category, checked_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (chicken_id, weight_standard_id, actual_weight, expected_weight, weight_percentage, weight_category, session['employee_id']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Weight recorded successfully! {weight_percentage:.1f}% of expected weight - {weight_category.title()}',
            'weight_percentage': round(weight_percentage, 1),
            'weight_category': weight_category
        })
        
    except Exception as e:
        print(f"Error submitting chicken weight: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error recording weight: {str(e)}'
        })

if __name__ == '__main__':
    print("Starting Pig Farm Management System...")
    print("Checking database and tables...")
    
    # Update pregnancy statuses on startup
    print("Updating pregnancy statuses...")
    update_pregnancy_status()
    
    # Update lactation statuses on startup
    print("Updating lactation statuses...")
    update_lactation_status()
    
    # Create database and tables on startup
    if create_database_and_tables():
        print("Database setup completed. Starting Flask application...")
        app.run(debug=True)
    else:
        print("Failed to setup database. Please check your MySQL connection.")
        print("Make sure MySQL is running and the credentials are correct.")
