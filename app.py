import os
import io
import csv
import re
import pandas as pd
from datetime import datetime
from flask import Flask, render_template, request, jsonify, session, send_file, g
from flask_session import Session
from flask_cors import CORS
from openpyxl import Workbook
from openpyxl.utils.exceptions import IllegalCharacterError
from openpyxl.utils import get_column_letter
import mysql.connector
from mysql.connector import errorcode
import bcrypt
import paramiko
import json
import logging
from io import StringIO
import numexpr
import numpy as np
import logging
from typing import Dict,Tuple, List
import pandas as pd
import numexpr
import logging
import pandas as pd
import re
from datetime import datetime, timedelta
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
import logging
import json
import mysql.connector
from io import BytesIO
import operator
from typing import Tuple, List
from dotenv import load_dotenv
import threading
from contextlib import contextmanager
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]  # Only console logging for Railway
)

app = Flask(__name__, static_folder='./dist', static_url_path='')
CORS(app, supports_credentials=True, origins=["http://localhost:3000", "http://localhost:8080", "*"])

app.secret_key = os.urandom(24).hex()

# Ensure directories exist and are writable in Railway.app (DO THIS FIRST)
try:
    session_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sessions')
    upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
    os.makedirs(session_dir, exist_ok=True)
    os.makedirs(upload_dir, exist_ok=True)
except OSError as e:
    logging.error(f"Failed to create directories: {e}")
    # Fallback to /tmp for Railway.app's ephemeral filesystem
    session_dir = '/tmp/sessions'
    upload_dir = '/tmp/uploads'
    os.makedirs(session_dir, exist_ok=True)
    os.makedirs(upload_dir, exist_ok=True)

# Configure session with the created directories
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = session_dir
app.config['UPLOAD_FOLDER'] = upload_dir
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = False
app.config['PERMANENT_SESSION_LIFETIME'] = 86400

# Initialize session (AFTER directory creation and config)
Session(app)  # ✅ Fixed: removed extra parenthesis

# Ensure directories exist and are writable in Railway.app
try:
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)
except OSError as e:
    logging.error(f"Failed to create directories: {e}")
    # Fallback to /tmp for Railway.app's ephemeral filesystem
    app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
    app.config['SESSION_FILE_DIR'] = '/tmp/sessions'
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)

# Debug environment variables
logging.info(f"Environment variables:")
logging.info(f"MYSQL_HOST: {os.getenv('MYSQL_HOST', 'NOT_SET')}")
logging.info(f"MYSQL_USER: {os.getenv('MYSQL_USER', 'NOT_SET')}")
logging.info(f"MYSQL_PASSWORD: {'SET' if os.getenv('MYSQL_PASSWORD') else 'NOT_SET'}")
logging.info(f"MYSQL_DATABASE: {os.getenv('MYSQL_DATABASE', 'NOT_SET')}")

# Hardcode the credentials temporarily since Railway env vars aren't working
DB_CONFIG = {
    'host': 'mysql.railway.internal',
    'user': 'root',
    'password': 'nmHNKdIcsHaFpYPirsWYPBgrVLjhbZZI',
    'database': 'railway',
}

logging.info(f"Final DB_CONFIG: host={DB_CONFIG['host']}, user={DB_CONFIG['user']}, database={DB_CONFIG['database']}")

def get_direct_db_connection():
    """Get database connection without using Flask's g object"""
    try:
        # First, ensure database exists
        temp_conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = temp_conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.close()
        temp_conn.close()
        
        # Now connect to the specific database
        conn = mysql.connector.connect(**DB_CONFIG)
        logging.info("Direct database connection established successfully")
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Database connection failed: Access denied for user - check username/password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Database connection failed: Database does not exist")
        else:
            logging.error(f"Database connection failed: {err}")
        raise Exception(f"Failed to connect to database: {str(err)}")

def get_db_connection():
    if 'db' not in g:
        try:
            conn = mysql.connector.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            cursor.close()
            conn.close()
            g.db = mysql.connector.connect(**DB_CONFIG)
            logging.info("Database connection established successfully")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error("Database connection failed: Access denied for user - check username/password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error("Database connection failed: Database does not exist")
            else:
                logging.error(f"Database connection failed: {err}")
            raise Exception(f"Failed to connect to database: {str(err)}")
    return g.db

@app.teardown_appcontext
def close_db(error):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        tables = [
            """
            CREATE TABLE IF NOT EXISTS login_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255) UNIQUE,
                mobile VARCHAR(10),
                password VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS excel_templates (
                template_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                template_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                user_id INT NOT NULL,
                sheet_name VARCHAR(255),
                headers JSON,
                status ENUM('ACTIVE', 'INACTIVE') DEFAULT 'ACTIVE',
                is_corrected BOOLEAN DEFAULT FALSE,
                remote_file_path VARCHAR(512),
                FOREIGN KEY (user_id) REFERENCES login_details(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS template_columns (
                column_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                template_id BIGINT NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                column_position INT NOT NULL,
                is_validation_enabled BOOLEAN DEFAULT FALSE,
                is_selected BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                UNIQUE (template_id, column_name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS validation_rule_types (
                rule_type_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                rule_name VARCHAR(255) UNIQUE NOT NULL,
                description TEXT,
                parameters TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                is_custom BOOLEAN DEFAULT FALSE,
                column_name VARCHAR(255),
                template_id BIGINT,
                data_type VARCHAR(50),
                source_format VARCHAR(50),
                target_format VARCHAR(50),
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS column_validation_rules (
                column_validation_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                column_id BIGINT NOT NULL,
                rule_type_id BIGINT NOT NULL,
                rule_config JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (column_id) REFERENCES template_columns(column_id) ON DELETE CASCADE,
                FOREIGN KEY (rule_type_id) REFERENCES validation_rule_types(rule_type_id) ON DELETE RESTRICT,
                UNIQUE (column_id, rule_type_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS validation_history (
                history_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                template_id BIGINT NOT NULL,
                template_name VARCHAR(255) NOT NULL,
                error_count INT NOT NULL,
                corrected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                corrected_file_path VARCHAR(512) NOT NULL,
                user_id INT NOT NULL,
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES login_details(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS validation_corrections (
                correction_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                history_id BIGINT NOT NULL,
                row_index INT NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                original_value TEXT,
                corrected_value TEXT,
                rule_failed VARCHAR(255) DEFAULT NULL,
                FOREIGN KEY (history_id) REFERENCES validation_history(history_id) ON DELETE CASCADE
            )
            """
        ]
        for table_sql in tables:
            cursor.execute(table_sql)
        cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'source_format'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN source_format VARCHAR(50)")
            logging.info("Added source_format column to validation_rule_types table")
        cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'target_format'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN target_format VARCHAR(50)")
            logging.info("Added target_format column to validation_rule_types table")
        cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'data_type'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN data_type VARCHAR(50)")
            logging.info("Added data_type column to validation_rule_types table")
        cursor.execute("SHOW COLUMNS FROM excel_templates LIKE 'remote_file_path'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE excel_templates ADD COLUMN remote_file_path VARCHAR(512)")
            logging.info("Added remote_file_path column to excel_templates table")
        cursor.execute("SHOW COLUMNS FROM template_columns LIKE 'is_selected'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE template_columns ADD COLUMN is_selected BOOLEAN DEFAULT FALSE")
            logging.info("Added is_selected column to template_columns table")
        conn.commit()
        cursor.close()
        logging.info("Database tables initialized")
    except Exception as e:
        logging.error(f"Failed to initialize database: {str(e)}")
        raise

def create_admin_user():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        admin_password = bcrypt.hashpw('admin'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cursor.execute("""
            INSERT IGNORE INTO login_details (first_name, last_name, email, mobile, password)
            VALUES (%s, %s, %s, %s, %s)
        """, ('Admin', 'User', 'admin@example.com', '1234567890', admin_password))
        conn.commit()
        cursor.close()
        logging.info("Admin user created or already exists")
    except Exception as e:
        logging.error(f"Failed to create admin user: {str(e)}")
        raise



def create_default_validation_rules():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        default_rules = [
            ("Required", "Ensures the field is not null", '{"allow_null": false}', None, None, None),
            ("Int", "Validates integer format", '{"format": "integer"}', None, None, "Int"),
            ("Float", "Validates number format (integer or decimal)", '{"format": "float"}', None, None, "Float"),
            ("Text", "Allows text with quotes and parentheses", '{"allow_special": false}', None, None, "Text"),
            ("Email", "Validates email format", '{"regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$"}', None, None, "Email"),
            ("Date", "Validates date", '{"format": "%d-%m-%Y"}', "DD-MM-YYYY", None, "Date"),
            ("Boolean", "Validates boolean format (true/false or 0/1)", '{"format": "boolean"}', None, None, "Boolean"),
            ("Alphanumeric", "Validates alphanumeric format", '{"format": "alphanumeric"}', None, None, "Alphanumeric")
        ]
        cursor.executemany("""
            INSERT IGNORE INTO validation_rule_types (rule_name, description, parameters, is_custom, source_format, target_format, data_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [(name, desc, params, False, source, target, dtype) for name, desc, params, source, target, dtype in default_rules])
        conn.commit()
        cursor.close()
        logging.info("Default validation rules ensured successfully")
    except Exception as e:
        logging.error(f"Failed to ensure default validation rules: {str(e)}")
        raise


def detect_column_type(series):
    non_null = series.dropna().astype(str)
    if non_null.empty:
        return "Text"
    if non_null.str.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$").all():
        return "Email"
    try:
        pd.to_datetime(non_null, format="%d-%m-%Y")
        return "Date"
    except Exception:
        try:
            pd.to_datetime(non_null, format="%Y-%m-%d")
            return "Date"
        except Exception:
            pass
    if non_null.str.lower().isin(['true', 'false', '0', '1']).all():
        return "Boolean"
    if non_null.str.match(r"^-?\d+$").all():
        return "Int"
    if non_null.str.match(r"^-?\d+(\.\d+)?$").all():
        return "Float"
    if non_null.str.match(r"^[a-zA-Z0-9]+$").all():
        return "Alphanumeric"
    return "Text"

def assign_default_rules_to_columns(df, headers):
    assignments = {}
    for col in headers:
        col_type = detect_column_type(df[col])
        rules = ["Required"]
        if col_type != "Text" or not any(
            col.lower().startswith(prefix) for prefix in ["name", "address", "phone", "username", "status", "period"]
        ):
            rules.append(col_type)
        else:
            rules.append("Text")
        assignments[col] = rules
    return assignments

from typing import Tuple, List, Dict
import pandas as pd
import numexpr
import logging
import re
import operator

def evaluate_column_rule(df: pd.DataFrame, column_name: str, formula: str, headers: List[str], data_type: str) -> Tuple[bool, List[Tuple[int, str, str, str]]]:
    try:
        error_locations = []
        column_name = column_name.strip().lower()
        headers_lower = [h.strip().lower() for h in headers]

        if column_name not in df.columns.str.lower():
            return False, [(0, "", "ColumnNotFound", f"Column '{column_name}' not found in data")]

        is_arithmetic = ' = ' in formula

        if is_arithmetic:
            formula_parts = formula.strip().split(' = ', 1)
            if len(formula_parts) != 2 or formula_parts[0] != f"'{column_name}'":
                return False, [(0, "", "InvalidFormula", "Arithmetic formula must be 'column_name = expression'")]
            
            right_side = formula_parts[1]
            referenced_columns = [item.strip().lower() for item in re.findall(r"'([^']+)'", right_side)]
            for col in referenced_columns:
                if col not in df.columns.str.lower():
                    return False, [(0, "", "ColumnNotFound", f"Referenced column '{col}' not found in data")]
            
            for col in referenced_columns + [column_name]:
                for i, value in enumerate(df[col]):
                    if pd.isna(value) or str(value).strip() == "":
                        error_locations.append((i + 1, "NULL", f"{column_name}_Formula", f"Value is null or empty in column {col}"))
                    else:
                        try:
                            float(str(value).strip())
                        except ValueError:
                            error_locations.append((i + 1, str(value), f"{column_name}_DataType", f"Invalid numeric value in column {col}: {value}"))

            valid = True
            for i in range(len(df)):
                row_errors = [err for err in error_locations if err[0] == i + 1]
                if row_errors:
                    valid = False
                    continue

                local_dict = {}
                try:
                    for col in referenced_columns:
                        local_dict[col] = float(df.at[i, col])
                except ValueError:
                    valid = False
                    error_locations.append((i + 1, "", f"{column_name}_DataType", f"Invalid numeric value in referenced columns for row {i+1}"))
                    continue

                expr = right_side.replace("'", "").replace(" AND ", " and ").replace(" OR ", " or ")
                
                try:
                    expected = eval(expr, {"__builtins__": {}}, local_dict)
                    expected_num = float(expected)
                except Exception as eval_err:
                    valid = False
                    error_locations.append((i + 1, "", "FormulaEvaluation", f"Error evaluating formula for row {i+1}: {str(eval_err)}"))
                    continue

                actual = df.at[i, column_name]
                if pd.isna(actual):
                    valid = False
                    error_locations.append((i + 1, "NULL", f"{column_name}_Formula", "Value is null"))
                    continue

                actual_value = str(actual).strip()
                expected_value = str(expected_num)
                try:
                    actual_num = float(actual_value)
                    if abs(actual_num - expected_num) > 1e-10:
                        valid = False
                        error_locations.append((i + 1, actual_value, f"{column_name}_Formula", f"Data Error: {column_name} ({actual_value}) does not match formula {right_side} ({expected_value})"))
                except ValueError:
                    valid = False
                    error_locations.append((i + 1, actual_value, f"{column_name}_DataType", f"Invalid numeric value for {column_name}: {actual_value}"))

            return valid, error_locations
        else:
            parts = formula.strip().split(' ', 3)
            if len(parts) != 3 or parts[0] != f"'{column_name}'" or parts[1] not in ['=', '>', '<', '>=', '<=']:
                return False, [(0, "", "InvalidFormula", "Comparison formula must be 'column_name <operator> operand'")]
            
            operator_str = parts[1]
            operand = parts[2]
            valid = True
            operator_map = {'=': operator.eq, '>': operator.gt, '<': operator.lt, '>=': operator.ge, '<=': operator.le}
            op_func = operator_map[operator_str]
            
            if operand.startswith("'") and operand.endswith("'"):
                second_column = operand[1:-1].strip().lower()
                if second_column not in df.columns.str.lower():
                    return False, [(0, "", "ColumnNotFound", f"Second column '{second_column}' not found in data")]
                
                for i in range(len(df)):
                    left_value = df.iloc[i][column_name]
                    right_value = df.iloc[i][second_column]
                    if pd.isna(left_value) or str(left_value).strip() == "":
                        error_locations.append((i + 1, "NULL", f"{column_name}_Formula", f"Value is null in column {column_name}"))
                        valid = False
                        continue
                    if pd.isna(right_value) or str(right_value).strip() == "":
                        error_locations.append((i + 1, str(left_value), f"{column_name}_Formula", f"Value is null in column {second_column}"))
                        valid = False
                        continue
                    
                    try:
                        left_num = float(str(left_value).strip())
                        right_num = float(str(right_value).strip())
                        if not op_func(left_num, right_num):
                            valid = False
                            error_locations.append((i + 1, str(left_value), f"{column_name}_Formula", f"Failed comparison: {left_value} {operator_str} {right_value}"))
                    except ValueError:
                        valid = False
                        error_locations.append((i + 1, str(left_value), f"{column_name}_DataType", f"Invalid numeric value in column {column_name}: {left_value} or {second_column}: {right_value}"))
            else:
                try:
                    operand_value = float(operand)
                    for i, value in enumerate(df[column_name]):
                        if pd.isna(value) or str(value).strip() == "":
                            error_locations.append((i + 1, "NULL", f"{column_name}_Formula", "Value is null"))
                            valid = False
                            continue
                        try:
                            value_num = float(str(value).strip())
                            if not op_func(value_num, operand_value):
                                valid = False
                                error_locations.append((i + 1, str(value), f"{column_name}_Formula", f"Failed comparison: {value} {operator_str} {operand_value}"))
                        except ValueError:
                            valid = False
                            error_locations.append((i + 1, str(value), f"{column_name}_DataType", f"Invalid numeric value in column {column_name}: {value}"))
                except ValueError:
                    return False, [(0, "", "InvalidOperand", f"Invalid operand for comparison: {operand}")]

            return valid, error_locations

    except Exception as e:
        return False, error_locations + [(0, "", "FormulaEvaluation", f"Error evaluating formula for column {column_name}: {str(e)}")]

def read_file(file_path):
    try:
        logging.debug(f"Reading file: {file_path}")
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            xl = pd.ExcelFile(file_path)
            logging.debug(f"Excel file detected, sheets: {xl.sheet_names}")
            sheets = {sheet_name: pd.read_excel(file_path, sheet_name=sheet_name, header=None) 
                     for sheet_name in xl.sheet_names}
            return sheets
        elif file_path.endswith(('.txt', '.csv', '.dat')):
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            if not content.strip():
                logging.error("File is empty")
                raise ValueError("File is empty.")
            try:
                dialect = csv.Sniffer().sniff(content[:1024])
                sep = dialect.delimiter
                logging.debug(f"CSV file detected, delimiter: {sep}")
            except:
                sep = detect_delimiter(file_path)
                logging.debug(f"Delimiter detection failed, using fallback: {sep}")
            df = pd.read_csv(file_path, header=None, sep=sep, encoding='utf-8', quotechar='"', engine='python')
            df.columns = [str(col) for col in df.columns]
            logging.debug(f"CSV file read, shape: {df.shape}")
            return {'Sheet1': df}
        else:
            logging.error("Unsupported file type")
            raise ValueError("Unsupported file type.")
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {str(e)}")
        raise ValueError(f"Error reading file: {str(e)}")

def detect_delimiter(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read(1024)
        if not content.strip():
            logging.warning("File content is empty, using default delimiter: ','")
            return ','
        delimiters = [',', ';', '|', '/', '\t', ':', '-']
        best_delimiter, max_columns, best_consistency = None, 0, 0
        for delim in delimiters:
            try:
                sample_df = pd.read_csv(io.StringIO(content), sep=delim, header=None, nrows=5, quotechar='"', engine='python')
                column_count = sample_df.shape[1]
                row_lengths = [len(row.dropna()) for _, row in sample_df.iterrows()]
                consistency = sum(1 for length in row_lengths if length == column_count) / len(row_lengths)
                if column_count > 1 and column_count > max_columns and consistency > best_consistency:
                    max_columns = column_count
                    best_consistency = consistency
                    best_delimiter = delim
            except Exception:
                continue
        delimiter = best_delimiter or ','
        logging.debug(f"Detected delimiter: {delimiter}")
        return delimiter
    except Exception as e:
        logging.error(f"Error detecting delimiter for {file_path}: {str(e)}")
        return ','

def find_header_row(df, max_rows=10):
    try:
        for i in range(min(len(df), max_rows)):
            row = df.iloc[i].dropna()
            if not row.empty and all(isinstance(x, str) for x in row if pd.notna(x)):
                logging.debug(f"Header row detected at index {i}")
                return i
        logging.warning(f"No header row detected within the first {max_rows} rows")
        return 0 if not df.empty and len(df.columns) > 0 else -1
    except Exception as e:
        logging.error(f"Error finding header row: {str(e)}")
        return -1

def has_special_characters_except_quotes_and_parenthesis(s):
    if not isinstance(s, str):
        logging.debug(f"Value '{s}' is not a string, failing Text validation")
        return True
    for char in s:
        if char not in ['"', '(', ')'] and not char.isalpha() and char != ' ':
            logging.debug(f"Character '{char}' in '{s}' is not allowed for Text validation")
            return True
    return False

def is_valid_date_format(date_string, accepted_date_formats):
    if not isinstance(date_string, str):
        return False
    for date_format in accepted_date_formats:
        try:
            datetime.strptime(date_string, date_format)
            return True
        except ValueError:
            pass
    return False


def apply_default_rules(column_name):
    return '%d-%m-%Y' if "date" in column_name.lower() else None

def check_special_characters_in_column(df, col_name, metadata_type, accepted_date_formats, check_null_cells=True):
    try:
        logging.debug(f"Validating column: {col_name}, type: {metadata_type}, check_null_cells: {check_null_cells}")
        special_char_count, error_cell_locations = 0, []
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT parameters, is_custom, source_format, data_type
            FROM validation_rule_types
            WHERE rule_name = %s
        """, (metadata_type,))
        rule_data = cursor.fetchone()
        cursor.close()
        
        accepted_formats = accepted_date_formats
        if metadata_type.startswith("Date(") and rule_data and rule_data['source_format']:
            format_map = {
                'MM-DD-YYYY': '%m-%d-%Y', 'DD-MM-YYYY': '%d-%m-%Y', 'MM/DD/YYYY': '%m/%d/%Y', 'DD/MM/YYYY': '%d/%m/%Y',
                'MM-YYYY': '%m-%Y', 'MM-YY': '%m-%y', 'MM/YYYY': '%m/%Y', 'MM/YY': '%m/%y'
            }
            accepted_formats = [format_map.get(rule_data['source_format'], '%d-%m-%Y')]
            logging.debug(f"Using specific date format for {col_name}: {rule_data['source_format']} ({accepted_formats[0]})")
        
        if rule_data and rule_data['is_custom'] and not metadata_type.startswith('Date('):
            params = json.loads(rule_data['parameters'])
            logic = params.get('logic')
            base_rules = params.get('base_rules', [])
            for i, cell_value in enumerate(df[col_name], start=1):
                cell_value = str(cell_value).strip() if pd.notna(cell_value) else ""
                error_reason = None
                rule_failed = metadata_type
                if check_null_cells and pd.isna(cell_value):
                    special_char_count += 1
                    error_reason = "Value is null"
                    error_cell_locations.append((i, "NULL", rule_failed, error_reason))
                    continue
                if not cell_value and metadata_type == "Required":
                    special_char_count += 1
                    error_reason = "Value is empty"
                    error_cell_locations.append((i, "EMPTY", rule_failed, error_reason))
                    continue
                valid = True if logic == "OR" else False
                for base_rule in base_rules:
                    base_valid, base_errors = check_special_characters_in_column(
                        df.iloc[[i-1]], col_name, base_rule, accepted_date_formats, check_null_cells
                    )
                    if logic == "AND":
                        valid = valid and (base_valid == 0)
                    elif logic == "OR":
                        valid = valid or (base_valid == 0)
                    if base_valid > 0:
                        for err in base_errors:
                            special_char_count += 1
                            error_reason = err[3] if len(err) > 3 else "Failed base rule"
                            error_cell_locations.append((i, cell_value, rule_failed, error_reason))
                if not valid and not error_cell_locations:
                    special_char_count += 1
                    error_reason = f"Failed custom rule {metadata_type}"
                    error_cell_locations.append((i, cell_value, rule_failed, error_reason))
        else:
            for i, cell_value in enumerate(df[col_name], start=1):
                error_reason = None
                rule_failed = metadata_type
                if check_null_cells and pd.isna(cell_value):
                    special_char_count += 1
                    error_reason = "Value is null"
                    error_cell_locations.append((i, "NULL", rule_failed, error_reason))
                    continue
                cell_value = str(cell_value).strip() if pd.notna(cell_value) else ""
                if not cell_value and metadata_type == "Required":
                    special_char_count += 1
                    error_reason = "Value is empty"
                    error_cell_locations.append((i, "EMPTY", rule_failed, error_reason))
                    continue
                if metadata_type.startswith("Date("):
                    if not cell_value:
                        special_char_count += 1
                        error_reason = "Value is empty"
                        error_cell_locations.append((i, "EMPTY", rule_failed, error_reason))
                    elif not is_valid_date_format(cell_value, accepted_formats):
                        special_char_count += 1
                        error_reason = f"Invalid date format (expected {rule_data['source_format']})"
                        error_cell_locations.append((i, cell_value, rule_failed, error_reason))
                        logging.debug(f"Date validation failed for {col_name} at row {i}: {cell_value}, expected {rule_data['source_format']}")
                elif metadata_type == "Alphanumeric":
                    if not cell_value:
                        special_char_count += 1
                        error_reason = "Value is empty or contains only whitespace"
                        error_cell_locations.append((i, cell_value, rule_failed, error_reason))
                    elif not re.match(r'^[a-zA-Z0-9]+$', cell_value):
                        special_char_count += 1
                        error_reason = "Contains non-alphanumeric characters"
                        error_cell_locations.append((i, cell_value, rule_failed, error_reason))
                elif metadata_type == "Int":
                    if not cell_value.replace('-', '', 1).isdigit():
                        special_char_count += 1
                        error_reason = "Must be an integer"
                        error_cell_locations.append((i, cell_value, rule_failed, error_reason))
                elif metadata_type == "Float":
                    try:
                        float(cell_value)
                    except ValueError:
                        special_char_count += 1
                        error_reason = "Must be a number (integer or decimal)"
                        error_cell_locations.append((i, cell_value, rule_failed, error_reason))
                elif metadata_type == "Text":
                    has_special = has_special_characters_except_quotes_and_parenthesis(cell_value)
                    if has_special:
                        special_char_count += 1
                        error_reason = "Contains invalid characters"
                        error_cell_locations.append((i, cell_value, rule_failed, error_reason))
                elif metadata_type == "Email":
                    if not re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA.Z0-9-.]+$', cell_value):
                        special_char_count += 1
                        error_reason = "Invalid email format"
                        error_cell_locations.append((i, cell_value, rule_failed, error_reason))
                elif metadata_type == "Boolean":
                    if not re.match(r'^(true|false|0|1)$', cell_value, re.IGNORECASE):
                        special_char_count += 1
                        error_reason = "Must be a boolean (true/false or 0/1)"
                        error_cell_locations.append((i, cell_value, rule_failed, error_reason))
        return special_char_count, error_cell_locations
    except Exception as e:
        logging.error(f"Error validating column {col_name}: {str(e)}")
        raise

def transform_date(value, source_format, target_format):
    try:
        if not value or pd.isna(value) or str(value).strip() in ['NULL', '', 'nan']:
            return value
            
        value_str = str(value).strip()
        
        # Format mapping from display format to Python strftime format
        format_map = {
            'MM-DD-YYYY': '%m-%d-%Y', 
            'DD-MM-YYYY': '%d-%m-%Y', 
            'MM/DD/YYYY': '%m/%d/%Y', 
            'DD/MM/YYYY': '%d/%m/%Y',
            'MM-YYYY': '%m-%Y', 
            'MM-YY': '%m-%y', 
            'MM/YYYY': '%m/%Y', 
            'MM/YY': '%m/%y'
        }
        
        # Get Python formats
        source_py_format = format_map.get(source_format)
        target_py_format = format_map.get(target_format)
        
        if not source_py_format or not target_py_format:
            logging.error(f"Invalid date format: source={source_format}, target={target_format}")
            return value
            
        # Parse date with source format and convert to target format
        parsed_date = datetime.strptime(value_str, source_py_format)
        transformed_value = parsed_date.strftime(target_py_format)
        
        logging.debug(f"Date transformation: '{value_str}' ({source_format}) -> '{transformed_value}' ({target_format})")
        return transformed_value
        
    except ValueError as ve:
        logging.error(f"Date parsing error: {ve} - value: '{value}', source: {source_format}, target: {target_format}")
        return value
    except Exception as e:
        logging.error(f"Error transforming date {value} from {source_format} to {target_format}: {str(e)}")
        return value

def get_validation_errors(df, rules, headers):
    error_locations = {}
    for rule in rules:
        if rule['is_custom']:
            formula = rule['parameters']  # e.g., "A + B == C"
            column_name = rule['column_name']
            for idx, row in df.iterrows():
                try:
                    # Parse and evaluate formula with numexpr
                    parsed = parse_formula(formula, headers)  # Assume function returns evaluable str like "(row['A'] + row['B']) == row['C']"
                    result = numexpr.evaluate(parsed, local_dict={'row': row})
                    if not result:
                        left, op, right = formula.partition('==')  # Simplify for ==; extend for other ops
                        left_val = numexpr.evaluate(left.strip(), local_dict={'row': row})
                        right_val = numexpr.evaluate(right.strip(), local_dict={'row': row})
                        reason = f"Failed {rule['rule_name']}: {left_val} != {right_val}"
                        if column_name not in error_locations:
                            error_locations[column_name] = []
                        error_locations[column_name].append({
                            'row': idx + 1,
                            'value': row[column_name],
                            'rule_failed': rule['rule_name'],
                            'reason': reason
                        })
                except Exception as e:
                    reason = f"Formula evaluation failed: {str(e)}"
                    # Add error similarly
        else:
            # Existing generic validation logic
            pass
    return error_locations

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    # Serve static files from the Vite build (dist folder)
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return app.send_static_file(path)
    return app.send_static_file('index.html')

@app.route('/check-auth', methods=['GET'])
def check_auth():
    try:
        logging.debug(f"Checking auth with session: {dict(session)}")
        if 'loggedin' in session and 'user_id' in session:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT email, first_name FROM login_details WHERE id = %s", (session['user_id'],))
            user = cursor.fetchone()
            cursor.close()
            conn.close()
            if user:
                logging.info(f"User {session['user_email']} is authenticated")
                return jsonify({
                    'success': True,
                    'user': {
                        'email': user['email'],
                        'id': session['user_id'],
                        'first_name': user['first_name']  # Add first_name
                    }
                })
            else:
                logging.warning("User not found in database")  
                session.clear()
                return jsonify({'success': False, 'message': 'User not found'}), 401
        logging.warning("User not authenticated")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    except Exception as e:
        logging.error(f"Error in check-auth endpoint: {str(e)}")
        return jsonify({'success': False, 'message': f'Server error: {str(e)}'}), 500

@app.route('/authenticate', methods=['POST'])
def authenticate():
    try:
        email = request.form.get('username') or request.form.get('email')
        password = request.form.get('password')
        logging.debug(f"Login attempt: email={email}, password={'*' * len(password) if password else 'None'}")
        if not email or not password:
            logging.warning(f"Login failed: Email or password missing")
            return jsonify({'success': False, 'message': 'Email and password are required'}), 400

        if email == "admin" and password == "admin":
            session['loggedin'] = True
            session['user_email'] = "admin@example.com"
            session['user_id'] = 1
            session.permanent = True
            logging.info(f"Admin login successful. Session: {dict(session)}")
            return jsonify({'success': True, 'message': 'Login successful', 'user': {'email': 'admin@example.com', 'id': 1}}), 200

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM login_details WHERE LOWER(email) = LOWER(%s)", (email.lower(),))
        account = cursor.fetchone()
        cursor.close()
        if account:
            if bcrypt.checkpw(password.encode('utf-8'), account['password'].encode('utf-8')):
                session['loggedin'] = True
                session['user_email'] = account['email']
                session['user_id'] = account['id']
                session.permanent = True
                logging.info(f"User {email} logged in successfully. Session: {dict(session)}")
                return jsonify({
                    'success': True,
                    'message': 'Login successful',
                    'user': {'email': account['email'], 'id': account['id']}
                }), 200
            else:
                logging.warning(f"Invalid password for {email}")
                return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
        else:
            logging.warning(f"Email {email} not found")
            return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
    except mysql.connector.Error as db_err:
        logging.error(f"Database error during login: {str(db_err)}")
        return jsonify({'success': False, 'message': f'Database error: {str(db_err)}'}), 500
    except Exception as e:
        logging.error(f"Unexpected error during login: {str(e)}")
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500

@app.route('/register', methods=['POST'])
def register():
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')
    email = request.form.get('email')
    mobile = request.form.get('mobile')
    password = request.form.get('password')
    confirm_password = request.form.get('confirm_password')

    if not all([first_name, last_name, email, mobile, password, confirm_password]):
        return jsonify({'success': False, 'message': 'All fields are required'}), 400
    if password != confirm_password:
        return jsonify({'success': False, 'message': 'Passwords do not match'}), 400

    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO login_details (first_name, last_name, email, mobile, password)
            VALUES (%s, %s, %s, %s, %s)
        """, (first_name, last_name, email, mobile, hashed_password))
        user_id = cursor.lastrowid
        conn.commit()
        cursor.close()
        session['loggedin'] = True
        session['user_email'] = email
        session['user_id'] = user_id
        return jsonify({
            'success': True,
            'message': 'Registration successful',
            'user': {'email': email, 'id': user_id}
        }), 200
    except mysql.connector.Error as e:
        logging.error(f"Database error during registration: {str(e)}")
        return jsonify({'success': False, 'message': f'Registration error: {str(e)}'}), 500

@app.route('/reset_password', methods=['POST'])
def reset_password():
    data = request.get_json() or request.form.to_dict()
    email = data.get('email')
    new_password = data.get('new_password')
    confirm_password = data.get('confirm_password')
    if not all([email, new_password, confirm_password]):
        return jsonify({'success': False, 'message': 'All fields are required'}), 400
    if new_password != confirm_password:
        return jsonify({'success': False, 'message': 'Passwords do not match'}), 400
    hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE login_details SET password = %s WHERE email = %s", (hashed_password, email))
        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'Email not found'}), 404
        conn.commit()
        cursor.close()
        return jsonify({'success': True, 'message': 'Password reset successful'}), 200
    except mysql.connector.Error as e:
        logging.error(f"Database error during password reset: {str(e)}")
        return jsonify({'success': False, 'message': f'Error resetting password: {str(e)}'}), 500

@app.route('/templates', methods=['GET'])
def get_templates():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /templates: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT template_id, template_name, created_at, status
            FROM excel_templates
            WHERE user_id = %s AND status = 'ACTIVE'
            ORDER BY created_at DESC
            LIMIT 100
        """, (session['user_id'],))
        templates = cursor.fetchall()
        cursor.execute("SHOW COLUMNS FROM excel_templates LIKE 'is_corrected'")
        if cursor.fetchone():
            cursor.execute("""
                SELECT template_id, template_name, created_at, status, is_corrected
                FROM excel_templates
                WHERE user_id = %s AND status = 'ACTIVE'
                ORDER BY created_at DESC
                LIMIT 100
            """, (session['user_id'],))
            templates = cursor.fetchall()
        cursor.close()
        logging.info(f"Fetched {len(templates)} templates for user {session['user_id']}")
        return jsonify({'success': True, 'templates': templates})
    except mysql.connector.Error as e:
        logging.error(f'Error fetching templates: {str(e)}')
        return jsonify({'success': False, 'message': f'Error fetching templates: {str(e)}'}), 500
    except Exception as e:
        logging.error(f'Unexpected error fetching templates: {str(e)}')
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500


@app.route('/rule-configurations', methods=['GET'])
def get_rule_configurations():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning(f"Unauthorized access to /rule-configurations: session={dict(session)}")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        user_id = session['user_id']
        logging.debug(f"Fetching rule configurations for user_id: {user_id}")

        # Main query to fetch templates with configured rules
        cursor.execute("""
            SELECT 
                t.template_id, 
                t.template_name, 
                t.created_at, 
                COUNT(cvr.column_validation_id) as rule_count
            FROM excel_templates t
            LEFT JOIN template_columns tc ON t.template_id = tc.template_id
            LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            WHERE t.user_id = %s AND t.status = 'ACTIVE' AND t.is_corrected = FALSE
            GROUP BY t.template_id, t.template_name, t.created_at
            HAVING rule_count > 0
            ORDER BY t.created_at DESC
            LIMIT 100
        """, (user_id,))
        templates = cursor.fetchall()
        logging.debug(f"Fetched rule-configured templates: {templates}")

        # Additional debug: Log all templates for the user
        cursor.execute("""
            SELECT 
                t.template_id, 
                t.template_name, 
                t.created_at, 
                t.user_id, 
                t.status, 
                t.is_corrected, 
                t.headers,
                COUNT(cvr.column_validation_id) as rule_count
            FROM excel_templates t
            LEFT JOIN template_columns tc ON t.template_id = tc.template_id
            LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            WHERE t.user_id = %s
            GROUP BY t.template_id, t.template_name, t.created_at
        """, (user_id,))
        all_templates = cursor.fetchall()
        logging.debug(f"All templates for user_id {user_id}: {all_templates}")

        # Log details for each template
        for template in all_templates:
            cursor.execute("""
                SELECT 
                    tc.column_name, 
                    tc.is_selected, 
                    tc.is_validation_enabled
                FROM template_columns tc
                WHERE tc.template_id = %s
            """, (template['template_id'],))
            columns = cursor.fetchall()
            logging.debug(f"Columns for template_id {template['template_id']}: {columns}")

            cursor.execute("""
                SELECT 
                    cvr.column_validation_id, 
                    vrt.rule_name
                FROM column_validation_rules cvr
                JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
                WHERE cvr.column_id IN (
                    SELECT column_id FROM template_columns WHERE template_id = %s
                )
            """, (template['template_id'],))
            rules = cursor.fetchall()
            logging.debug(f"Rules for template_id {template['template_id']}: {rules}")

        cursor.close()
        conn.close()

        if not templates:
            logging.info(f"No templates with rules found for user_id: {user_id}")
        else:
            logging.info(f"Found {len(templates)} templates with rules for user_id: {user_id}")

        return jsonify({'success': True, 'templates': templates})
    except mysql.connector.Error as e:
        logging.error(f"Database error fetching rule configurations: {str(e)}")
        return jsonify({'success': False, 'message': f"Database error: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error fetching rule configurations: {str(e)}")
        return jsonify({'success': False, 'message': f"Unexpected error: {str(e)}"}), 500

@app.route('/validation-history', methods=['GET'])
def get_validation_history():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /validation-history: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT 
                vh.history_id, 
                vh.template_id, 
                vh.template_name, 
                vh.error_count, 
                vh.corrected_at, 
                vh.corrected_file_path,
                et.created_at AS original_uploaded_at
            FROM validation_history vh
            JOIN excel_templates et ON vh.template_id = et.template_id
            WHERE vh.user_id = %s
            ORDER BY et.created_at DESC, vh.corrected_at DESC
        """, (session['user_id'],))
        history_entries = cursor.fetchall()

        grouped_history = {}
        for entry in history_entries:
            template_name = entry['template_name']
            if not template_name.endswith('_corrected.xlsx') and not template_name.endswith('_corrected.csv'):
                continue
            base_template_name = template_name.replace('_corrected.xlsx', '').replace('_corrected.csv', '')
            if base_template_name not in grouped_history:
                cursor.execute("""
                    SELECT created_at
                    FROM excel_templates
                    WHERE template_name = %s AND user_id = %s
                    ORDER BY created_at ASC
                    LIMIT 1
                """, (base_template_name, session['user_id']))
                original_entry = cursor.fetchone()
                original_uploaded_at = original_entry['created_at'] if original_entry else entry['original_uploaded_at']
                grouped_history[base_template_name] = {
                    'original_uploaded_at': original_uploaded_at.isoformat(),  # Ensure ISO format
                    'data_loads': []
                }
            grouped_history[base_template_name]['data_loads'].append({
                'history_id': entry['history_id'],
                'template_id': entry['template_id'],
                'template_name': entry['template_name'],
                'error_count': entry['error_count'],
                'corrected_at': entry['corrected_at'].isoformat(),  # Ensure ISO format
                'corrected_file_path': entry['corrected_file_path']
            })

        cursor.close()
        logging.info(f"Fetched validation history for user {session['user_id']}: {len(grouped_history)} templates")
        logging.debug(f"Validation history response: {json.dumps(grouped_history, default=str)}")
        return jsonify({'success': True, 'history': grouped_history})
    except mysql.connector.Error as e:
        logging.error(f'Error fetching validation history: {str(e)}')
        return jsonify({'success': False, 'message': f'Error fetching validation history: {str(e)}'}), 500
    except Exception as e:
        logging.error(f'Unexpected error fetching validation history: {str(e)}')
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500

@app.route('/validation-corrections/<int:history_id>', methods=['GET'])
def get_validation_corrections(history_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /validation-corrections: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT vh.template_id, vh.template_name, vh.corrected_file_path, et.headers
            FROM validation_history vh
            JOIN excel_templates et ON vh.template_id = et.template_id
            WHERE vh.history_id = %s AND vh.user_id = %s
        """, (history_id, session['user_id']))
        history_entry = cursor.fetchone()
        if not history_entry:
            cursor.close()
            return jsonify({'error': 'Validation history not found'}), 404

        headers = json.loads(history_entry['headers']) if history_entry['headers'] else []

        cursor.execute("""
            SELECT row_index, column_name, original_value, corrected_value, rule_failed
            FROM validation_corrections
            WHERE history_id = %s
        """, (history_id,))
        corrections = cursor.fetchall()

        file_path = history_entry['corrected_file_path']
        if not os.path.exists(file_path):
            cursor.close()
            return jsonify({'error': 'Corrected file not found'}), 404

        sheets = read_file(file_path)
        sheet_name = list(sheets.keys())[0]
        df = sheets[sheet_name]
        header_row = find_header_row(df)
        if header_row == -1:
            cursor.close()
            return jsonify({'error': 'Could not detect header row'}), 400
        df.columns = headers
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        correction_details = []
        for correction in corrections:
            row_index = correction['row_index'] - 1
            if row_index < 0 or row_index >= len(df):
                continue
            row_data = df.iloc[row_index].to_dict()
            correction_details.append({
                'row_index': correction['row_index'],
                'column_name': correction['column_name'],
                'original_value': correction['original_value'],
                'corrected_value': correction['corrected_value'],
                'row_data': row_data,
                'rule_failed': correction['rule_failed']  # Include rule_failed
            })

        cursor.close()
        return jsonify({
            'success': True,
            'headers': headers,
            'corrections': correction_details
        })
    except mysql.connector.Error as e:
        logging.error(f'Error fetching validation corrections: {str(e)}')
        return jsonify({'error': f'Error fetching validation corrections: {str(e)}'}), 500
    except Exception as e:
        logging.error(f'Unexpected error fetching validation corrections: {str(e)}')
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

@app.route('/delete-validation/<int:history_id>', methods=['DELETE'])
def delete_validation(history_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /delete-validation: session missing")
        return jsonify({'error': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT corrected_file_path
            FROM validation_history
            WHERE history_id = %s AND user_id = %s
        """, (history_id, session['user_id']))
        history_entry = cursor.fetchone()
        if not history_entry:
            cursor.close()
            return jsonify({'error': 'Validation history not found'}), 404

        file_path = history_entry[0]
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Deleted file: {file_path}")

        cursor.execute("""
            DELETE FROM validation_history
            WHERE history_id = %s AND user_id = %s
        """, (history_id, session['user_id']))

        conn.commit()
        cursor.close()
        return jsonify({'success': True, 'message': 'Validation history deleted successfully'})
    except mysql.connector.Error as e:
        logging.error(f'Error deleting validation history: {str(e)}')
        return jsonify({'error': f'Error deleting validation history: {str(e)}'}), 500
    except Exception as e:
        logging.error(f'Unexpected error deleting validation history: {str(e)}')
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

@app.route('/delete-template/<int:template_id>', methods=['DELETE'])
def delete_template(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /delete-template: session missing")
        return jsonify({'error': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT template_name
            FROM excel_templates
            WHERE template_id = %s AND user_id = %s AND status = 'ACTIVE'
        """, (template_id, session['user_id']))
        template_entry = cursor.fetchone()
        if not template_entry:
            cursor.close()
            return jsonify({'error': 'Template not found'}), 404

        cursor.execute("""
            DELETE FROM validation_history
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))

        cursor.execute("""
            DELETE FROM excel_templates
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))

        conn.commit()
        cursor.close()
        return jsonify({'success': True, 'message': 'Template deleted successfully'})
    except mysql.connector.Error as e:
        logging.error(f'Error deleting template: {str(e)}')
        return jsonify({'error': f'Error deleting template: {str(e)}'}), 500
    except Exception as e:
        logging.error(f'Unexpected error deleting template: {str(e)}')
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

@app.route('/upload', methods=['POST'])
def upload():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /upload: session missing")
        return jsonify({'error': 'Not logged in'}), 401
    if 'file' not in request.files:
        logging.warning("No file provided in upload request")
        return jsonify({'error': 'No file uploaded'}), 400
    file = request.files['file']
    if file.filename == '':
        logging.warning("No file selected in upload request")
        return jsonify({'error': 'No file selected'}), 400
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    try:
        file.save(file_path)
        logging.info(f"File saved: {file_path}")
    except Exception as e:
        logging.error(f"Failed to save file {file.filename}: {str(e)}")
        return jsonify({'error': f'Failed to save file: {str(e)}'}), 500

    try:
        sheets = read_file(file_path)
        logging.debug(f"Sheets extracted: {list(sheets.keys())}")
    except Exception as e:
        logging.error(f"Failed to read file {file_path}: {str(e)}")
        return jsonify({'error': f'Failed to read file: {str(e)}'}), 400

    try:
        sheet_names = list(sheets.keys())
        if not sheet_names:
            logging.error("No sheets found in the file")
            return jsonify({'error': 'No sheets found in the file'}), 400
        sheet_name = sheet_names[0]
        df = sheets[sheet_name]
        logging.debug(f"Raw DataFrame: {df.to_dict()}")
        logging.debug(f"DataFrame shape: {df.shape}")
        header_row = find_header_row(df)
        if header_row == -1:
            logging.warning(f"Could not detect header row in file {file.filename}")
            return jsonify({'error': 'Could not detect header row'}), 400
        headers = df.iloc[header_row].tolist()
        logging.debug(f"Headers extracted: {headers}")
        if not headers or all(not h for h in headers):
            logging.error("No valid headers found in file")
            return jsonify({'error': 'No valid headers found in the file'}), 400
    except Exception as e:
        logging.error(f"Error processing file {file.filename}: {str(e)}")
        return jsonify({'error': f'Error processing file: {str(e)}'}), 400

    session.pop('df', None)
    session.pop('header_row', None)
    session.pop('headers', None)
    session.pop('sheet_name', None)
    session.pop('current_step', None)
    session.pop('selected_headers', None)
    session.pop('validations', None)
    session.pop('error_cell_locations', None)
    session.pop('data_rows', None)
    session.pop('corrected_file_path', None)

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Check if a template with the same name, headers, and sheet name exists
        cursor.execute("""
            SELECT template_id, headers, sheet_name
            FROM excel_templates
            WHERE template_name = %s AND user_id = %s AND status = 'ACTIVE'
            ORDER BY created_at DESC
        """, (file.filename, session['user_id']))
        existing_templates = cursor.fetchall()
        logging.info(f"Found {len(existing_templates)} existing templates with name {file.filename}")

        template_id = None
        has_existing_rules = False
        validations = {}
        selected_headers = []

        matching_template = None
        for template in existing_templates:
            stored_headers = json.loads(template['headers']) if template['headers'] else []
            stored_sheet_name = template['sheet_name']
            if stored_headers == headers and stored_sheet_name == sheet_name:
                matching_template = template
                break

        if matching_template:
            template_id = matching_template['template_id']
            # Check for existing rules
            cursor.execute("""
                SELECT tc.column_name, vrt.rule_name
                FROM template_columns tc
                JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
                WHERE tc.template_id = %s AND tc.is_selected = TRUE
            """, (template_id,))
            rules_data = cursor.fetchall()
            for row in rules_data:
                column_name = row['column_name']
                rule_name = row['rule_name']
                if column_name not in validations:
                    validations[column_name] = []
                validations[column_name].append(rule_name)
                if column_name not in selected_headers:
                    selected_headers.append(column_name)
            has_existing_rules = len(validations) > 0
        else:
            # New template
            cursor.execute("""
                INSERT INTO excel_templates (template_name, user_id, sheet_name, headers, is_corrected)
                VALUES (%s, %s, %s, %s, %s)
            """, (file.filename, session['user_id'], sheet_name, json.dumps(headers), False))
            template_id = cursor.lastrowid
            column_data = [(template_id, header, i + 1, False) for i, header in enumerate(headers)]
            cursor.executemany("""
                INSERT INTO template_columns (template_id, column_name, column_position, is_selected)
                VALUES (%s, %s, %s, %s)
            """, column_data)

        conn.commit()
        cursor.close()

        session['file_path'] = file_path
        session['template_id'] = template_id
        session['df'] = df.to_json()
        session['header_row'] = header_row
        session['headers'] = headers
        session['sheet_name'] = sheet_name
        session['current_step'] = 1 if not has_existing_rules else 3
        session['validations'] = validations
        session['selected_headers'] = selected_headers
        session['has_existing_rules'] = has_existing_rules

        logging.info(f"Upload processed: template_id={template_id}, filename={file.filename}, has_existing_rules={has_existing_rules}, redirecting to step={'3' if has_existing_rules else '1'}")

        return jsonify({
            'success': True,
            'sheets': {sheet_name: {'headers': headers}},
            'file_name': file.filename,
            'template_id': template_id,
            'has_existing_rules': has_existing_rules,
            'sheet_name': sheet_name,
            'skip_to_step_3': has_existing_rules
        })
    except mysql.connector.Error as e:
        logging.error(f'Error saving template: {str(e)}')
        return jsonify({'error': f'Error saving template: {str(e)}'}), 500
    except Exception as e:
        logging.error(f'Unexpected error saving template: {str(e)}')
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/step/1', methods=['POST'])
def submit_step_one():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /step/1: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        headers = request.form.getlist('headers')
        new_header_row = request.form.get('new_header_row')
        logging.debug(f"Step 1 submitted: headers={headers}, new_header_row={new_header_row}")
        if not headers:
            logging.error("No headers provided in step 1")
            return jsonify({'success': False, 'message': 'No headers provided'}), 400
        if 'file_path' not in session or 'template_id' not in session:
            logging.error("Session missing file_path or template_id")
            return jsonify({'success': False, 'message': 'Session data missing'}), 400

        file_path = session['file_path']
        template_id = session['template_id']
        sheets = read_file(file_path)
        sheet_name = session.get('sheet_name', list(sheets.keys())[0])
        df = sheets[sheet_name]
        header_row = find_header_row(df)
        if header_row == -1:
            logging.error("Could not detect header row")
            return jsonify({'success': False, 'message': 'Could not detect header row'}), 400
        df.columns = session['headers']
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        # Auto-detect rules
        validations = assign_default_rules_to_columns(df, headers)
        session['selected_headers'] = headers
        session['validations'] = validations
        session['current_step'] = 2

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE template_columns
            SET is_selected = FALSE
            WHERE template_id = %s
        """, (template_id,))
        for header in headers:
            cursor.execute("""
                UPDATE template_columns
                SET is_selected = TRUE
                WHERE template_id = %s AND column_name = %s
            """, (template_id, header))
            cursor.execute("""
                SELECT column_id FROM template_columns
                WHERE template_id = %s AND column_name = %s
            """, (template_id, header))
            column_id = cursor.fetchone()[0]
            for rule_name in validations.get(header, []):
                cursor.execute("""
                    SELECT rule_type_id FROM validation_rule_types
                    WHERE rule_name = %s AND is_custom = FALSE
                """, (rule_name,))
                result = cursor.fetchone()
                if result:
                    rule_type_id = result[0]
                    cursor.execute("""
                        INSERT IGNORE INTO column_validation_rules (column_id, rule_type_id, rule_config)
                        VALUES (%s, %s, %s)
                    """, (column_id, rule_type_id, '{}'))
        conn.commit()
        cursor.close()
        logging.info(f"Step 1 completed: headers={headers}, auto-assigned rules={validations}")
        return jsonify({'success': True, 'headers': headers, 'validations': validations})
    except Exception as e:
        logging.error(f"Error in step 1: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/step/2', methods=['POST'])
def submit_step_two():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /step/2: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        action = request.form.get('action', 'save')
        validations = {}
        for key, values in request.form.lists():
            if key.startswith('validations_'):
                header = key.replace('validations_', '')
                validations[header] = values
        logging.debug(f"Step 2 submitted: action={action}, validations={validations}")
        if not validations and action == 'review':
            logging.error("No validations provided for review")
            return jsonify({'success': False, 'message': 'No validations provided'}), 400

        template_id = session.get('template_id')
        if not template_id:
            logging.error("Session missing template_id")
            return jsonify({'success': False, 'message': 'Session data missing'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM column_validation_rules
            WHERE column_id IN (
                SELECT column_id FROM template_columns WHERE template_id = %s
            )
        """, (template_id,))
        for header, rules in validations.items():
            cursor.execute("""
                SELECT column_id FROM template_columns
                WHERE template_id = %s AND column_name = %s
            """, (template_id, header))
            result = cursor.fetchone()
            if not result:
                continue
            column_id = result[0]
            for rule_name in rules:
                cursor.execute("""
                    SELECT rule_type_id FROM validation_rule_types
                    WHERE rule_name = %s
                """, (rule_name,))
                result = cursor.fetchone()
                if result:
                    rule_type_id = result[0]
                    cursor.execute("""
                        INSERT IGNORE INTO column_validation_rules (column_id, rule_type_id, rule_config)
                        VALUES (%s, %s, %s)
                    """, (column_id, rule_type_id, '{}'))
        conn.commit()
        cursor.close()

        session['validations'] = validations
        session['current_step'] = 3 if action == 'review' else 2
        logging.info(f"Step 2 completed: action={action}, validations={validations}")
        return jsonify({'success': True, 'message': 'Step 2 completed successfully'})
    except Exception as e:
        logging.error(f"Error in step 2: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/template/<int:template_id>/<sheet_name>', methods=['GET'])
def get_template(template_id, sheet_name):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /template: session missing")
        return jsonify({'error': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT template_name, sheet_name, headers
            FROM excel_templates
            WHERE template_id = %s AND user_id = %s AND status = 'ACTIVE'
        """, (template_id, session['user_id']))
        template_record = cursor.fetchone()
        logging.debug(f"Template query result for template_id {template_id}: {template_record}")
        if not template_record:
            logging.error(f"Template not found for template_id: {template_id}, user_id: {session['user_id']}")
            cursor.close()
            return jsonify({'error': 'Template not found'}), 404

        headers = json.loads(template_record['headers']) if template_record['headers'] else []
        stored_sheet_name = template_record['sheet_name'] or sheet_name
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], template_record['template_name'])
        logging.debug(f"Template details: template_name={template_record['template_name']}, sheet_name={stored_sheet_name}, headers={headers}, file_path={file_path}")

        cursor.execute("""
            SELECT COUNT(*) as rule_count
            FROM template_columns tc
            JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            WHERE tc.template_id = %s AND tc.is_selected = TRUE
        """, (template_id,))
        rule_count = cursor.fetchone()['rule_count']
        has_existing_rules = rule_count > 0
        logging.debug(f"Template {template_id} has {rule_count} validation rules, has_existing_rules: {has_existing_rules}")

        if not headers or not os.path.exists(file_path):
            logging.warning(f"No headers or file missing for template_id: {template_id}, attempting to read from file")
            if os.path.exists(file_path):
                try:
                    sheets = read_file(file_path)
                    sheet_names = list(sheets.keys())
                    logging.debug(f"Available sheets: {sheet_names}")
                    if not sheet_names:
                        logging.error(f"No sheets found in file {file_path}")
                        cursor.close()
                        return jsonify({'error': 'No sheets found in the file'}), 400
                    actual_sheet_name = stored_sheet_name if stored_sheet_name in sheets else sheet_names[0]
                    df = sheets[actual_sheet_name]
                    header_row = find_header_row(df)
                    if header_row == -1:
                        logging.error(f"Could not detect header row in file {file_path}")
                        cursor.close()
                        return jsonify({'error': 'Could not detect header row'}), 400
                    headers = df.iloc[header_row].tolist()
                    logging.debug(f"Headers extracted from file: {headers}")
                    # Update database with new headers
                    cursor.execute("""
                        UPDATE excel_templates
                        SET headers = %s, sheet_name = %s
                        WHERE template_id = %s
                    """, (json.dumps(headers), actual_sheet_name, template_id))
                    conn.commit()
                    session['file_path'] = file_path
                    session['template_id'] = template_id
                    session['df'] = df.to_json()
                    session['header_row'] = header_row
                    session['headers'] = headers
                    session['sheet_name'] = actual_sheet_name
                    session['current_step'] = 1
                except Exception as e:
                    logging.error(f"Error reading file {file_path}: {str(e)}")
                    cursor.close()
                    return jsonify({'error': f'Error reading file: {str(e)}'}), 400
            else:
                logging.error(f"Template file not found: {file_path}")
                cursor.close()
                return jsonify({'error': 'Template file not found and no headers stored'}), 404

        if not headers:
            logging.error(f"No valid headers could be retrieved for template_id: {template_id}")
            cursor.close()
            return jsonify({'error': 'No valid headers found'}), 400

        cursor.close()
        return jsonify({
            'success': True,
            'sheets': {stored_sheet_name: {'headers': headers}},
            'file_name': template_record['template_name'],
            'file_path': file_path,
            'sheet_name': stored_sheet_name,
            'has_existing_rules': has_existing_rules
        })
    except mysql.connector.Error as e:
        logging.error(f"Database error in get_template: {str(e)}")
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()

@app.route('/template/<int:template_id>/rules', methods=['GET'])
def get_template_rules(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'error': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT tc.column_name, vrt.rule_name
            FROM template_columns tc
            LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            LEFT JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
            WHERE tc.template_id = %s AND tc.is_selected = TRUE
        """, (template_id,))
        rules_data = cursor.fetchall()
        logging.debug(f"Rules data for template_id {template_id}: {rules_data}")
        cursor.close()

        rules = {}
        for row in rules_data:
            column_name = row['column_name']
            rule_name = row['rule_name']
            if column_name not in rules:
                rules[column_name] = []
            if rule_name:
                rules[column_name].append(rule_name)

        logging.debug(f"Constructed rules for template_id {template_id}: {rules}")
        return jsonify({'success': True, 'rules': rules})
    except mysql.connector.Error as e:
        logging.error(f'Error fetching template rules: {str(e)}')
        return jsonify({'error': f'Error fetching template rules: {str(e)}'}), 500

@app.route('/template/<int:template_id>/rules', methods=['POST'])
def update_template_rules(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'error': 'Not logged in'}), 401
    data = request.get_json()
    rules = data.get('rules', {})

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT column_id, column_name FROM template_columns WHERE template_id = %s AND is_selected = TRUE", (template_id,))
        column_map = {row['column_name']: row['column_id'] for row in cursor.fetchall()}

        cursor.execute("SELECT rule_type_id, rule_name FROM validation_rule_types WHERE is_active = TRUE")
        rule_map = {row['rule_name']: row['rule_type_id'] for row in cursor.fetchall()}

        cursor.execute("DELETE FROM column_validation_rules WHERE column_id IN (SELECT column_id FROM template_columns WHERE template_id = %s AND is_selected = TRUE)", (template_id,))

        validation_data = []
        for header, rule_names in rules.items():
            column_id = column_map.get(header)
            if not column_id:
                continue
            for rule_name in rule_names:
                rule_type_id = rule_map.get(rule_name)
                if rule_type_id:
                    validation_data.append((column_id, rule_type_id, json.dumps({})))
                else:
                    logging.warning(f"No rule_type_id found for validation {rule_name}")
        if validation_data:
            cursor.executemany("""
                INSERT INTO column_validation_rules (column_id, rule_type_id, rule_config)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE rule_config = VALUES(rule_config)
            """, validation_data)
            logging.debug(f"Inserted validation rules: {validation_data}")
        conn.commit()
        cursor.close()
        return jsonify({'success': True})
    except mysql.connector.Error as e:
        logging.error(f'Error updating template rules: {str(e)}')
        return jsonify({'error': f'Error updating template rules: {str(e)}'}), 500

@app.route('/validate-existing/<int:template_id>', methods=['GET'])
def validate_existing_template(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /validate-existing: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        df_json = session.get('df')
        if not df_json:
            logging.error("No data available in session")
            return jsonify({'success': False, 'message': 'No data available'}), 400
        df = pd.read_json(StringIO(df_json))
        headers = session['headers']
        df.columns = headers
        df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT tc.column_name, vrt.rule_name, vrt.source_format
            FROM template_columns tc
            JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
            WHERE tc.template_id = %s AND tc.is_selected = TRUE AND vrt.rule_name NOT LIKE 'Transform-Date(%'
        """, (template_id,))
        rules = cursor.fetchall()
        cursor.close()

        error_cell_locations = {}
        accepted_date_formats = ['%d-%m-%Y', '%m-%d-%Y', '%m/%d/%Y', '%d/%m/%Y', '%m-%Y', '%m-%y', '%m/%Y', '%m/%y']
        for rule in rules:
            column_name = rule['column_name']
            rule_name = rule['rule_name']
            if rule_name.startswith('Date(') and rule['source_format']:
                format_map = {
                    'MM-DD-YYYY': '%m-%d-%Y', 'DD-MM-YYYY': '%d-%m-%Y', 'MM/DD/YYYY': '%m/%d/%Y', 'DD/MM/YYYY': '%d/%m/%Y',
                    'MM-YYYY': '%m-%Y', 'MM-YY': '%m-%y', 'MM/YYYY': '%m/%Y', 'MM/YY': '%m/%y'
                }
                accepted_date_formats = [format_map.get(rule['source_format'], '%d-%m-%Y')]
            error_count, locations = check_special_characters_in_column(
                df, column_name, rule_name, accepted_date_formats, check_null_cells=True
            )
            if error_count > 0:
                error_cell_locations[column_name] = [
                    {'row': loc[0], 'value': loc[1], 'rule_failed': loc[2], 'reason': loc[3]}
                    for loc in locations
                ]

        data_rows = df.to_dict('records')
        for row in data_rows:
            for key, value in row.items():
                if pd.isna(value) or value == '':
                    row[key] = 'NULL'

        logging.info(f"Validation completed for template {template_id}: {len(error_cell_locations)} columns with errors")
        return jsonify({
            'success': True,
            'error_cell_locations': error_cell_locations,
            'data_rows': data_rows
        })
    except Exception as e:
        logging.error(f"Error validating template {template_id}: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/validate-existing/<int:template_id>', methods=['POST'])
def save_existing_template_corrections_updated(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    try:
        data = request.get_json()
        corrections = data.get('corrections', {})
        phase = data.get('phase', 'generic')  # 'generic', 'custom', or 'final'
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get template details
        cursor.execute("""
            SELECT template_name, sheet_name, headers 
            FROM excel_templates 
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404
        
        # Get the data to correct
        df_json = session.get('df')
        if not df_json:
            cursor.close()
            return jsonify({'success': False, 'message': 'No data available in session'}), 400
        
        df = pd.read_json(StringIO(df_json))
        headers = json.loads(template['headers'])
        df.columns = headers
        df = df.iloc[session.get('header_row', 0) + 1:].reset_index(drop=True)
        
        # Apply corrections
        correction_count = 0
        for column, row_corrections in corrections.items():
            if column not in headers:
                continue
            for row_str, value in row_corrections.items():
                try:
                    row_index = int(row_str)
                    if 0 <= row_index < len(df):
                        # Store original value for correction tracking
                        original_value = df.at[row_index, column]
                        df.at[row_index, column] = value
                        correction_count += 1
                        logging.info(f"Applied correction: Row {row_index+1}, Column {column}, {original_value} → {value}")
                except (ValueError, IndexError) as e:
                    logging.warning(f"Invalid correction: {row_str}, {column}, {value} - {str(e)}")
                    continue
        
        # Save corrected file
        base_name, ext = os.path.splitext(template['template_name'])
        
        # Create appropriate filename based on phase
        if phase == 'final':
            corrected_filename = f"{base_name}_final_corrected{ext}"
        else:
            corrected_filename = f"{base_name}_corrected_{phase}{ext}"
            
        corrected_file_path = os.path.join(app.config['UPLOAD_FOLDER'], corrected_filename)
        
        try:
            if ext.lower() == '.xlsx':
                df.to_excel(corrected_file_path, index=False, sheet_name=template['sheet_name'])
                logging.info(f"Saved Excel file: {corrected_file_path}")
            else:
                df.to_csv(corrected_file_path, index=False)
                logging.info(f"Saved CSV file: {corrected_file_path}")
        except Exception as save_error:
            cursor.close()
            logging.error(f"Failed to save corrected file: {str(save_error)}")
            return jsonify({'success': False, 'message': f'Failed to save corrected file: {str(save_error)}'}), 500
        
        # Save to validation history only for final phase or if it's the first phase
        if phase == 'final' or phase == 'generic':
            cursor.execute("""
                INSERT INTO validation_history (template_id, template_name, error_count, corrected_file_path, user_id)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    corrected_file_path = VALUES(corrected_file_path),
                    error_count = VALUES(error_count)
            """, (template_id, corrected_filename, correction_count, corrected_file_path, session['user_id']))
            history_id = cursor.lastrowid or cursor.execute("SELECT LAST_INSERT_ID()").fetchone()[0]
            
            # Save individual corrections for tracking
            correction_records = []
            for column, row_corrections in corrections.items():
                if column not in headers:
                    continue
                for row_str, corrected_value in row_corrections.items():
                    try:
                        row_index = int(row_str)
                        if 0 <= row_index < len(df):
                            # Get original value from session data for comparison
                            original_df = pd.read_json(StringIO(session['df']))
                            original_df.columns = headers
                            original_df = original_df.iloc[session.get('header_row', 0) + 1:].reset_index(drop=True)
                            
                            original_value = str(original_df.at[row_index, column]) if row_index < len(original_df) else 'NULL'
                            
                            correction_records.append((
                                history_id, 
                                row_index + 1, 
                                column, 
                                original_value, 
                                corrected_value, 
                                f'{phase}_rule'
                            ))
                    except (ValueError, IndexError):
                        continue
            
            if correction_records:
                cursor.executemany("""
                    INSERT INTO validation_corrections 
                    (history_id, row_index, column_name, original_value, corrected_value, rule_failed)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        corrected_value = VALUES(corrected_value),
                        rule_failed = VALUES(rule_failed)
                """, correction_records)
        
        conn.commit()
        cursor.close()
        
        # Update session with corrected data for future steps
        session['corrected_df'] = df.to_json()
        session['corrected_file_path'] = corrected_file_path
        
        logging.info(f"Successfully saved {correction_count} {phase} corrections for template {template_id}")
        
        return jsonify({
            'success': True, 
            'corrected_file_path': corrected_file_path, 
            'history_id': history_id if phase in ['final', 'generic'] else None,
            'correction_count': correction_count,
            'message': f'{correction_count} {phase} corrections applied successfully'
        })
        
    except Exception as e:
        logging.error(f"Error saving {phase} corrections: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.rollback()
        return jsonify({'success': False, 'message': f'Failed to save corrections: {str(e)}'}), 500

    
@app.route('/validate-row/<int:template_id>', methods=['POST'])
def validate_row_updated(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
   
    try:
        data = request.get_json()
        row_index = data['row_index']
        updated_row = data['updated_row']
        use_corrected = data.get('use_corrected', True)
       
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
       
        # Get template
        cursor.execute("SELECT * FROM excel_templates WHERE template_id = %s AND user_id = %s",
                      (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404
       
        # Get headers and create single-row dataframe
        headers = json.loads(template['headers'])
        single_row_df = pd.DataFrame([updated_row], columns=headers)
       
        # UPDATED: Fetch only active custom rules for this template
        cursor.execute("""
            SELECT vrt.rule_name, vrt.parameters, vrt.column_name
            FROM validation_rule_types vrt
            WHERE vrt.template_id = %s AND vrt.is_custom = TRUE AND vrt.is_active = TRUE
        """, (template_id,))
        rules = cursor.fetchall()
        cursor.close()
 
        # Validate the single row against active custom rules only
        errors = []
       
        for rule in rules:
            try:
                column_name = rule['column_name']
                formula = rule['parameters']
                rule_name = rule['rule_name']
               
                # Use the updated evaluate_column_rule function
                data_type = 'Float'  # Default for custom rules
                is_valid, error_locations = evaluate_column_rule(single_row_df, column_name, formula, headers, data_type)
               
                for error in error_locations:
                    if len(error) > 3:  # Ensure we have all error details
                        errors.append({
                            'column': column_name,
                            'rule_failed': error[2],
                            'reason': error[3],
                            'value': error[1]
                        })
            except Exception as e:
                logging.error(f"Error validating rule {rule['rule_name']} for row: {str(e)}")
                errors.append({
                    'column': rule['column_name'],
                    'rule_failed': rule['rule_name'],
                    'reason': f'Validation error: {str(e)}',
                    'value': updated_row.get(rule['column_name'], 'NULL')
                })
 
        valid = len(errors) == 0
        updated_data_row = single_row_df.iloc[0].to_dict()
       
        return jsonify({
            'success': True,
            'valid': valid,
            'errors': errors,
            'updated_data_row': updated_data_row,
            'validation_details': {
                'rules_checked': len(rules),
                'errors_found': len(errors),
                'row_index': row_index
            }
        })
       
    except Exception as e:
        logging.error(f"Error validating row: {str(e)}")
        return jsonify({'success': False, 'message': f'Validation error: {str(e)}'}), 500
 

@app.route('/step/<int:step>', methods=['GET', 'POST'])
def step(step):
    if 'loggedin' not in session:
        return jsonify({'error': 'Not logged in'}), 401
    if 'df' not in session or session['df'] is None:
        logging.error("Session data missing: 'df' not found or is None")
        return jsonify({'error': 'Please upload a file first'}), 400
    session['current_step'] = step
    try:
        df = pd.read_json(StringIO(session['df']))
    except Exception as e:
        logging.error(f"Error reading session['df']: {str(e)}")
        return jsonify({'error': 'Invalid session data: Unable to load DataFrame'}), 500
    headers = session['headers']
    
    if step == 1:
        if request.method == 'POST':
            selected_headers = request.form.getlist('headers')
            new_header_row = request.form.get('new_header_row')
            if new_header_row:
                try:
                    header_row = int(new_header_row)
                    headers = df.iloc[header_row].tolist()
                    session['header_row'] = header_row
                    session['headers'] = headers
                    return jsonify({'headers': headers})
                except ValueError:
                    return jsonify({'error': 'Invalid header row number'}), 400
            if not selected_headers:
                return jsonify({'error': 'Please select at least one column'}), 400
            session['selected_headers'] = selected_headers
            session['current_step'] = 2

            # Mark selected headers in the database
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE template_columns SET is_selected = FALSE WHERE template_id = %s", (session['template_id'],))
            for header in selected_headers:
                cursor.execute("""
                    UPDATE template_columns
                    SET is_selected = TRUE
                    WHERE template_id = %s AND column_name = %s
                """, (session['template_id'], header))
            conn.commit()
            cursor.close()

            return jsonify({'success': True})
        return jsonify({'headers': headers})
    elif step == 2:
        if 'selected_headers' not in session:
            session['current_step'] = 1
            return jsonify({'error': 'Select headers first'}), 400
        selected_headers = session['selected_headers']
        if request.method == 'POST':
            try:
                logging.debug(f"Received form data: {dict(request.form)}")
                validations = {header: request.form.getlist(f'validations_{header}') 
                              for header in selected_headers}
                logging.debug(f"Constructed validations: {validations}")
                session['validations'] = validations
                df.columns = session['headers']
                logging.debug(f"DataFrame after setting headers: {df.to_dict()}")
                df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)
                logging.debug(f"DataFrame after removing header row: {df.to_dict()}")

                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT column_id, column_name FROM template_columns WHERE template_id = %s AND is_selected = TRUE", 
                              (session['template_id'],))
                column_map = {row['column_name']: row['column_id'] for row in cursor.fetchall()}
                logging.debug(f"Column map: {column_map}")

                cursor.execute("SELECT rule_type_id, rule_name FROM validation_rule_types WHERE is_active = TRUE")
                rule_map = {row['rule_name']: row['rule_type_id'] for row in cursor.fetchall()}
                logging.debug(f"Rule map: {rule_map}")

                cursor.execute("DELETE FROM column_validation_rules WHERE column_id IN (SELECT column_id FROM template_columns WHERE template_id = %s AND is_selected = TRUE)", 
                              (session['template_id'],))

                validation_data = []
                for header in selected_headers:
                    column_id = column_map.get(header)
                    if not column_id:
                        logging.warning(f"No column_id found for header {header}")
                        continue
                    cursor.execute("UPDATE template_columns SET is_validation_enabled = TRUE WHERE column_id = %s", 
                                  (column_id,))
                    for validation in validations.get(header, []):
                        rule_type_id = rule_map.get(validation)
                        if rule_type_id:
                            validation_data.append((column_id, rule_type_id, json.dumps({})))
                        else:
                            logging.warning(f"No rule_type_id found for validation {validation}")
                if validation_data:
                    cursor.executemany("""
                        INSERT INTO column_validation_rules (column_id, rule_type_id, rule_config)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE rule_config = VALUES(rule_config)
                    """, validation_data)
                    logging.debug(f"Inserted validation rules: {validation_data}")
                conn.commit()
                cursor.close()

                action = request.form.get('action', 'review')
                logging.debug(f"Action received: {action}")
                if action == 'save':
                    return jsonify({
                        'success': True,
                        'message': 'Configurations saved successfully'
                    })
                else:
                    return jsonify({
                        'success': True,
                        'message': 'Configurations saved successfully'
                    })
            except Exception as e:
                logging.error(f"Error in Step 2: {str(e)}")
                return jsonify({'error': f'Error in Step 2: {str(e)}'}), 500
        return jsonify({'selected_headers': selected_headers})
    elif step == 3:
        if request.method == 'POST':
            return jsonify({'error': 'Step 3 is read-only for new templates'}), 400
        return jsonify({'selected_headers': session.get('selected_headers', [])})
    return jsonify({'error': 'Invalid step'}), 400

@app.route('/update_template/<int:template_id>', methods=['POST'])
def update_template(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'error': 'Not logged in'}), 401
    data = request.get_json()
    updated_data = data.get('updated_data')
    if not updated_data:
        return jsonify({'error': 'No updated data provided'}), 400
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT template_name
            FROM excel_templates
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template_record = cursor.fetchone()
        if not template_record:
            cursor.close()
            return jsonify({'error': 'Template not found'}), 404
        df = pd.read_json(StringIO(session['df']))
        df.columns = session['headers']
        df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)
        for sheet_name, sheet_data in updated_data['sheets'].items():
            for col, rows in sheet_data['data'].items():
                for row_idx, value in rows.items():
                    df.at[int(row_idx), col] = value
        updated_file_path = os.path.join(app.config['UPLOAD_FOLDER'], template_record['template_name'].replace('.', '_updated.'))
        if updated_file_path.endswith('.xlsx'):
            df.to_excel(updated_file_path, index=False)
        else:
            df.to_csv(updated_file_path, index=False)
        cursor.execute("""
            INSERT INTO excel_templates (template_name, user_id, status, is_corrected)
            VALUES (%s, %s, %s, %s)
        """, (os.path.basename(updated_file_path), session['user_id'], 'ACTIVE', False))
        new_template_id = cursor.lastrowid
        cursor.execute("SELECT column_name, column_position, is_selected FROM template_columns WHERE template_id = %s", (template_id,))
        columns = cursor.fetchall()
        column_data = [(new_template_id, col['column_name'], col['column_position'], col['is_selected']) for col in columns]
        cursor.executemany("""
            INSERT INTO template_columns (template_id, column_name, column_position, is_selected)
            VALUES (%s, %s, %s, %s)
        """, column_data)
        conn.commit()
        cursor.close()
        return jsonify({'success': True, 'file_path': updated_file_path})
    except mysql.connector.Error as e:
        logging.error(f"Database error in update_template: {str(e)}")
        return jsonify({'error': f'Database error: {str(e)}'}), 500

@app.route('/download/<path:filename>')
def download(filename):
    if 'loggedin' not in session:
        logging.warning("Unauthorized access to /download: session missing")
        return jsonify({'error': 'Not logged in'}), 401
    clean_filename = os.path.basename(filename)
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], clean_filename)
    logging.debug(f"Attempting to download file: {file_path} (original filename: {filename})")
    if not os.path.exists(file_path):
        logging.error(f"Download failed: File {file_path} not found")
        available_files = os.listdir(app.config['UPLOAD_FOLDER'])
        logging.debug(f"Available files in {app.config['UPLOAD_FOLDER']}: {available_files}")
        return jsonify({'error': f'File {clean_filename} not found'}), 404
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT template_id
            FROM validation_history
            WHERE corrected_file_path LIKE %s
            LIMIT 1
        """, (f"%{clean_filename}",))
        history = cursor.fetchone()
        if not history:
            cursor.close()
            logging.error(f"No history found for file: {clean_filename}")
            return jsonify({'error': 'No history found for file'}), 404
        template_id = history['template_id']
        # Fetch date formats
        cursor.execute("""
            SELECT column_name, source_format, target_format, rule_name
            FROM validation_rule_types
            WHERE template_id = %s AND (rule_name LIKE 'Date(%%' OR rule_name LIKE 'Transform-Date(%%')
        """, (template_id,))
        date_formats = {}
        for row in cursor.fetchall():
            logging.debug(f"Fetched rule: {row}")
            column_name = row['column_name'].lower()  # Normalize to lowercase for matching
            if row['rule_name'].startswith('Date('):
                date_formats[column_name] = {'source_format': row['source_format']}
            elif row['rule_name'].startswith('Transform-Date('):
                if column_name in date_formats:
                    date_formats[column_name]['target_format'] = row['target_format']
                else:
                    date_formats[column_name] = {'target_format': row['target_format']}
        logging.debug(f"Date formats for transformation: {date_formats}")
        # Read the file
        if clean_filename.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif clean_filename.endswith(('.csv', '.txt', '.dat')):
            df = pd.read_csv(file_path)
        else:
            cursor.close()
            logging.error(f"Unsupported file type: {clean_filename}")
            return jsonify({'error': 'Unsupported file type'}), 400
        # Normalize DataFrame column names to lowercase for matching
        df.columns = [col.lower() for col in df.columns]
        logging.debug(f"DataFrame columns: {list(df.columns)}")
        # Apply date transformations
        for column, formats in date_formats.items():
            if column in df.columns and 'target_format' in formats and 'source_format' in formats:
                logging.debug(f"Applying transformation to column {column}: {formats['source_format']} -> {formats['target_format']}")
                df[column] = df[column].apply(lambda x: transform_date(x, formats['source_format'], formats['target_format']))
                logging.debug(f"Transformed column {column}: {df[column].head().to_list()}")
            else:
                logging.warning(f"Skipping transformation for column {column}: not found in DataFrame or missing formats")
        # Save transformed file temporarily
        temp_filename = f"transformed_{clean_filename}"
        temp_file_path = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
        logging.debug(f"Saving transformed file to: {temp_file_path}")
        if clean_filename.endswith('.xlsx'):
            df.to_excel(temp_file_path, index=False)
        else:
            df.to_csv(temp_file_path, index=False)
        download_name = clean_filename
        logging.info(f"Sending file for download: {temp_file_path} as {download_name}")
        cursor.close()
        return send_file(temp_file_path, as_attachment=True, download_name=download_name)
    except Exception as e:
        logging.error(f"Error downloading file {file_path}: {str(e)}")
        return jsonify({'error': f'Error downloading file: {str(e)}'}), 500
    
import re
import logging
import pandas as pd
import numpy as np
import numexpr
from typing import Dict, Tuple, List

def parse_formula(formula: str, columns: List[str]) -> Dict:
    """
    Parse a formula string and return a dict with target, operator, expression, and operands.
    Supports arithmetic (e.g., "'Sum' = 'Number1' + 'Number2'") and comparison (e.g., "'Comparison' <= 'Number2'") formulas.
    The '=' operator is treated as arithmetic only.
    
    Args:
        formula (str): The formula string (e.g., "'Sum' = 'Number1' + 'Number2'" or "'Comparison' <= 'Number2'")
        columns (List[str]): List of valid column names in the dataset
    
    Returns:
        Dict: Parsed formula with keys 'target', 'operator', 'expression', 'right', 'operands', 'is_arithmetic'
    
    Raises:
        ValueError: If the formula is invalid or contains unknown columns
    """
    # Normalize formula: replace double quotes with single quotes and strip whitespace
    formula = formula.replace('"', "'").strip()
    logging.debug(f"Parsing formula: {formula}")

    # Create a case-insensitive mapping of column names
    columns_lower = {col.lower(): col for col in columns}
    
    # Define supported operators
    comparison_ops = ['>=', '<=', '>', '<']  # Removed '=' from comparison operators
    arithmetic_ops = ['+', '-', '*', '/', '%', 'AND', 'OR']
    
    # Initialize parsed result
    parsed = {
        'target': None,
        'operator': None,
        'expression': None,
        'right': None,
        'operands': [],
        'is_arithmetic': False
    }

    # Check for comparison formula
    for op in comparison_ops:
        if f" {op} " in formula:
            parts = formula.split(f" {op} ", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid comparison formula format: {formula}")
            
            target, right = parts[0].strip(), parts[1].strip()
            # Remove quotes from target and right
            target = target.strip("'").strip()
            right = right.strip("'").strip()
            
            # Validate target column
            if target.lower() not in columns_lower:
                raise ValueError(f"Target column '{target}' not found in dataset")
            
            parsed['target'] = columns_lower[target.lower()]
            parsed['operator'] = op
            parsed['right'] = right
            # Determine if right is a column or a constant
            if right.lower() in columns_lower:
                parsed['operands'] = [columns_lower[right.lower()]]
            else:
                try:
                    float(right)  # Validate if right is a numeric constant
                    parsed['operands'] = []
                except ValueError:
                    raise ValueError(f"Right operand '{right}' is neither a valid column nor a number")
            return parsed
    
    # Check for arithmetic formula
    if ' = ' in formula:
        target, expression = formula.split(' = ', 1)
        target = target.strip("'").strip()
        expression = expression.strip()
        
        if target.lower() not in columns_lower:
            raise ValueError(f"Target column '{target}' not found in dataset")
        
        # Extract operands using regex for quoted column names
        operand_pattern = r"'[\w\s]+'"  # Matches quoted column names
        operands = re.findall(operand_pattern, expression)
        operands = [op.strip("'").strip() for op in operands]
        
        # Validate operands
        for op in operands:
            if op.lower() not in columns_lower:
                raise ValueError(f"Operand column '{op}' not found in dataset")
        
        # Prepare expression for numexpr
        expr = expression
        for op in operands:
            var_name = f"var_{columns_lower[op.lower()].replace(' ', '_')}"
            expr = re.sub(rf"'{re.escape(op)}'", var_name, expr)
        
        parsed['target'] = columns_lower[target.lower()]
        parsed['operator'] = '='
        parsed['expression'] = expr
        parsed['operands'] = [columns_lower[op.lower()] for op in operands]
        parsed['is_arithmetic'] = True
        return parsed
    
    raise ValueError("Formula must contain a comparison (>, <, >=, <=) or arithmetic (=) operator")

def apply_and_validate_formula(df: pd.DataFrame, parsed_formula: Dict, data_type: str, column_name: str, headers: List[str]) -> Tuple[pd.DataFrame, List[Tuple[int, str, str, str]]]:
    """
    Apply formula-based and standard rule validation, supporting both arithmetic and comparison formulas.
    Updates the DataFrame with 'Result' and 'Reason' columns and returns error locations.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data
        parsed_formula (Dict): Parsed formula from parse_formula
        data_type (str): Expected data type (e.g., 'Int', 'Float')
        column_name (str): Target column name for validation
        headers (List[str]): List of DataFrame headers
    
    Returns:
        Tuple[pd.DataFrame, List[Tuple[int, str, str, str]]]: Updated DataFrame and list of error locations
        where error_locations is [(row_index, value, rule_failed, reason), ...]
    """
    # Normalize headers and column names
    headers_map = {h.lower(): h for h in headers}
    target = headers_map.get(parsed_formula['target'].lower(), parsed_formula['target'])
    operator = parsed_formula['operator']
    right = parsed_formula.get('right')
    expression = parsed_formula.get('expression')
    operands = parsed_formula.get('operands', [])
    is_arithmetic = parsed_formula.get('is_arithmetic', False)
    
    # Initialize Result and Reason columns
    if 'Result' not in df.columns:
        df['Result'] = 'PASS'
    if 'Reason' not in df.columns:
        df['Reason'] = ''
    
    error_locations = []
    
    # Handle arithmetic formula
    if is_arithmetic and expression and operands:
        try:
            # Convert relevant columns to numeric
            for col in operands + [target]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                else:
                    logging.error(f"Column '{col}' not found in DataFrame")
                    error_locations.append((0, "", f"{column_name}_Formula", f"Column '{col}' not found"))
                    df['Result'] = 'FAILED'
                    df['Reason'] = f"Column '{col}' not found"
                    return df, error_locations
            
            # Prepare variables for numexpr
            local_dict = {f"var_{col.replace(' ', '_')}": df[col].astype(float) for col in operands}
            
            # Evaluate the formula
            expected_result = numexpr.evaluate(expression, local_dict=local_dict)
            
            # Compare actual vs expected values
            for i in range(len(df)):
                actual = df.at[i, target]
                expected = expected_result[i]
                if pd.isna(actual) or pd.isna(expected):
                    df.at[i, 'Result'] = 'FAILED'
                    df.at[i, 'Reason'] = f"Data Error: Missing or non-numeric value in {target} or operands"
                    error_locations.append((i + 1, str(actual), f"{column_name}_Formula", df.at[i, 'Reason']))
                elif not np.isclose(float(actual), float(expected), rtol=1e-5, equal_nan=False):
                    df.at[i, 'Result'] = 'FAILED'
                    df.at[i, 'Reason'] = f"Data Error: {target} ({actual}) does not match formula {expression} ({expected})"
                    error_locations.append((i + 1, str(actual), f"{column_name}_Formula", df.at[i, 'Reason']))
        except Exception as e:
            logging.error(f"Error evaluating arithmetic formula for {target}: {str(e)}")
            error_locations.append((0, "", f"{column_name}_Formula", f"Formula evaluation failed: {str(e)}"))
            df['Result'] = 'FAILED'
            df['Reason'] = f"Formula evaluation failed: {str(e)}"
            return df, error_locations
    
    # Handle comparison formula
    elif operator in ['>=', '<=', '>', '<'] and right is not None:
        import operator as pyop
        op_map = {
            '>=': pyop.ge,
            '<=': pyop.le,
            '>': pyop.gt,
            '<': pyop.lt
        }
        cmp_func = op_map[operator]
        
        # Compare to another column
        if right in headers_map.values():
            right_col = right
            if right_col not in df.columns:
                logging.error(f"Right column '{right_col}' not found in DataFrame")
                error_locations.append((0, "", f"{column_name}_Formula", f"Right column '{right_col}' not found"))
                df['Result'] = 'FAILED'
                df['Reason'] = f"Right column '{right_col}' not found"
                return df, error_locations
            
            left_vals = pd.to_numeric(df[target], errors='coerce')
            right_vals = pd.to_numeric(df[right_col], errors='coerce')
            
            for i in range(len(df)):
                left_val = left_vals[i]
                right_val = right_vals[i]
                if pd.isna(left_val) or pd.isna(right_val):
                    df.at[i, 'Result'] = 'FAILED'
                    df.at[i, 'Reason'] = f"Data Error: Missing or non-numeric value in {target} or {right_col}"
                    error_locations.append((i + 1, str(left_val), f"{column_name}_Formula", df.at[i, 'Reason']))
                elif not cmp_func(float(left_val), float(right_val)):
                    df.at[i, 'Result'] = 'FAILED'
                    df.at[i, 'Reason'] = f"Failed comparison: {target} ({left_val}) {operator} {right_col} ({right_val})"
                    error_locations.append((i + 1, str(left_val), f"{column_name}_Formula", df.at[i, 'Reason']))
        
        # Compare to a constant
        else:
            try:
                right_val = float(right)
            except ValueError:
                logging.error(f"Invalid constant in comparison: {right}")
                error_locations.append((0, "", f"{column_name}_Formula", f"Invalid constant '{right}'"))
                df['Result'] = 'FAILED'
                df['Reason'] = f"Invalid constant '{right}'"
                return df, error_locations
            
            left_vals = pd.to_numeric(df[target], errors='coerce')
            for i in range(len(df)):
                left_val = left_vals[i]
                if pd.isna(left_val):
                    df.at[i, 'Result'] = 'FAILED'
                    df.at[i, 'Reason'] = f"Data Error: Missing or non-numeric value in {target}"
                    error_locations.append((i + 1, str(left_val), f"{column_name}_Formula", df.at[i, 'Reason']))
                elif not cmp_func(float(left_val), right_val):
                    df.at[i, 'Result'] = 'FAILED'
                    df.at[i, 'Reason'] = f"Failed comparison: {target} ({left_val}) {operator} {right_val}"
                    error_locations.append((i + 1, str(left_val), f"{column_name}_Formula", df.at[i, 'Reason']))
    
    # Apply standard rule validation
    from app import check_special_characters_in_column
    error_count, type_error_locations = check_special_characters_in_column(
        df, target, data_type, ['%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y'], True
    )
    for error in type_error_locations:
        row_index = error[0] - 1  # Convert to 0-based index
        if 0 <= row_index < len(df):
            df.at[row_index, 'Result'] = 'FAILED'
            df.at[row_index, 'Reason'] = error[3] if len(error) > 3 else f"Failed {data_type} validation"
            error_locations.append(error)
    
    return df, error_locations

@app.route('/validate-corrected/<int:template_id>', methods=['POST'])
def validate_corrected_template(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /validate-corrected: session missing")
        return jsonify({'success': False, 'message': 'User not authenticated. Please log in and try again.'}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Fetch template details
        cursor.execute("""
            SELECT template_name, sheet_name, headers, remote_file_path
            FROM excel_templates
            WHERE template_id = %s AND user_id = %s AND status = 'ACTIVE'
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            logging.error(f"Template not found for template_id: {template_id}, user_id: {session['user_id']}")
            return jsonify({'success': False, 'message': 'Template not found or not accessible for this user'}), 404

        headers = json.loads(template['headers']) if template['headers'] else []
        if not headers:
            cursor.close()
            logging.error(f"No headers found for template_id: {template_id}")
            return jsonify({'success': False, 'message': 'No headers defined for this template'}), 400
        sheet_name = template['sheet_name'] or 'Sheet1'

        # Fetch corrected file path from session or validation history
        file_path = session.get('corrected_file_path')
        history_id = None
        if not file_path:
            cursor.execute("""
                SELECT history_id, corrected_file_path
                FROM validation_history
                WHERE template_id = %s AND user_id = %s
                ORDER BY corrected_at DESC
                LIMIT 1
            """, (template_id, session['user_id']))
            history_entry = cursor.fetchone()
            if not history_entry:
                cursor.close()
                logging.error(f"No validation history found for template_id: {template_id}, user_id: {session['user_id']}")
                return jsonify({'success': False, 'message': 'No validation history found. Please complete error correction in Step 5.'}), 404
            file_path = history_entry['corrected_file_path']
            history_id = history_entry['history_id']

        # Check if file exists; if not, regenerate from session corrections
        if not os.path.exists(file_path):
            logging.warning(f"Corrected file not found at: {file_path}. Attempting to regenerate.")
            corrections = session.get('corrections', {})
            if not corrections:
                cursor.close()
                logging.error(f"No corrections found in session for template_id: {template_id}")
                return jsonify({
                    'success': False,
                    'message': 'Corrected file not found and no corrections available in session. Please reapply corrections in Step 5.'
                }), 400

            if 'df' not in session or 'header_row' not in session:
                cursor.close()
                logging.error(f"Session missing df or header_row for template_id: {template_id}")
                return jsonify({
                    'success': False,
                    'message': 'Session data missing (df or header_row). Please restart the validation process.'
                }), 400

            df = pd.read_json(StringIO(session['df']))
            df.columns = headers
            df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)

            # Apply corrections
            for header, rows in corrections.items():
                if header not in headers:
                    logging.warning(f"Invalid header in corrections: {header}")
                    continue
                for row_idx, value in rows.items():
                    try:
                        row_idx_int = int(row_idx)
                        if 0 <= row_idx_int < len(df):
                            df.at[row_idx_int, header] = value
                        else:
                            logging.warning(f"Invalid row index {row_idx} for header {header}")
                    except ValueError:
                        logging.warning(f"Invalid row index format {row_idx} for header {header}")

            # Save regenerated corrected file
            base_name, ext = os.path.splitext(template['template_name'])
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{base_name}_corrected{ext}")
            try:
                if ext == '.xlsx':
                    df.to_excel(file_path, index=False)
                else:
                    df.to_csv(file_path, index=False)
                logging.info(f"Regenerated corrected file at: {file_path}")

                # Update validation history
                if history_id:
                    cursor.execute("""
                        UPDATE validation_history
                        SET corrected_file_path = %s
                        WHERE history_id = %s AND user_id = %s
                    """, (file_path, history_id, session['user_id']))
                    conn.commit()
            except Exception as e:
                cursor.close()
                logging.error(f"Failed to save regenerated corrected file: {str(e)}")
                return jsonify({'success': False, 'message': f'Failed to regenerate corrected file: {str(e)}'}), 500

        # Read the corrected file
        sheets = read_file(file_path)
        if sheet_name not in sheets:
            cursor.close()
            logging.error(f"Sheet {sheet_name} not found in corrected file: {file_path}")
            return jsonify({'success': False, 'message': f'Sheet {sheet_name} not found in corrected file'}), 400
        df = sheets[sheet_name]
        header_row = find_header_row(df)
        if header_row == -1:
            cursor.close()
            logging.error(f"Could not detect header row in corrected file: {file_path}")
            return jsonify({'success': False, 'message': 'Could not detect header row in corrected file'}), 400
        df.columns = headers
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        # Fetch all rules from validation_rule_types
        cursor.execute("""
            SELECT rule_name, parameters, column_name, is_custom, is_active
            FROM validation_rule_types
            WHERE template_id = %s AND ((is_custom = FALSE) OR (is_custom = TRUE AND is_active = TRUE))
        """, (template_id,))
        rules = cursor.fetchall()
        if not rules:
            cursor.close()
            logging.warning(f"No rules found for template_id: {template_id}")
            return jsonify({
                'success': False,
                'message': 'No validation rules configured for this template. Please configure rules in Rule Configurations.'
            }), 400

        df['Result'] = 'PASS'
        reason_lists = [[] for _ in range(len(df))]
        validation_results = []

        for rule in rules:
            try:
                error_locations = []
                if rule['is_custom']:
                    # Only process if custom rule is active
                    if not rule['is_active']:
                        continue
                    # Replace old calls with this; assume data_type from rule or default to 'Int'
                    data_type = 'Int'  # Or fetch from column_validation_rules if stored
                    _, error_locations = evaluate_column_rule(df, rule['column_name'], rule['parameters'], headers, data_type)
                else:
                    # Existing generic call (always active)
                    error_count, error_locations = check_special_characters_in_column(df, rule['column_name'], rule['rule_name'], ['%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y'], True)
                
                for error in error_locations:
                    if len(error) > 3:  # Ensure reason exists
                        row_index = error[0] - 1
                        if 0 <= row_index < len(df):
                            df.at[row_index, 'Result'] = 'FAILED'
                            reason = error[3]
                            reason_lists[row_index].append(f"{rule['rule_name']}: {reason}")
            except Exception as e:
                logging.error(f"Error validating rule {rule['rule_name']}: {str(e)}")
                # Optionally add a fallback reason
                reason_lists.append(f"Validation error for {rule['rule_name']}: {str(e)}")

        # After all rules, join reasons for each row into a string
        df['Reason'] = ['; '.join(reasons) if reasons else '' for reasons in reason_lists]

        # Store validation results in session for report generation
        session['validation_results'] = {
            'template_id': template_id,
            'file_path': file_path,
            'results': validation_results,
            'headers': headers,
            'corrected_df': df.to_json()
        }

        cursor.close()
        logging.info(f"Validation completed for template_id: {template_id}, file: {file_path}, results: {len(validation_results)} rules processed")
        return jsonify({
            'success': True,
            'message': 'Validation of corrected template completed successfully',
            'results': validation_results
        })
    except mysql.connector.Error as db_err:
        logging.error(f"Database error validating corrected template: {str(db_err)}")
        return jsonify({'success': False, 'message': f'Database error: {str(db_err)}'}), 500
    except Exception as e:
        logging.error(f"Unexpected error validating corrected template: {str(e)}")
        return jsonify({'success': False, 'message': f'Unexpected error during validation: {str(e)}'}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


@app.route('/download-validation-report/<int:template_id>', methods=['GET'])
def download_validation_report(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /download-validation-report: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        validation_results = session.get('validation_results', {})
        if not validation_results or validation_results['template_id'] != template_id:
            logging.error(f"No validation results found in session for template_id: {template_id}")
            return jsonify({'success': False, 'message': 'No validation results available. Please validate the corrected template first.'}), 400

        file_path = validation_results['file_path']
        headers = validation_results['headers']

        # Load corrected DataFrame from session
        if 'corrected_df' not in validation_results:
            logging.error(f"No corrected_df found in session for template_id: {template_id}")
            return jsonify({'success': False, 'message': 'Corrected data not available in session. Please revalidate the template.'}), 400

        df = pd.read_json(StringIO(validation_results['corrected_df']))

        # Ensure Result and Reason columns are included
        if 'Result' not in df.columns or 'Reason' not in df.columns:
            logging.error(f"Result or Reason columns missing in corrected_df for template_id: {template_id}")
            return jsonify({'success': False, 'message': 'Validation results incomplete. Please revalidate the template.'}), 400

        # Save report to a new file
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT template_name
            FROM excel_templates
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404

        # --- Apply date transformation logic as in /download ---
        # Fetch date formats for this template
        cursor.execute("""
            SELECT column_name, source_format, target_format, rule_name
            FROM validation_rule_types
            WHERE template_id = %s AND (rule_name LIKE 'Date(%%' OR rule_name LIKE 'Transform-Date(%%')
        """, (template_id,))
        date_formats = {}
        for row in cursor.fetchall():
            column_name = row['column_name'].lower()
            if row['rule_name'].startswith('Date('):
                if column_name not in date_formats:
                    date_formats[column_name] = {}
                date_formats[column_name]['source_format'] = row['source_format']
            elif row['rule_name'].startswith('Transform-Date('):
                if column_name not in date_formats:
                    date_formats[column_name] = {}
                date_formats[column_name]['target_format'] = row['target_format']

        # --- Fix: Normalize columns for transformation, then restore original names ---
        original_columns = list(df.columns)
        lower_map = {col.lower(): col for col in original_columns}
        df.columns = [col.lower() for col in df.columns]

        # Apply date transformations (always use transform_date and ensure string output)
        for column, formats in date_formats.items():
            if column in df.columns and 'target_format' in formats and 'source_format' in formats:
                try:
                    df[column] = df[column].apply(
                        lambda x: transform_date(x, formats['source_format'], formats['target_format'])
                    )
                    # Ensure all values are strings (important for Excel export)
                    df[column] = df[column].astype(str)
                except Exception as e:
                    logging.error(f"Date transformation failed for column {column}: {str(e)}")
            # else: skip if formats not available

        # Restore original column names (preserve order)
        df.columns = [lower_map.get(col, col) for col in df.columns]

        cursor.close()

        base_name, ext = os.path.splitext(template['template_name'])
        report_filename = f"{base_name}_validation_report.xlsx"
        report_filepath = os.path.join(app.config['UPLOAD_FOLDER'], report_filename)
        df.to_excel(report_filepath, index=False)

        logging.info(f"Generated validation report at: {report_filepath}")
        return send_file(report_filepath, as_attachment=True, download_name=report_filename)
    except Exception as e:
        logging.error(f"Error generating validation report: {str(e)}")
        return jsonify({'success': False, 'message': f'Error generating validation report: {str(e)}'}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
    
            
                
@app.route('/logout', methods=['POST'])
def logout():
    logging.info(f"User {session.get('user_email', 'unknown')} logged out. Session before logout: {dict(session)}")
    session.clear()
    logging.info(f"Session after logout: {dict(session)}")
    return jsonify({'success': True, 'message': 'Logged out successfully'})
@app.route('/connect-sftp', methods=['POST'])
def connect_sftp():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /connect-sftp: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        data = request.get_json()
        if not data:
            logging.warning("No JSON data provided in SFTP connection request")
            return jsonify({'success': False, 'message': 'No data provided'}), 400

        hostname = data.get('hostname')
        username = data.get('username')
        password = data.get('password')
        port = int(data.get('port', 22))
        path = data.get('path', '').strip()

        if not all([hostname, username, password]):
            logging.warning("Missing required fields in SFTP connection request")
            return jsonify({'success': False, 'message': 'Hostname, username, and password are required'}), 400

        logging.debug(f"Attempting SFTP connection to {hostname}:{port} as {username}, path: {path}")

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=hostname,
                port=port,
                username=username,
                password=password,
                timeout=10,
                allow_agent=False,
                look_for_keys=False
            )
            sftp = client.open_sftp()
            try:
                # Validate the path (default to current directory if path is empty)
                sftp.listdir(path or '.')
                logging.info(f"Successfully connected to SFTP server {hostname}:{port} at path {path or '.'}")
                return jsonify({'success': True, 'message': f'SFTP connection successful to path {path or "."}'}), 200
            except IOError as io_err:
                logging.error(f"Invalid path {path} on SFTP server: {str(io_err)}")
                return jsonify({'success': False, 'message': f'Invalid path: {str(io_err)}'}), 400
            except Exception as sftp_err:
                logging.error(f"SFTP operation failed: {str(sftp_err)}")
                return jsonify({'success': False, 'message': f'SFTP operation failed: {str(sftp_err)}'}), 500
            finally:
                sftp.close()
        except paramiko.AuthenticationException as auth_err:
            logging.error(f"Authentication failed for {username}@{hostname}:{port}: {str(auth_err)}")
            return jsonify({'success': False, 'message': 'Authentication failed: Invalid credentials'}), 401
        except paramiko.SSHException as ssh_err:
            logging.error(f"SSH error connecting to {hostname}:{port}: {str(ssh_err)}")
            return jsonify({'success': False, 'message': f'SSH connection failed: {str(ssh_err)}'}), 500
        except Exception as conn_err:
            logging.error(f"Failed to connect to SFTP server {hostname}:{port}: {str(conn_err)}")
            return jsonify({'success': False, 'message': f'Failed to connect to SFTP server: {str(conn_err)}'}), 500
        finally:
            client.close()
    except ValueError as ve:
        logging.error(f"Invalid port value: {str(ve)}")
        return jsonify({'success': False, 'message': f'Invalid port: {str(ve)}'}), 400
    except Exception as e:
        logging.error(f"Unexpected error in connect-sftp: {str(e)}")
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500

@app.route('/sftp-fetch', methods=['POST'])
def fetch_from_sftp():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /sftp-fetch: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        hostname = request.form.get('hostname')
        username = request.form.get('username')
        password = request.form.get('password')
        remote_file_path = request.form.get('remote_file_path', '').strip()

        if not all([hostname, username, password, remote_file_path]):
            logging.warning("Missing required fields in SFTP fetch request")
            return jsonify({'success': False, 'message': 'Hostname, username, password, and remote file path are required'}), 400

        logging.debug(f"Fetching file from SFTP: {remote_file_path} on {hostname} as {username}")

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=hostname,
                port=22,
                username=username,
                password=password,
                timeout=10,
                allow_agent=False,
                look_for_keys=False
            )
            sftp = client.open_sftp()
            try:
                os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
                
                filename = os.path.basename(remote_file_path)
                if not filename:
                    logging.error("Invalid remote file path: no filename provided")
                    return jsonify({'success': False, 'message': 'Invalid remote file path'}), 400
                
                local_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                
                sftp.get(remote_file_path, local_file_path)
                logging.info(f"Successfully downloaded file {remote_file_path} to {local_file_path}")

                sheets = read_file(local_file_path)
                sheet_names = list(sheets.keys())
                if not sheet_names:
                    logging.error("No sheets found in the file")
                    return jsonify({'success': False, 'message': 'No sheets found in the file'}), 400
                
                sheet_name = sheet_names[0]
                df = sheets[sheet_name]
                header_row = find_header_row(df)
                if header_row == -1:
                    logging.warning(f"Could not detect header row in file {filename}")
                    return jsonify({'success': False, 'message': 'Could not detect header row'}), 400
                
                headers = df.iloc[header_row].tolist()
                if not headers or all(not h for h in headers):
                    logging.error("No valid headers found in file")
                    return jsonify({'success': False, 'message': 'No valid headers found in the file'}), 400

                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                
                cursor.execute("""
                    SELECT template_id, headers, sheet_name, remote_file_path
                    FROM excel_templates
                    WHERE template_name = %s AND user_id = %s AND status = 'ACTIVE'
                    ORDER BY created_at DESC
                """, (filename, session['user_id']))
                existing_templates = cursor.fetchall()

                template_id = None
                has_existing_rules = False
                validations = {}
                skip_to_step_3 = False

                matching_template = None
                for template in existing_templates:
                    stored_headers = json.loads(template['headers']) if template['headers'] else []
                    stored_sheet_name = template['sheet_name']
                    if stored_headers == headers and stored_sheet_name == sheet_name:
                        matching_template = template
                        break

                if matching_template:
                    template_id = matching_template['template_id']
                    # Update remote_file_path if changed
                    cursor.execute("""
                        UPDATE excel_templates
                        SET remote_file_path = %s
                        WHERE template_id = %s
                    """, (remote_file_path, template_id))
                    cursor.execute("""
                        SELECT tc.column_name, vrt.rule_name
                        FROM template_columns tc
                        JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                        JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
                        WHERE tc.template_id = %s AND tc.is_selected = TRUE
                    """, (template_id,))
                    rules_data = cursor.fetchall()
                    for row in rules_data:
                        column_name = row['column_name']
                        rule_name = row['rule_name']
                        if column_name not in validations:
                            validations[column_name] = []
                        validations[column_name].append(rule_name)
                    has_existing_rules = len(validations) > 0
                    skip_to_step_3 = has_existing_rules
                else:
                    cursor.execute("""
                        INSERT INTO excel_templates (template_name, user_id, sheet_name, headers, is_corrected, remote_file_path)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (filename, session['user_id'], sheet_name, json.dumps(headers), False, remote_file_path))
                    template_id = cursor.lastrowid
                    column_data = [(template_id, header, i + 1, False) for i, header in enumerate(headers)]
                    cursor.executemany("""
                        INSERT INTO template_columns (template_id, column_name, column_position, is_selected)
                        VALUES (%s, %s, %s, %s)
                    """, column_data)

                conn.commit()
                cursor.close()

                session['file_path'] = local_file_path
                session['template_id'] = template_id
                session['df'] = df.to_json()
                session['header_row'] = header_row
                session['headers'] = headers
                session['sheet_name'] = sheet_name
                session['current_step'] = 1
                session['validations'] = validations
                session['has_existing_rules'] = has_existing_rules

                return jsonify({
                    'success': True,
                    'sheets': {sheet_name: {'headers': headers}},
                    'file_name': filename,
                    'template_id': template_id,
                    'has_existing_rules': has_existing_rules,
                    'sheet_name': sheet_name,
                    'skip_to_step_3': skip_to_step_3
                })
            except IOError as io_err:
                logging.error(f"Failed to fetch file {remote_file_path}: {str(io_err)}")
                return jsonify({'success': False, 'message': f'File not found or inaccessible: {str(io_err)}'}), 400
            except Exception as sftp_err:
                logging.error(f"SFTP operation failed: {str(sftp_err)}")
                return jsonify({'success': False, 'message': f'SFTP operation failed: {str(sftp_err)}'}), 500
            finally:
                sftp.close()
        except paramiko.AuthenticationException as auth_err:
            logging.error(f"Authentication failed for {username}@{hostname}: {str(auth_err)}")
            return jsonify({'success': False, 'message': 'Authentication failed: Invalid credentials'}), 401
        except paramiko.SSHException as ssh_err:
            logging.error(f"SSH error connecting to {hostname}: {str(ssh_err)}")
            return jsonify({'success': False, 'message': f'SSH connection failed: {str(ssh_err)}'}), 500
        except Exception as conn_err:
            logging.error(f"Failed to connect to SFTP server {hostname}: {str(conn_err)}")
            return jsonify({'success': False, 'message': f'Failed to connect to SFTP server: {str(conn_err)}'}), 500
        finally:
            client.close()
    except Exception as e:
        logging.error(f"Unexpected error in sftp-fetch: {str(e)}")
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500
    
@app.route('/rules', methods=['GET'])
def get_rules():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Debug: Get all rules first to see what's in the database
        cursor.execute("""
            SELECT rule_type_id AS rule_id, rule_name, description, parameters, is_custom, 
                   column_name, template_id, source_format, target_format, data_type, is_active
            FROM validation_rule_types
        """)
        all_rules = cursor.fetchall()
        logging.debug(f"All rules in database: {all_rules}")
        
        # Filter only active rules (but for debugging, let's see all rules first)
        cursor.execute("""
            SELECT rule_type_id AS rule_id, rule_name, description, parameters, is_custom, 
                   column_name, template_id, source_format, target_format, data_type, is_active
            FROM validation_rule_types
        """)
        rules = cursor.fetchall()
        
        cursor.close()
        
        logging.info(f"Returning {len(rules)} rules from getRules endpoint")
        logging.debug(f"Rules data: {rules}")
        
        return jsonify({'success': True, 'rules': rules})
    except mysql.connector.Error as e:
        logging.error(f"Database error fetching rules: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500
    except Exception as e:
        logging.error(f"Unexpected error fetching rules: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/rules', methods=['POST'])
def create_rule():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /rules: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    data = request.get_json()
    logging.debug(f"Received payload for /rules: {data}")
    
    rule_name = data.get('rule_name')
    description = data.get('description', '')
    parameters = data.get('parameters')
    column_name = data.get('column_name')
    template_id = data.get('template_id')
    source_format = data.get('source_format')
    target_format = data.get('target_format')

    # Basic validation
    missing_fields = []
    if not rule_name:
        missing_fields.append('rule_name')
    if not column_name:
        missing_fields.append('column_name')
    if not template_id:
        missing_fields.append('template_id')
    if not parameters:
        missing_fields.append('parameters')
    
    if missing_fields:
        logging.error(f"Missing required fields: {', '.join(missing_fields)}")
        return jsonify({'success': False, 'message': f"Missing required fields: {', '.join(missing_fields)}"}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Verify template exists and user has access
        cursor.execute("""
            SELECT headers, template_name
            FROM excel_templates
            WHERE template_id = %s AND user_id = %s AND status = 'ACTIVE'
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        
        if not template:
            cursor.close()
            logging.error(f"Template not found: template_id={template_id}, user_id={session['user_id']}")
            return jsonify({'success': False, 'message': 'Template not found'}), 404

        logging.info(f"Creating rule for template: {template['template_name']} (ID: {template_id})")

        # Validate column exists
        headers = json.loads(template['headers']) if template['headers'] else []
        headers_lower = [h.strip().lower() for h in headers]
        if column_name.lower() not in headers_lower:
            cursor.close()
            logging.error(f"Invalid column name: {column_name}, available headers: {headers}")
            return jsonify({'success': False, 'message': f"Invalid column name: {column_name}"}), 400

        # NEW: Append random number to Date and Transform-Date rules
        if rule_name.startswith('Date(') or rule_name.startswith('Transform-Date('):
            import random
            random_number = random.randint(100000, 999999)  # 6-digit random number
            rule_name = f"{rule_name}-{random_number}"
            logging.info(f"Appended random number to rule name: {rule_name}")

        # Determine rule properties
        is_date_rule = rule_name.startswith('Date(')
        is_transform_rule = rule_name.startswith('Transform-Date(')
        is_custom = not (is_date_rule or is_transform_rule)
        data_type = 'Date' if (is_date_rule or is_transform_rule) else None
        
        # Custom rules start as inactive, others start as active
        initial_is_active = False if is_custom else True
        
        logging.info(f"Rule properties: is_custom={is_custom}, is_active={initial_is_active}")

        # Insert the rule
        insert_query = """
            INSERT INTO validation_rule_types 
            (rule_name, description, parameters, column_name, template_id, data_type, 
             source_format, target_format, is_active, is_custom)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        insert_values = (
            rule_name, description, parameters, column_name, template_id, 
            data_type, source_format, target_format, initial_is_active, is_custom
        )
        
        logging.debug(f"Executing insert query: {insert_query}")
        logging.debug(f"Insert values: {insert_values}")
        
        cursor.execute(insert_query, insert_values)
        rule_id = cursor.lastrowid
        
        conn.commit()
        
        # Verify the rule was inserted
        cursor.execute("""
            SELECT rule_type_id, rule_name, is_custom, is_active, template_id, column_name
            FROM validation_rule_types
            WHERE rule_type_id = %s
        """, (rule_id,))
        created_rule = cursor.fetchone()
        
        cursor.close()
        
        logging.info(f"Successfully created rule: {created_rule}")
        
        return jsonify({
            'success': True, 
            'message': 'Rule created successfully',
            'rule': {
                'rule_id': rule_id,
                'rule_name': rule_name,
                'is_active': initial_is_active,
                'is_custom': is_custom,
                'template_id': template_id,
                'column_name': column_name,
                'parameters': parameters
            }
        })
        
    except mysql.connector.Error as db_err:
        logging.error(f"Database error creating rule: {str(db_err)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.rollback()
        return jsonify({'success': False, 'message': f'Database error: {str(db_err)}'}), 500
    except Exception as e:
        logging.error(f"Unexpected error creating rule: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.rollback()
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500

@app.route('/rules/<int:rule_id>', methods=['PUT'])
def update_rule(rule_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /rules: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        data = request.get_json()
        logging.debug(f"Received payload for /rules/{rule_id}: {data}")
        rule_name = data.get('rule_name')
        description = data.get('description', '')
        parameters = data.get('parameters')
        column_name = data.get('column_name')
        template_id = data.get('template_id')

        missing_fields = []
        if not rule_name:
            missing_fields.append('rule_name')
        if not parameters:
            missing_fields.append('parameters')
        if not column_name:
            missing_fields.append('column_name')
        if not template_id:
            missing_fields.append('template_id')
        if missing_fields:
            logging.error(f"Missing required fields: {', '.join(missing_fields)}")
            return jsonify({'success': False, 'message': f"Missing required fields: {', '.join(missing_fields)}"}), 400

        # Normalize column_name
        column_name = column_name.strip().lower()
        logging.debug(f"Normalized column_name: {column_name}")

        # Validate formula structure
        logging.debug(f"Validating formula: {parameters}")
        if not parameters.startswith(f"'{column_name}'"):
            logging.error(f"Invalid formula format: {parameters}")
            return jsonify({'success': False, 'message': "Formula must start with 'column_name'"}), 400

        # Check if the formula is arithmetic/logical (contains '=') or comparison
        is_arithmetic = ' = ' in parameters
        logging.debug(f"Is arithmetic formula: {is_arithmetic}")

        if is_arithmetic:
            # Validate arithmetic/logical formula: 'column_name = expression'
            formula_parts = parameters.strip().split(' = ', 1)
            if len(formula_parts) != 2 or formula_parts[0] != f"'{column_name}'":
                logging.error(f"Invalid arithmetic/logical formula format: {parameters}")
                return jsonify({'success': False, 'message': "Arithmetic/logical formula must be 'column_name = expression'"}), 400
            dragged_formula = formula_parts[1].split(' ')
            # Ensure at least one column and one arithmetic/logical operator
            arithmetic_logical_operators = ['+', '-', '/', '%', '*', 'AND', 'OR']
            has_column = any(item.startswith("'") and item.endswith("'") for item in dragged_formula)
            has_operator = any(item in arithmetic_logical_operators for item in dragged_formula)
            if not (has_column and has_operator):
                logging.error(f"Invalid arithmetic/logical formula: {parameters} must contain at least one column and one arithmetic/logical operator")
                return jsonify({
                    'success': False,
                    'message': 'Arithmetic/logical formula must contain at least one column and one arithmetic/logical operator (+, -, /, %, *, AND, OR)'
                }), 400
        else:
            # Validate comparison formula: 'column_name <operator> integer' or 'column_name <operator> column'
            parts = parameters.strip().split(' ', 2)
            if len(parts) != 3 or parts[0] != f"'{column_name}'" or parts[1] not in ['=', '>', '<', '>=', '<=']:
                logging.error(f"Invalid comparison formula: {parameters}")
                return jsonify({'success': False, 'message': "Comparison formula must be 'column_name <operator> integer' or 'column_name <operator> column'"}), 400
            if parts[2].replace('-', '', 1).isdigit():
                dragged_formula = parts[1:]  # Comparison with integer
            elif parts[2].startswith("'") and parts[2].endswith("'"):
                dragged_formula = parts[1:]  # Comparison with another column
            else:
                logging.error(f"Invalid comparison operand: {parts[2]}")
                return jsonify({'success': False, 'message': "Comparison formula must compare with an integer or another column"}), 400

        # Validate data types of columns in formula
        column_data_types = {
            'name': 'Text',
            'age': 'Int',
            'salary': 'Float',
            'int': 'Int',
            'float': 'Float',
            'text': 'Text',
            'email': 'Email',
            'date': 'Date',
            'boolean': 'Boolean',
            'period': 'Text',
            'cgst': 'Float',
            'sgst': 'Float',
            'igst': 'Float',
            'gst': 'Float',
            'loss': 'Float',
            'address': 'Text',
            'phone': 'Text',
            'id': 'Int',
            'username': 'Text',
            'status': 'Text',
            'created_at': 'Date',
            'updated_at': 'Date',
        }
        selected_column_type = column_data_types.get(column_name, 'Unknown')
        if is_arithmetic:
            for item in dragged_formula:
                if item.startswith("'") and item.endswith("'"):
                    column = item[1:-1].strip().lower()
                    if column in column_data_types and column_data_types[column] != selected_column_type and selected_column_type != 'Unknown' and column_data_types[column] != 'Unknown':
                        logging.error(f"Column '{column}' has type {column_data_types[column]}, expected {selected_column_type}")
                        return jsonify({
                            'success': False,
                            'message': f"Column '{column}' has type {column_data_types[column]}, expected {selected_column_type}"
                        }), 400
        else:
            if parts[2].startswith("'") and parts[2].endswith("'"):
                second_column = parts[2][1:-1].strip().lower()
                second_column_type = column_data_types.get(second_column, 'Unknown')
                if selected_column_type != 'Unknown' and second_column_type != 'Unknown' and second_column_type != selected_column_type:
                    logging.error(f"Second column '{second_column}' has type {second_column_type}, expected {selected_column_type}")
                    return jsonify({
                        'success': False,
                        'message': f"Second column '{second_column}' has type {second_column_type}, expected {selected_column_type}"
                    }), 400
            # No data type restriction for integer comparisons

        # Validate template_id and column_name
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT headers
            FROM excel_templates
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404

        headers = json.loads(template['headers']) if template['headers'] else []
        headers_lower = [h.strip().lower() for h in headers]
        if column_name not in headers_lower:
            cursor.close()
            return jsonify({'success': False, 'message': f"Invalid column name: {column_name}"}), 400

        # Validate second column for comparison formulas
        if not is_arithmetic and parts[2].startswith("'") and parts[2].endswith("'"):
            second_column = parts[2][1:-1].strip().lower()
            if second_column not in headers_lower:
                cursor.close()
                return jsonify({'success': False, 'message': f"Invalid second column name: {second_column}"}), 400

        # Check for duplicate rule_name within the template, excluding the current rule
        cursor.execute("""
            SELECT rule_type_id
            FROM validation_rule_types
            WHERE rule_name = %s AND template_id = %s AND rule_type_id != %s
        """, (rule_name, template_id, rule_id))
        if cursor.fetchone():
            cursor.close()
            return jsonify({'success': False, 'message': f"Rule name '{rule_name}' already exists for this template"}), 400

        cursor.execute("""
            UPDATE validation_rule_types
            SET rule_name = %s, description = %s, parameters = %s, column_name = %s, template_id = %s
            WHERE rule_type_id = %s AND is_custom = TRUE
        """, (rule_name, description, parameters, column_name, template_id, rule_id))
        conn.commit()
        cursor.close()
        logging.info(f"Updated rule {rule_id} with column {column_name}, template {template_id}")
        return jsonify({'success': True, 'message': 'Custom rule updated'})
    except mysql.connector.Error as e:
        logging.error(f"Database error updating rule: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500
    except Exception as e:
        logging.error(f"Unexpected error updating rule: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/rules/<int:rule_id>', methods=['DELETE'])
def delete_rule(rule_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /rules DELETE: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Check if the rule exists and is custom
        cursor.execute("""
            SELECT rule_type_id, is_custom
            FROM validation_rule_types
            WHERE rule_type_id = %s
        """, (rule_id,))
        rule = cursor.fetchone()
        
        if not rule:
            cursor.close()
            logging.error(f"Rule not found: rule_id={rule_id}")
            return jsonify({'success': False, 'message': f'Rule with ID {rule_id} not found'}), 404
        
        if not rule['is_custom']:
            cursor.close()
            logging.error(f"Cannot delete non-custom rule: rule_id={rule_id}")
            return jsonify({'success': False, 'message': 'Only custom rules can be deleted'}), 400

        # Delete dependent records in column_validation_rules
        cursor.execute("""
            DELETE FROM column_validation_rules
            WHERE rule_type_id = %s
        """, (rule_id,))
        
        # Delete the rule from validation_rule_types
        cursor.execute("""
            DELETE FROM validation_rule_types
            WHERE rule_type_id = %s AND is_custom = TRUE
        """, (rule_id,))
        
        if cursor.rowcount == 0:
            cursor.close()
            logging.error(f"No rule deleted: rule_id={rule_id} (possibly already deleted or not custom)")
            return jsonify({'success': False, 'message': 'No rule deleted'}), 400

        conn.commit()
        cursor.close()
        logging.info(f"Successfully deleted custom rule: rule_id={rule_id}")
        return jsonify({'success': True, 'message': 'Custom rule deleted successfully'})
    except mysql.connector.Error as db_err:
        logging.error(f"Database error deleting rule {rule_id}: {str(db_err)}")
        return jsonify({'success': False, 'message': f'Database error: {str(db_err)}'}), 500
    except Exception as e:
        logging.error(f"Unexpected error deleting rule {rule_id}: {str(e)}")
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route('/sftp-list-files', methods=['POST'])
def list_sftp_files():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /sftp-list-files: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        hostname = request.form.get('hostname')
        username = request.form.get('username')
        password = request.form.get('password')
        port = int(request.form.get('port', 22))
        folder_path = request.form.get('folder_path', '').strip()

        if not all([hostname, username, password, folder_path]):
            logging.warning("Missing required fields in SFTP list files request")
            return jsonify({'success': False, 'message': 'All fields are required'}), 400

        logging.debug(f"Listing files in SFTP: {folder_path}/Inbound on {hostname}:{port}")

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=hostname, port=port, username=username, password=password, timeout=10)
        sftp = client.open_sftp()

        inbound_path = os.path.join(folder_path, 'Inbound').replace('\\', '/')
        try:
            files = sftp.listdir_attr(inbound_path)
        except IOError as e:
            logging.error(f"Failed to access Inbound folder: {str(e)}")
            return jsonify({'success': False, 'message': 'Inbound folder not found'}), 400

        file_list = []
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        for file_attr in files:
            filename = file_attr.filename
            if filename.endswith(('.xlsx', '.csv', '.txt', '.dat')):
                cursor.execute("""
                    SELECT t.template_id, t.validation_frequency, t.first_identified_at,
                           COUNT(cvr.column_validation_id) as rule_count
                    FROM excel_templates t
                    LEFT JOIN template_columns tc ON t.template_id = tc.template_id
                    LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                    WHERE t.template_name = %s AND t.user_id = %s AND t.status = 'ACTIVE'
                    GROUP BY t.template_id
                """, (filename, session['user_id']))
                template = cursor.fetchone()
                file_list.append({
                    'filename': filename,
                    'template_id': template['template_id'] if template else None,
                    'status': 'Errors Detected' if template and template['rule_count'] > 0 else 'File Not Configured',
                    'validation_frequency': template['validation_frequency'] if template else None,
                    'first_identified_at': template['first_identified_at'].isoformat() if template and template['first_identified_at'] else None
                })

        cursor.close()
        sftp.close()
        client.close()
        logging.info(f"Listed {len(file_list)} files from Inbound folder")
        return jsonify({'success': True, 'files': file_list})
    except Exception as e:
        logging.error(f"Error listing SFTP files: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500
@app.route('/template/<int:template_id>/frequency', methods=['POST'])
def set_validation_frequency(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /template/frequency: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        data = request.get_json()
        validation_frequency = data.get('validation_frequency')
        if validation_frequency not in ['WEEKLY', 'MONTHLY', 'YEARLY']:
            return jsonify({'success': False, 'message': 'Invalid frequency'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE excel_templates
            SET validation_frequency = %s
            WHERE template_id = %s AND user_id = %s
        """, (validation_frequency, template_id, session['user_id']))
        if cursor.rowcount == 0:
            return jsonify({'success': False, 'message': 'Template not found'}), 404
        conn.commit()
        cursor.close()
        logging.info(f"Set validation frequency to {validation_frequency} for template {template_id}")
        return jsonify({'success': True, 'message': 'Frequency updated successfully'})
    except Exception as e:
        logging.error(f"Error setting validation frequency: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500
@app.route('/sftp-list-outbound-files', methods=['POST'])
def list_sftp_outbound_files():
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /sftp-list-outbound-files: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        hostname = request.form.get('hostname')
        username = request.form.get('username')
        password = request.form.get('password')
        port = int(request.form.get('port', 22))
        folder_path = request.form.get('folder_path', '').strip()

        if not all([hostname, username, password, folder_path]):
            logging.warning("Missing required fields in SFTP outbound list request")
            return jsonify({'success': False, 'message': 'Hostname, username, password, and folder path are required'}), 400

        logging.debug(f"Listing files in SFTP: {folder_path}/Outbound on {hostname}:{port}")

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            timeout=10,
            allow_agent=False,
            look_for_keys=False
        )
        sftp = client.open_sftp()

        outbound_path = os.path.join(folder_path, 'Outbound').replace('\\', '/')
        try:
            files = sftp.listdir_attr(outbound_path)
            file_list = [
                {
                    'filename': file_attr.filename,
                    'timestamp': datetime.fromtimestamp(file_attr.st_mtime).isoformat()
                }
                for file_attr in files
                if file_attr.filename.endswith(('.xlsx', '.csv', '.txt', '.dat'))
            ]
            sftp.close()
            client.close()
            logging.info(f"Listed {len(file_list)} files from Outbound folder")
            return jsonify({'success': True, 'files': file_list})
        except IOError as e:
            logging.error(f"Failed to access Outbound folder: {str(e)}")
            return jsonify({'success': False, 'message': f'Outbound folder not found: {str(e)}'}), 400
        except Exception as e:
            logging.error(f"SFTP operation failed: {str(e)}")
            return jsonify({'success': False, 'message': f'SFTP operation failed: {str(e)}'}), 500
    except ValueError as ve:
        logging.error(f"Invalid port value: {str(ve)}")
        return jsonify({'success': False, 'message': f'Invalid port: {str(ve)}'}), 400
    except Exception as e:
        logging.error(f"Unexpected error in sftp-list-outbound-files: {str(e)}")
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500
@app.route('/sftp-approve/<int:template_id>', methods=['POST'])
def approve_sftp_file(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /sftp-approve: session missing")
        return jsonify({'error': 'Not logged in'}), 401

    try:
        hostname = request.form.get('hostname')
        username = request.form.get('username')
        password = request.form.get('password')
        corrected_file_path = request.form.get('corrected_file_path')

        logging.debug(f"SFTP approve request: template_id={template_id}, hostname={hostname}, username={username}, corrected_file_path={corrected_file_path}")

        if not all([hostname, username, password, corrected_file_path]):
            missing = [k for k, v in {'hostname': hostname, 'username': username, 'password': password, 'corrected_file_path': corrected_file_path}.items() if not v]
            logging.error(f"Missing required fields: {missing}")
            return jsonify({'success': False, 'message': f'Missing required fields: {", ".join(missing)}'}), 400

        # Verify template exists
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT template_name, remote_file_path
            FROM excel_templates
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            logging.error(f"Template not found: template_id={template_id}")
            return jsonify({'success': False, 'message': 'Template not found'}), 404

        # Connect to SFTP
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(hostname=hostname, username=username, password=password, timeout=10)
            sftp = client.open_sftp()

            # Define fixed folder paths
            inbound_path = "/Inbound"
            outbound_path = "/Outbound"
            processing_path = "/processing"

            # Verify Inbound folder exists
            try:
                sftp.stat(inbound_path)
                logging.debug(f"Inbound folder exists: {inbound_path}")
                inbound_files = sftp.listdir(inbound_path)
                logging.debug(f"Files in {inbound_path}: {inbound_files}")
            except IOError as e:
                logging.error(f"Inbound folder not found: {inbound_path}, error: {str(e)}")
                return jsonify({'success': False, 'message': f'Inbound folder not found: {inbound_path}'}), 400

            # Verify Outbound folder exists
            try:
                sftp.stat(outbound_path)
                logging.debug(f"Outbound folder exists: {outbound_path}")
            except IOError as e:
                logging.error(f"Outbound folder not found: {outbound_path}, error: {str(e)}")
                return jsonify({'success': False, 'message': f'Outbound folder not found: {outbound_path}'}), 400

            # Verify processing folder exists
            try:
                sftp.stat(processing_path)
                logging.debug(f"Processing folder exists: {processing_path}")
            except IOError as e:
                logging.error(f"Processing folder not found: {processing_path}, error: {str(e)}")
                return jsonify({'success': False, 'message': f'Processing folder not found: {processing_path}'}), 400

            # Verify and copy corrected file to Outbound
            local_corrected_path = os.path.join(app.config['UPLOAD_FOLDER'], os.path.basename(corrected_file_path))
            if not os.path.exists(local_corrected_path):
                logging.error(f"Local corrected file not found: {local_corrected_path}")
                return jsonify({'success': False, 'message': f'Corrected file not found locally: {os.path.basename(corrected_file_path)}'}), 404

            outbound_file_path = f"{outbound_path}/{os.path.basename(corrected_file_path)}"
            try:
                sftp.put(local_corrected_path, outbound_file_path)
                logging.info(f"Copied corrected file to: {outbound_file_path}")
            except Exception as e:
                logging.error(f"Failed to copy file to Outbound: {str(e)}")
                return jsonify({'success': False, 'message': f'Failed to copy file to Outbound: {str(e)}'}), 500

            # Locate original file in Inbound
            template_name = template['template_name']
            original_file = template.get('remote_file_path') or f"{inbound_path}/{template_name}"
            inbound_files_lower = {f.lower(): f for f in inbound_files}
            template_name_lower = os.path.basename(template_name).lower()
            possible_extensions = ['', '.xlsx', '.csv', '.txt', '.dat']
            found_original = False
            for ext in possible_extensions:
                test_file_lower = f"{template_name_lower}{ext.lower()}"
                if test_file_lower in inbound_files_lower:
                    original_file = f"{inbound_path}/{inbound_files_lower[test_file_lower]}"
                    logging.debug(f"Adjusted original file path to: {original_file}")
                    found_original = True
                    break

            if not found_original:
                try:
                    sftp.stat(original_file)
                    found_original = True
                    logging.debug(f"Found original file at provided path: {original_file}")
                except IOError:
                    pass

            if not found_original:
                logging.error(f"Original file not found: {original_file}, available files in {inbound_path}: {inbound_files}")
                return jsonify({
                    'success': False,
                    'message': f'Original file not found: {os.path.basename(original_file)}. Available files in Inbound: {", ".join(inbound_files)}'
                }), 400

            # Move original file to processing
            process_file_path = f"{processing_path}/{os.path.basename(original_file)}"
            try:
                sftp.stat(original_file)  # Confirm file exists before moving
                sftp.rename(original_file, process_file_path)
                logging.info(f"Moved original file from {original_file} to {process_file_path}")
            except IOError as e:
                logging.error(f"Failed to move original file to processing: {str(e)}")
                return jsonify({'success': False, 'message': f'Failed to move original file to processing: {str(e)}'}), 400
            except Exception as e:
                logging.error(f"Unexpected error moving file to processing: {str(e)}")
                return jsonify({'success': False, 'message': f'Unexpected error moving file to processing: {str(e)}'}), 500

        except paramiko.AuthenticationException as auth_err:
            logging.error(f"Authentication failed: {str(auth_err)}")
            return jsonify({'success': False, 'message': 'Authentication failed: Invalid credentials'}), 401
        except paramiko.SSHException as ssh_err:
            logging.error(f"SSH error: {str(ssh_err)}")
            return jsonify({'success': False, 'message': f'SSH connection failed: {str(ssh_err)}'}), 500
        except Exception as e:
            logging.error(f"Unexpected SFTP error: {str(e)}")
            return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500
        finally:
            sftp.close()
            client.close()
            cursor.close()
            conn.close()

        logging.info(f"Approved file for template {template_id}: copied to Outbound, moved original to processing")
        return jsonify({'success': True, 'message': 'File approved and moved successfully'})
    except Exception as e:
        logging.error(f"Error approving SFTP file: {str(e)}")
        return jsonify({'success': False, 'message': f'Error approving file: {str(e)}'}), 500
@app.route('/sftp-logout', methods=['POST'])
def sftp_logout():
    if 'sftp_credentials' in session:
        session.pop('sftp_credentials', None)
        logging.info("SFTP session cleared")
    return jsonify({'success': True, 'message': 'Disconnected from SFTP server'})
# Add /column-rules endpoints after existing routes
@app.route('/column-rules/<int:template_id>', methods=['GET'])
def get_column_rules(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT rule_id, rule_name, description, column_name, formula, data_type
            FROM column_rules
            WHERE template_id = %s
        """, (template_id,))
        rules = cursor.fetchall()
        cursor.close()
        return jsonify({'success': True, 'rules': rules})
    except Exception as e:
        logging.error(f"Error fetching column rules: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/column-rules/<int:template_id>', methods=['POST'])
def create_column_rule(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        data = request.get_json()
        rule_name = data.get('rule_name')
        description = data.get('description')
        column_name = data.get('column_name')
        data_type = data.get('data_type')
        formula = data.get('formula')

        if not all([rule_name, column_name, data_type, formula]):
            return jsonify({'success': False, 'message': 'Missing required fields'}), 400

        # Validate column_name
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT headers FROM excel_templates WHERE template_id = %s AND user_id = %s", (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404
        headers = json.loads(template['headers'])
        if column_name not in headers:
            cursor.close()
            return jsonify({'success': False, 'message': f"Invalid column: {column_name}"}, 400)

        # Validate data_type
        valid_data_types = ['Required', 'Int', 'Float', 'Text', 'Email', 'Date', 'Boolean', 'Alphanumeric']
        if data_type not in valid_data_types:
            cursor.close()
            return jsonify({'success': False, 'message': f"Invalid data type: {data_type}"}, 400)

        # Validate formula
        tokens = formula.split(' ')
        valid_operators = ['+', '-', '/', '%', '*', 'AND', 'OR', '=', '>', '<', '>=', '<=']
        for ref in tokens:
            if ref not in headers and ref not in valid_operators and ref not in valid_data_types:
                cursor.close()
                return jsonify({'success': False, 'message': f"Invalid token in formula: {ref}"}, 400)
        if '=' not in tokens:
            cursor.close()
            return jsonify({'success': False, 'message': 'Formula must include an equality operator (=)'}, 400)

        cursor.execute("""
            INSERT INTO column_rules (template_id, rule_name, description, column_name, formula, data_type)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (template_id, rule_name, description, column_name, formula, data_type))
        conn.commit()
        cursor.close()
        return jsonify({'success': True, 'message': 'Column rule created'})
    except Exception as e:
        logging.error(f"Error creating column rule: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/column-rules/<int:rule_id>', methods=['PUT'])
def update_column_rule(rule_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        data = request.get_json()
        rule_name = data.get('rule_name')
        description = data.get('description')
        column_name = data.get('column_name')
        data_type = data.get('data_type')
        formula = data.get('formula')

        if not all([rule_name, column_name, data_type, formula]):
            return jsonify({'success': False, 'message': 'Missing required fields'}), 400

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT template_id FROM column_rules WHERE rule_id = %s", (rule_id,))
        rule = cursor.fetchone()
        if not rule:
            cursor.close()
            return jsonify({'success': False, 'message': 'Rule not found'}), 404
        template_id = rule['template_id']

        cursor.execute("SELECT headers FROM excel_templates WHERE template_id = %s AND user_id = %s", (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404
        headers = json.loads(template['headers'])
        if column_name not in headers:
            cursor.close()
            return jsonify({'success': False, 'message': f"Invalid column: {column_name}"}, 400)

        valid_data_types = ['Required', 'Int', 'Float', 'Text', 'Email', 'Date', 'Boolean', 'Alphanumeric']
        if data_type not in valid_data_types:
            cursor.close()
            return jsonify({'success': False, 'message': f"Invalid data type: {data_type}"}, 400)

        tokens = formula.split(' ')
        valid_operators = ['+', '-', '/', '%', '*', 'AND', 'OR', '=', '>', '<', '>=', '<=']
        for ref in tokens:
            if ref not in headers and ref not in valid_operators and ref not in valid_data_types:
                cursor.close()
                return jsonify({'success': False, 'message': f"Invalid token in formula: {ref}"}, 400)
        if '=' not in tokens:
            cursor.close()
            return jsonify({'success': False, 'message': 'Formula must include an equality operator (=)'}, 400)

        cursor.execute("""
            UPDATE column_rules
            SET rule_name = %s, description = %s, column_name = %s, formula = %s, data_type = %s
            WHERE rule_id = %s
        """, (rule_name, description, column_name, formula, data_type, rule_id))
        conn.commit()
        cursor.close()
        return jsonify({'success': True, 'message': 'Column rule updated'})
    except Exception as e:
        logging.error(f"Error updating column rule: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/column-rules/<int:rule_id>', methods=['DELETE'])
def delete_column_rule(rule_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM column_rules WHERE rule_id = %s", (rule_id,))
        conn.commit()
        cursor.close()
        return jsonify({'success': True, 'message': 'Column rule deleted'})
    except Exception as e:
        logging.error(f"Error deleting column rule: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/validate-column-rules/<int:template_id>', methods=['GET'])
def validate_column_rules(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        df_json = session.get('df')
        if not df_json:
            return jsonify({'success': False, 'message': 'No data available'}), 400
        df = pd.read_json(StringIO(df_json))
        headers = session['headers']
        df.columns = headers
        df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT rule_type_id, rule_name, column_name, parameters, is_custom
            FROM validation_rule_types
            WHERE template_id = %s AND is_custom = TRUE
        """, (template_id,))
        rules = cursor.fetchall()
        cursor.close()

        errors = {}
        logging.debug(f"Fetched rules for template_id {template_id}: {rules}")
        for rule in rules:
            column_name = rule['column_name']
            formula = rule['parameters']
            rule_name = rule['rule_name']
            # Map column to data type (default to Float for arithmetic/comparison)
            data_type = {
                'name': 'Text', 'age': 'Int', 'salary': 'Float', 'int': 'Int',
                'float': 'Float', 'text': 'Text', 'email': 'Email', 'date': 'Date',
                'boolean': 'Boolean', 'cgst': 'Float', 'sgst': 'Float', 'igst': 'Float',
                'gst': 'Float', 'loss': 'Float', 'address': 'Text', 'phone': 'Text',
                'id': 'Int', 'username': 'Text', 'status': 'Text',
                'created_at': 'Date', 'updated_at': 'Date',
                'sum': 'Float', 'subtraction': 'Float', 'division': 'Float',
                'multiplication': 'Float', 'comparison': 'Float'
            }.get(column_name.lower(), 'Float')

            try:
                parsed_formula = parse_formula(formula, headers)
                df, error_locations = apply_and_validate_formula(df, parsed_formula, data_type, column_name, headers)
                if error_locations:
                    errors[rule_name] = [
                        {
                            'row': err[0],
                            'value': err[1],
                            'rule_failed': err[2],
                            'reason': err[3]
                        } for err in error_locations
                    ]
            except ValueError as e:
                logging.error(f"Error parsing formula {formula}: {str(e)}")
                errors[rule_name] = [{'row': 0, 'value': '', 'rule_failed': rule_name, 'reason': str(e)}]

        return jsonify({'success': True, 'errors': errors})
    except Exception as e:
        logging.error(f"Error validating column rules: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/validate-generic/<int:template_id>', methods=['GET'])
def validate_generic_template(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    try:
        # Get session data
        df_json = session.get('df')
        if not df_json:
            return jsonify({'success': False, 'message': 'No data available'}), 400
        
        df = pd.read_json(StringIO(df_json))
        headers = session['headers']
        df.columns = headers
        df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Fetch only generic rules (is_custom=False) for the template
        cursor.execute("""
            SELECT tc.column_name, vrt.rule_name, vrt.source_format, vrt.is_custom
            FROM template_columns tc
            JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
            WHERE tc.template_id = %s AND tc.is_selected = TRUE AND vrt.is_custom = FALSE
        """, (template_id,))
        rules = cursor.fetchall()
        cursor.close()

        # Validate generic rules only
        error_cell_locations = {}
        accepted_date_formats = ['%d-%m-%Y', '%m-%d-%Y', '%m/%d/%Y', '%d/%m/%Y', '%m-%Y', '%m-%y', '%m/%Y', '%m/%y']
        
        for rule in rules:
            column_name = rule['column_name']
            rule_name = rule['rule_name']
            
            # Set specific date format if available
            if rule_name.startswith('Date(') and rule['source_format']:
                format_map = {
                    'MM-DD-YYYY': '%m-%d-%Y', 'DD-MM-YYYY': '%d-%m-%Y', 'MM/DD/YYYY': '%m/%d/%Y', 'DD/MM/YYYY': '%d/%m/%Y',
                    'MM-YYYY': '%m-%Y', 'MM-YY': '%m-%y', 'MM/YYYY': '%m/%Y', 'MM/YY': '%m/%y'
                }
                accepted_date_formats = [format_map.get(rule['source_format'], '%d-%m-%Y')]
            
            error_count, locations = check_special_characters_in_column(
                df, column_name, rule_name, accepted_date_formats, check_null_cells=True
            )
            
            if error_count > 0:
                error_cell_locations[column_name] = [
                    {'row': loc[0], 'value': loc[1], 'rule_failed': loc[2], 'reason': loc[3]}
                    for loc in locations
                ]

        # Normalize data rows
        data_rows = df.to_dict('records')
        for row in data_rows:
            for key, value in row.items():
                if pd.isna(value) or value == '':
                    row[key] = 'NULL'

        logging.info(f"Generic validation completed for template {template_id}: {len(error_cell_locations)} columns with errors")
        return jsonify({
            'success': True,
            'error_cell_locations': error_cell_locations,
            'data_rows': data_rows
        })
    except Exception as e:
        logging.error(f"Error in generic validation for template {template_id}: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/validate-custom/<int:template_id>', methods=['GET'])
def validate_custom_template(template_id):
    use_corrected = request.args.get('use_corrected', 'false').lower() == 'true'
    
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get template details
        cursor.execute("SELECT * FROM excel_templates WHERE template_id = %s AND user_id = %s", 
                      (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404
        
        # Determine which file to use
        if use_corrected:
            # Get latest corrected file from history
            cursor.execute("""
                SELECT corrected_file_path FROM validation_history 
                WHERE template_id = %s AND user_id = %s
                ORDER BY history_id DESC LIMIT 1
            """, (template_id, session['user_id']))
            history = cursor.fetchone()
            if history:
                file_path = history['corrected_file_path']
            else:
                # Fallback to session data
                df_json = session.get('df')
                if not df_json:
                    cursor.close()
                    return jsonify({'success': False, 'message': 'No corrected data available'}), 400
                df = pd.read_json(StringIO(df_json))
                headers = json.loads(template['headers'])
                df.columns = headers
                df = df.iloc[session.get('header_row', 0) + 1:].reset_index(drop=True)
        else:
            # Use session data for original file
            df_json = session.get('df')
            if not df_json:
                cursor.close()
                return jsonify({'success': False, 'message': 'No data available'}), 400
            df = pd.read_json(StringIO(df_json))
            headers = json.loads(template['headers'])
            df.columns = headers
            df = df.iloc[session.get('header_row', 0) + 1:].reset_index(drop=True)
        
        # If we have a file path, read from file
        if 'file_path' in locals():
            try:
                if file_path.endswith('.xlsx'):
                    df = pd.read_excel(file_path, sheet_name=template['sheet_name'])
                else:
                    df = pd.read_csv(file_path)
                headers = json.loads(template['headers'])
                df.columns = headers
            except Exception as e:
                logging.error(f"Error reading corrected file {file_path}: {str(e)}")
                cursor.close()
                return jsonify({'success': False, 'message': f'Error reading corrected file: {str(e)}'}), 500
        
        # UPDATED: Fetch only active custom rules (is_custom=True AND is_active=True)
        cursor.execute("""
            SELECT vrt.rule_name, vrt.parameters, vrt.column_name, vrt.is_custom, vrt.is_active
            FROM validation_rule_types vrt
            WHERE vrt.template_id = %s AND vrt.is_custom = TRUE AND vrt.is_active = TRUE
        """, (template_id,))
        rules = cursor.fetchall()
        cursor.close()

        # Validate custom rules
        error_cell_locations = {}
        for rule in rules:
            try:
                column_name = rule['column_name']
                formula = rule['parameters']
                rule_name = rule['rule_name']
                
                # Use the custom validation function
                data_type = 'Float'  # Default for custom rules
                is_valid, error_locations = evaluate_column_rule(df, column_name, formula, headers, data_type)
                
                if error_locations:
                    error_cell_locations[column_name] = [
                        {'row': loc[0], 'value': loc[1], 'rule_failed': loc[2], 'reason': loc[3]}
                        for loc in error_locations
                    ]
            except Exception as e:
                logging.error(f"Error validating custom rule {rule['rule_name']}: {str(e)}")
                continue

        # Normalize data rows
        data_rows = df.to_dict('records')
        for row in data_rows:
            for key, value in row.items():
                if pd.isna(value) or value == '':
                    row[key] = 'NULL'

        logging.info(f"Custom validation completed for template {template_id}: {len(error_cell_locations)} columns with errors, {len(rules)} active custom rules applied")
        return jsonify({
            'success': True,
            'error_cell_locations': error_cell_locations,
            'data_rows': data_rows
        })
    except Exception as e:
        logging.error(f"Error in custom validation for template {template_id}: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500



    
@app.route('/rules/<int:rule_id>/toggle-active', methods=['PUT'])
def toggle_rule_active(rule_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        data = request.get_json()
        is_active = data.get('is_active')
        if is_active is None:
            logging.error(f"Missing is_active field in request for rule_id: {rule_id}")
            return jsonify({'success': False, 'message': 'Missing is_active field'}), 400

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Check if rule exists and get template info
        cursor.execute("""
            SELECT rule_type_id, template_id, is_custom, rule_name
            FROM validation_rule_types 
            WHERE rule_type_id = %s
        """, (rule_id,))
        rule = cursor.fetchone()
        
        if not rule:
            cursor.close()
            logging.error(f"Rule not found for rule_type_id: {rule_id}")
            return jsonify({'success': False, 'message': 'Rule not found'}), 404
            
        if not rule['is_custom']:
            cursor.close()
            logging.error(f"Attempt to toggle non-custom rule for rule_type_id: {rule_id}")
            return jsonify({'success': False, 'message': 'Cannot toggle active status for non-custom rules'}), 400

        # Verify user owns the template
        cursor.execute("""
            SELECT user_id 
            FROM excel_templates 
            WHERE template_id = %s
        """, (rule['template_id'],))
        template = cursor.fetchone()
        
        if not template or template['user_id'] != session['user_id']:
            cursor.close()
            logging.error(f"Unauthorized access or template not found for rule_type_id: {rule_id}, user_id: {session['user_id']}")
            return jsonify({'success': False, 'message': 'Unauthorized or template not found'}), 403

        # Update the rule's active status
        cursor.execute("""
            UPDATE validation_rule_types 
            SET is_active = %s 
            WHERE rule_type_id = %s
        """, (is_active, rule_id))
        
        if cursor.rowcount == 0:
            cursor.close()
            return jsonify({'success': False, 'message': 'Failed to update rule status'}), 500
            
        conn.commit()
        cursor.close()
        
        logging.info(f"Rule '{rule['rule_name']}' (ID: {rule_id}) active status updated to: {is_active}")
        return jsonify({
            'success': True, 
            'message': f"Rule '{rule['rule_name']}' {'activated' if is_active else 'deactivated'} successfully"
        })
        
    except Exception as e:
        logging.error(f"Error toggling rule active status for rule_type_id {rule_id}: {str(e)}")
        return jsonify({'success': False, 'message': f"Error toggling rule: {str(e)}"}), 500

@app.route('/apply-transformation/<int:template_id>', methods=['POST'])
def apply_transformation_rules(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /apply-transformation: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Get template details
        cursor.execute("""
            SELECT template_name, sheet_name, headers 
            FROM excel_templates 
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404

        # Get BOTH Date and Transform-Date rules for this template
        cursor.execute("""
            SELECT column_name, source_format, target_format, rule_name
            FROM validation_rule_types
            WHERE template_id = %s AND (
                (rule_name LIKE 'Transform-Date(%' AND target_format IS NOT NULL) OR
                (rule_name LIKE 'Date(%' AND source_format IS NOT NULL)
            )
            ORDER BY column_name, rule_name
        """, (template_id,))
        all_rules = cursor.fetchall()
        
        logging.info(f"Found {len(all_rules)} transformation-related rules for template {template_id}")
        for rule in all_rules:
            logging.info(f"Rule: {rule['rule_name']}, Column: {rule['column_name']}, Source: {rule['source_format']}, Target: {rule['target_format']}")

        # Group rules by column to get both source and target formats
        column_transforms = {}
        for rule in all_rules:
            column = rule['column_name']
            if column not in column_transforms:
                column_transforms[column] = {'source_format': None, 'target_format': None}
                
            if rule['rule_name'].startswith('Date(') and rule['source_format']:
                column_transforms[column]['source_format'] = rule['source_format']
            elif rule['rule_name'].startswith('Transform-Date(') and rule['target_format']:
                column_transforms[column]['target_format'] = rule['target_format']

        # Filter to only columns that have both source and target formats
        transform_rules = []
        for column, formats in column_transforms.items():
            if formats['source_format'] and formats['target_format']:
                transform_rules.append({
                    'column_name': column,
                    'source_format': formats['source_format'],
                    'target_format': formats['target_format']
                })
                logging.info(f"Will transform column '{column}': {formats['source_format']} -> {formats['target_format']}")

        cursor.close()

        if not transform_rules:
            return jsonify({
                'success': False, 
                'message': 'No complete transformation rules found. Need both Date rule (source format) and Transform-Date rule (target format) for each column.'
            }), 400

        # Get the corrected file path
        corrected_file_path = session.get('corrected_file_path')
        if not corrected_file_path:
            cursor = get_db_connection().cursor(dictionary=True)
            cursor.execute("""
                SELECT corrected_file_path FROM validation_history 
                WHERE template_id = %s AND user_id = %s
                ORDER BY history_id DESC LIMIT 1
            """, (template_id, session['user_id']))
            history = cursor.fetchone()
            cursor.close()
            if history:
                corrected_file_path = history['corrected_file_path']
            else:
                return jsonify({
                    'success': False, 
                    'message': 'No corrected file found. Please complete error correction first.'
                }), 400

        if not os.path.exists(corrected_file_path):
            return jsonify({
                'success': False, 
                'message': f'Corrected file not found at: {corrected_file_path}'
            }), 404

        # Read the file
        headers = json.loads(template['headers'])
        logging.info(f"Reading file: {corrected_file_path}")
        
        if corrected_file_path.endswith('.xlsx'):
            df = pd.read_excel(corrected_file_path, sheet_name=template['sheet_name'])
        else:
            df = pd.read_csv(corrected_file_path)
        
        # Ensure column names match
        if len(df.columns) != len(headers):
            logging.warning(f"Column count mismatch: file has {len(df.columns)}, expected {len(headers)}")
        df.columns = headers
        
        logging.info(f"File loaded with {len(df)} rows and {len(df.columns)} columns")

        # Apply transformations with detailed logging
        transformations_applied = 0
        transformation_details = []
        
        for rule in transform_rules:
            column_name = rule['column_name']
            source_format = rule['source_format']
            target_format = rule['target_format']
            
            if column_name not in df.columns:
                logging.warning(f"Column '{column_name}' not found in DataFrame")
                continue
                
            logging.info(f"Transforming column '{column_name}': {source_format} -> {target_format}")
            
            # Count values before transformation
            non_null_values = df[column_name].notna().sum()
            logging.info(f"Column '{column_name}' has {non_null_values} non-null values")
            
            # Sample some original values
            original_sample = df[column_name].dropna().head(3).tolist()
            logging.info(f"Original sample values: {original_sample}")
            
            # Apply transformation
            df[column_name] = df[column_name].apply(
                lambda x: transform_date(x, source_format, target_format)
            )
            
            # Sample transformed values
            transformed_sample = df[column_name].dropna().head(3).tolist()
            logging.info(f"Transformed sample values: {transformed_sample}")
            
            transformations_applied += 1
            transformation_details.append({
                'column': column_name,
                'source_format': source_format,
                'target_format': target_format,
                'sample_before': original_sample,
                'sample_after': transformed_sample
            })

        if transformations_applied == 0:
            return jsonify({
                'success': False, 
                'message': 'No transformations could be applied'
            }), 400

        # Save transformed file
        base_name, ext = os.path.splitext(template['template_name'])
        transformed_filename = f"{base_name}_transformed{ext}"
        transformed_file_path = os.path.join(app.config['UPLOAD_FOLDER'], transformed_filename)

        try:
            if ext.lower() == '.xlsx':
                df.to_excel(transformed_file_path, index=False, sheet_name=template['sheet_name'])
                logging.info(f"Saved Excel file: {transformed_file_path}")
            else:
                df.to_csv(transformed_file_path, index=False)
                logging.info(f"Saved CSV file: {transformed_file_path}")
            
            # Store in session for download
            session['transformed_file_path'] = transformed_file_path
            
            return jsonify({
                'success': True, 
                'message': f'Successfully applied {transformations_applied} transformation rules',
                'transformed_file_path': transformed_file_path,
                'transformation_details': transformation_details
            })

        except Exception as save_error:
            logging.error(f"Failed to save transformed file: {str(save_error)}")
            return jsonify({
                'success': False, 
                'message': f'Failed to save transformed file: {str(save_error)}'
            }), 500

    except Exception as e:
        logging.error(f"Error applying transformation rules: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            'success': False, 
            'message': f'Error applying transformation rules: {str(e)}'
        }), 500

@app.route('/debug-transformation-rules/<int:template_id>', methods=['GET'])
def debug_transformation_rules(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Get all rules for this template
        cursor.execute("""
            SELECT rule_type_id, rule_name, column_name, source_format, target_format, is_active, is_custom
            FROM validation_rule_types
            WHERE template_id = %s
            ORDER BY column_name, rule_name
        """, (template_id,))
        all_rules = cursor.fetchall()
        cursor.close()

        return jsonify({
            'success': True,
            'template_id': template_id,
            'total_rules': len(all_rules),
            'rules': all_rules
        })

    except Exception as e:
        logging.error(f"Error debugging transformation rules: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500
    
@app.route('/get-file-data-for-transformation/<int:template_id>', methods=['GET'])
def get_file_data_for_transformation(template_id):
    """Get the corrected file data to display in transformation step"""
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Get template details
        cursor.execute("""
            SELECT template_name, sheet_name, headers 
            FROM excel_templates 
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404

        # Get the most recent corrected file
        file_path = session.get('corrected_file_path')
        if not file_path:
            cursor.execute("""
                SELECT corrected_file_path FROM validation_history 
                WHERE template_id = %s AND user_id = %s
                ORDER BY history_id DESC LIMIT 1
            """, (template_id, session['user_id']))
            history = cursor.fetchone()
            if history:
                file_path = history['corrected_file_path']

        # If no corrected file, try to get data from session
        if not file_path or not os.path.exists(file_path):
            # Use session data as fallback
            df_json = session.get('df')
            if df_json:
                df = pd.read_json(StringIO(df_json))
                headers = json.loads(template['headers'])
                df.columns = headers
                df = df.iloc[session.get('header_row', 0) + 1:].reset_index(drop=True)
                
                # Apply any corrections from session
                corrections = session.get('corrections', {})
                for header, rows in corrections.items():
                    if header in headers:
                        for row_idx, value in rows.items():
                            try:
                                row_idx_int = int(row_idx)
                                if 0 <= row_idx_int < len(df):
                                    df.at[row_idx_int, header] = value
                            except (ValueError, IndexError):
                                continue
            else:
                cursor.close()
                return jsonify({'success': False, 'message': 'No data available'}), 400
        else:
            # Read from file
            headers = json.loads(template['headers'])
            if file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path, sheet_name=template['sheet_name'])
            else:
                df = pd.read_csv(file_path)
            
            # Ensure proper column setup
            if len(df.columns) >= len(headers):
                df = df.iloc[:, :len(headers)]
                df.columns = headers
            else:
                cursor.close()
                return jsonify({'success': False, 'message': 'File structure doesn\'t match template'}), 400
        
        cursor.close()
        
        # Convert to dict for JSON response
        data_rows = df.to_dict('records')
        
        # Normalize NULL values
        for row in data_rows:
            for key, value in row.items():
                if pd.isna(value) or value == '':
                    row[key] = 'NULL'

        logging.info(f"Loaded {len(data_rows)} rows for transformation display")

        return jsonify({
            'success': True,
            'data_rows': data_rows,
            'headers': headers
        })

    except Exception as e:
        logging.error(f"Error getting file data for transformation: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get-transformed-data/<int:template_id>', methods=['GET'])
def get_transformed_data(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Get template details
        cursor.execute("""
            SELECT template_name, sheet_name, headers 
            FROM excel_templates 
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404

        # Get the corrected file or original file
        file_path = session.get('corrected_file_path')
        if not file_path:
            cursor.execute("""
                SELECT corrected_file_path FROM validation_history 
                WHERE template_id = %s AND user_id = %s
                ORDER BY history_id DESC LIMIT 1
            """, (template_id, session['user_id']))
            history = cursor.fetchone()
            cursor.close()
            if history:
                file_path = history['corrected_file_path']
            else:
                return jsonify({'success': False, 'message': 'No corrected file found'}), 400

        if not os.path.exists(file_path):
            return jsonify({'success': False, 'message': 'File not found'}), 404

        # Read the file
        headers = json.loads(template['headers'])
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, sheet_name=template['sheet_name'])
        else:
            df = pd.read_csv(file_path)
        
        df.columns = headers
        
        # Convert to dict for JSON response
        data_rows = df.to_dict('records')
        
        # Normalize NULL values
        for row in data_rows:
            for key, value in row.items():
                if pd.isna(value) or value == '':
                    row[key] = 'NULL'

        cursor.close()
        return jsonify({
            'success': True,
            'data_rows': data_rows,
            'headers': headers
        })

    except Exception as e:
        logging.error(f"Error getting transformed data: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/download-transformed/<int:template_id>', methods=['GET'])
def download_transformed_file(template_id):
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /download-transformed: session missing")
        return jsonify({'error': 'Not logged in'}), 401

    try:
        # Get transformed file path from session
        transformed_file_path = session.get('transformed_file_path')
        if not transformed_file_path or not os.path.exists(transformed_file_path):
            return jsonify({'error': 'Transformed file not found'}), 404

        filename = os.path.basename(transformed_file_path)
        logging.info(f"Downloading transformed file: {transformed_file_path}")
        
        return send_file(transformed_file_path, as_attachment=True, download_name=filename)

    except Exception as e:
        logging.error(f"Error downloading transformed file: {str(e)}")
        return jsonify({'error': f'Error downloading file: {str(e)}'}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

# Move this entire block to the BOTTOM of app.py
def initialize_database_once():
    """Initialize database on first worker startup"""
    try:
        logging.info("=== STARTING DATABASE INITIALIZATION ===")
        
        # Use direct connection instead of Flask's g object
        conn = get_direct_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()  # ✅ Fixed: fetch the result
        cursor.close()
        logging.info("Database connection test successful")
        
        # Check if tables exist
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        existing_tables = cursor.fetchall()  # ✅ Already correct
        cursor.close()
        
        if len(existing_tables) == 0:
            logging.info("No tables found. Creating database structure...")
            
            # Create tables directly without Flask context
            tables = [
                """
                CREATE TABLE IF NOT EXISTS login_details (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    email VARCHAR(255) UNIQUE,
                    mobile VARCHAR(10),
                    password VARCHAR(255)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS excel_templates (
                    template_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    template_name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    user_id INT NOT NULL,
                    sheet_name VARCHAR(255),
                    headers JSON,
                    status ENUM('ACTIVE', 'INACTIVE') DEFAULT 'ACTIVE',
                    is_corrected BOOLEAN DEFAULT FALSE,
                    remote_file_path VARCHAR(512),
                    FOREIGN KEY (user_id) REFERENCES login_details(id) ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS template_columns (
                    column_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    template_id BIGINT NOT NULL,
                    column_name VARCHAR(255) NOT NULL,
                    column_position INT NOT NULL,
                    is_validation_enabled BOOLEAN DEFAULT FALSE,
                    is_selected BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                    UNIQUE (template_id, column_name)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS validation_rule_types (
                    rule_type_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    rule_name VARCHAR(255) UNIQUE NOT NULL,
                    description TEXT,
                    parameters TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    is_custom BOOLEAN DEFAULT FALSE,
                    column_name VARCHAR(255),
                    template_id BIGINT,
                    data_type VARCHAR(50),
                    source_format VARCHAR(50),
                    target_format VARCHAR(50),
                    FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS column_validation_rules (
                    column_validation_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    column_id BIGINT NOT NULL,
                    rule_type_id BIGINT NOT NULL,
                    rule_config JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (column_id) REFERENCES template_columns(column_id) ON DELETE CASCADE,
                    FOREIGN KEY (rule_type_id) REFERENCES validation_rule_types(rule_type_id) ON DELETE RESTRICT,
                    UNIQUE (column_id, rule_type_id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS validation_history (
                    history_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    template_id BIGINT NOT NULL,
                    template_name VARCHAR(255) NOT NULL,
                    error_count INT NOT NULL,
                    corrected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    corrected_file_path VARCHAR(512) NOT NULL,
                    user_id INT NOT NULL,
                    FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES login_details(id) ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS validation_corrections (
                    correction_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    history_id BIGINT NOT NULL,
                    row_index INT NOT NULL,
                    column_name VARCHAR(255) NOT NULL,
                    original_value TEXT,
                    corrected_value TEXT,
                    rule_failed VARCHAR(255) DEFAULT NULL,
                    FOREIGN KEY (history_id) REFERENCES validation_history(history_id) ON DELETE CASCADE
                )
                """
            ]
            
            cursor = conn.cursor()
            for table_sql in tables:
                cursor.execute(table_sql)
            
            # Add missing columns if needed
            try:
                cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'source_format'")
                result = cursor.fetchall()  # ✅ Fixed: fetch the result
                if not result:
                    cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN source_format VARCHAR(50)")
                    logging.info("Added source_format column to validation_rule_types table")
            except:
                pass
                
            try:
                cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'target_format'")
                result = cursor.fetchall()  # ✅ Fixed: fetch the result
                if not result:
                    cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN target_format VARCHAR(50)")
                    logging.info("Added target_format column to validation_rule_types table")
            except:
                pass
                
            try:
                cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'data_type'")
                result = cursor.fetchall()  # ✅ Fixed: fetch the result
                if not result:
                    cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN data_type VARCHAR(50)")
                    logging.info("Added data_type column to validation_rule_types table")
            except:
                pass
                
            try:
                cursor.execute("SHOW COLUMNS FROM excel_templates LIKE 'remote_file_path'")
                result = cursor.fetchall()  # ✅ Fixed: fetch the result
                if not result:
                    cursor.execute("ALTER TABLE excel_templates ADD COLUMN remote_file_path VARCHAR(512)")
                    logging.info("Added remote_file_path column to excel_templates table")
            except:
                pass
                
            try:
                cursor.execute("SHOW COLUMNS FROM template_columns LIKE 'is_selected'")
                result = cursor.fetchall()  # ✅ Fixed: fetch the result
                if not result:
                    cursor.execute("ALTER TABLE template_columns ADD COLUMN is_selected BOOLEAN DEFAULT FALSE")
                    logging.info("Added is_selected column to template_columns table")
            except:
                pass
            
            conn.commit()
            logging.info("Tables created successfully")
            
            # Create admin user
            import bcrypt
            admin_password = bcrypt.hashpw('admin'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            cursor.execute("""
                INSERT IGNORE INTO login_details (first_name, last_name, email, mobile, password)
                VALUES (%s, %s, %s, %s, %s)
            """, ('Admin', 'User', 'admin@example.com', '1234567890', admin_password))
            conn.commit()
            logging.info("Admin user created")

            cursor.execute("SELECT id, email, first_name FROM login_details WHERE email = 'admin@example.com'")
            admin_check = cursor.fetchall()
            if admin_check:
                logging.info(f"Admin user found in database: {admin_check}")
            else:
                logging.error("Admin user NOT found in database!")
    
cursor.execute("SELECT COUNT(*) as count FROM login_details")
user_count = cursor.fetchone()
logging.info(f"Total users in database: {user_count[0]}")
            
            # Create default validation rules
            default_rules = [
                ("Required", "Ensures the field is not null", '{"allow_null": false}', None, None, None),
                ("Int", "Validates integer format", '{"format": "integer"}', None, None, "Int"),
                ("Float", "Validates number format (integer or decimal)", '{"format": "float"}', None, None, "Float"),
                ("Text", "Allows text with quotes and parentheses", '{"allow_special": false}', None, None, "Text"),
                ("Email", "Validates email format", '{"regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$"}', None, None, "Email"),
                ("Date", "Validates date", '{"format": "%d-%m-%Y"}', "DD-MM-YYYY", None, "Date"),
                ("Boolean", "Validates boolean format (true/false or 0/1)", '{"format": "boolean"}', None, None, "Boolean"),
                ("Alphanumeric", "Validates alphanumeric format", '{"format": "alphanumeric"}', None, None, "Alphanumeric")
            ]
            cursor.executemany("""
                INSERT IGNORE INTO validation_rule_types (rule_name, description, parameters, is_custom, source_format, target_format, data_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, [(name, desc, params, False, source, target, dtype) for name, desc, params, source, target, dtype in default_rules])
            conn.commit()
            logging.info("Default validation rules created")
            
            cursor.close()
            
            # Verify creation
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            new_tables = cursor.fetchall()  # ✅ Already correct
            cursor.close()
            
            logging.info(f"=== DATABASE INITIALIZATION COMPLETE ===")
            logging.info(f"Created {len(new_tables)} tables: {[t[0] for t in new_tables]}")
        else:
            logging.info(f"Database already initialized with {len(existing_tables)} tables")
            
        conn.close()
            
    except Exception as e:
        logging.error(f"=== DATABASE INITIALIZATION FAILED ===")
        logging.error(f"Error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())


# Initialize database when module is loaded (for Gunicorn)
try:
    logging.info("Attempting to initialize database on module load...")
    initialize_database_once()
    logging.info("Database initialization completed on module load")
except Exception as e:
    logging.error(f"Failed to initialize database on module load: {e}")
    import traceback
    logging.error(traceback.format_exc())

if __name__ == '__main__':
    try:
        # Get port from environment variable (Railway sets this automatically)
        port = int(os.environ.get('PORT', 8000))
        logging.info(f"Starting Flask server on port {port}...")
        app.run(debug=False, host='0.0.0.0', port=port)
    except Exception as e:
        logging.error(f"Failed to start application: {e}")
        raise
















