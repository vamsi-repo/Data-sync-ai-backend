import os
import logging
from app import app, init_db, create_admin_user, create_default_validation_rules

logging.basicConfig(level=logging.INFO)

def initialize_app():
    """Initialize database when starting with Gunicorn"""
    try:
        with app.app_context():
            logging.info("Initializing database...")
            init_db()
            logging.info("Creating admin user...")
            create_admin_user()
            logging.info("Creating default validation rules...")
            create_default_validation_rules()
            logging.info("Database initialization complete!")
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        raise

if __name__ == '__main__':
    initialize_app()
