
# Data Validation Tool

A full-stack web application for validating Excel/CSV data, built with Flask (backend) and React/TypeScript (frontend).

## Project Structure

- `app.py`: Flask backend application
- `/src`: React frontend application

## Setup Instructions

### Backend Setup

1. Install Python dependencies:
```bash
pip install flask flask_session flask_cors pandas openpyxl mysql-connector-python bcrypt
```

2. Configure MySQL:
   - Make sure MySQL server is running
   - Update the DB_CONFIG in app.py if needed (currently set to user 'root', password 'Keansa@2025')

3. Run the Flask backend:
```bash
python app.py
```
The backend will run on http://localhost:5000

### Frontend Setup

1. Install Node.js dependencies:
```bash
npm install
```

2. Run the React frontend:
```bash
npm run dev
```
The frontend will run on http://localhost:8080

## Usage

1. Access the application at http://localhost:8080
2. Login with the default admin account:
   - Email: admin@example.com
   - Password: admin
3. Or create a new user account through the registration page

## Development

- Backend (app.py): Handles API endpoints, database operations, and file processing logic
- Frontend: React application that communicates with the backend API
