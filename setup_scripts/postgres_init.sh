#!/bin/bash
sudo apt update
# Install PostgreSQL
sudo apt install postgresql

# Start PostgreSQL service
sudo systemctl start postgresql@12-main

# Switch to postgres user and open psql
sudo -u postgres psql <<EOF
-- Create a new user
CREATE USER admin WITH PASSWORD 'password';

-- Create a new database
CREATE DATABASE benchbase;

-- Grant privileges to the user on the database
GRANT ALL PRIVILEGES ON DATABASE benchbase TO admin;

-- Exit psql
\q
EOF
