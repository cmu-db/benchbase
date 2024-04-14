#!/bin/bash

# Accept PostgreSQL profile as input variable
PROFILE=$1

# Check if profile is provided
if [ -z "$PROFILE" ]; then
    echo "Please provide a profile (e.g., postgres) as an argument."
    exit 1
fi

# Change directory to benchbase
cd ..

# Update package list
sudo apt update

# Install OpenJDK 21
sudo apt install openjdk-21-jdk

# Set JAVA_HOME environment variable
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64/

# Build the project with Maven and specify the profile
./mvnw clean package -P ${PROFILE}

# Navigate to the target directory
cd target

# Extract the generated tarball
tar xvzf benchbase-${PROFILE}.tgz

# Navigate into the extracted directory
cd benchbase-${PROFILE}


