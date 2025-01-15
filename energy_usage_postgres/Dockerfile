# Use the official PostgreSQL image from the Docker Hub
FROM postgres:latest

# Set the working directory in the container
WORKDIR /docker-entrypoint-initdb.d/

# Copy the SQL file to the Docker entrypoint init directory
COPY energy_usage_db_init.sql /docker-entrypoint-initdb.d/

# Expose the default PostgreSQL port
EXPOSE 5432
