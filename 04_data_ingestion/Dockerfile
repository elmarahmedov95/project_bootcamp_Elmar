# Use the official Python 3.12 slim image
FROM python:3.12-slim

# Set environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files
# PYTHONUNBUFFERED: Ensures logs are sent straight to terminal without buffering
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOME=/app

# Set the working directory
WORKDIR $APP_HOME

# Install system dependencies (if needed)
# slim images are bare-bones, so you might need build-essential or libpq-dev for some packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# Copy only requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY 01_github_to_s3.py 01_github_to_s3.py
COPY 02_github_to_s3_initial_load.py 02_github_to_s3_initial_load.py
COPY 02_github_to_s3_initial_load_async.py 02_github_to_s3_initial_load_async.py

# Create a non-privileged user and switch to it for security
RUN adduser --disabled-password --gecos "" appuser && chown -R appuser $APP_HOME
USER appuser

# Expose the port the app runs on
EXPOSE 8000

# Define the command to run the app
CMD ["python", "01_github_to_s3.py"]