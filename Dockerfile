# ------------------------------------------------------------
# Dockerfile for Event Log Quality Monitor
# ------------------------------------------------------------
# Base image: lightweight official Python 3.11 slim image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy dependency list and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source files into the container
COPY . .

# Expose FastAPI app port
EXPOSE 8000

# Default command to launch FastAPI service
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]