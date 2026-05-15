FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for MySQL client if needed
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Run the data generation script
CMD ["python", "data_generation/data_generation.py"]
