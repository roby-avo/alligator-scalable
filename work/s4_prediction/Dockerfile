# Use a minimal Python image as the base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /code

# Install system dependencies required for TensorFlow and h5py
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    pkg-config \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

# Install TensorFlow and other Python packages
RUN pip install --no-cache-dir tensorflow==2.16.1 orjson

# Copy the current directory contents into the container
COPY . .

# Define the command to run the application
CMD ["python", "prediction.py", "./output.json", "rho"]
