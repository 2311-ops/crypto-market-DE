FROM bitnami/spark:3.5.0

USER root

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/spark/workdir

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Switch back to spark user
USER 1001

# Run the Spark streaming job
CMD ["python3", "spark_streaming_job.py"]
