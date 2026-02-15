FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements/base.txt requirements/ml.txt ./
RUN pip install --no-cache-dir -r base.txt -r ml.txt

# Copy source code
COPY src/ ./src/
COPY config/ ./config/
COPY models/ ./models/
COPY data/ ./data/

# Create directories
RUN mkdir -p logs models/artifacts

# Set Python path
ENV PYTHONPATH=/app

CMD ["python", "-m", "src.modeling.train_pipeline"]
