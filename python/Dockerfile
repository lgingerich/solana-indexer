# Use an official Python runtime as the base image
FROM python:3.10-slim

# Install default-jdk
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Set the working directory in the container
WORKDIR /solana-indexer

# Install Poetry
RUN pip install --no-cache-dir poetry

# Copy only the pyproject.toml and poetry.lock (if it exists) files
COPY pyproject.toml poetry.lock* /solana-indexer/

# Install project dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Copy the rest of the application code
COPY . /solana-indexer

# Set the environment variable for Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Run main.py when the container launches
CMD ["python", "src/solana_indexer/main.py"]
