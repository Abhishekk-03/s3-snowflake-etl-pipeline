FROM python:3.11-slim

WORKDIR /app

# Java required by Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install PySpark, boto3, pandas, pytest
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Application
COPY jobs/ ./jobs/
COPY utils/ ./utils/
COPY jars/ ./jars/
COPY main.py .

CMD ["python", "main.py"]