FROM eclipse-temurin:17.0.12_7-jre-jammy AS java
FROM python:3.11.9-bookworm
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}" PYTHONUNBUFFERED=1 PYTHONPATH=/app SPARK_LOCAL_IP=127.0.0.1
WORKDIR /app
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt
# Resolve the matching Scala 2.12 Kafka connector at build time, not at demo time.
RUN python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7').getOrCreate(); s.stop()"
COPY backend /app/backend
COPY spark /app/spark
COPY scripts /app/scripts
COPY pytest.ini /app/pytest.ini
CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
