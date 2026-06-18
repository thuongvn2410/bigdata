FROM apache/spark:3.5.1
USER root

ARG MVN=https://repo1.maven.org/maven2

# postgresql JDBC driver
RUN curl -fL \
    "$MVN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
    -o "$SPARK_HOME/jars/postgresql-42.7.4.jar"

# Kafka connector for Spark + its runtime dependencies
RUN curl -fL \
    "$MVN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" \
    -o "$SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar" && \
    curl -fL \
    "$MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" \
    -o "$SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" && \
    curl -fL \
    "$MVN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
    -o "$SPARK_HOME/jars/kafka-clients-3.4.1.jar" && \
    curl -fL \
    "$MVN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
    -o "$SPARK_HOME/jars/commons-pool2-2.11.1.jar"

USER spark
