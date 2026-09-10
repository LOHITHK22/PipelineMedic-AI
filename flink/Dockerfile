# Custom Flink image with PyFlink + Kafka connector installed.
# Build: docker build -t pipelinemedic-flink ./flink
# This image is referenced by docker-compose's "full" profile once you swap
# the plain flink:1.18 image for this one (see README limitations note).
FROM flink:1.18-scala_2.12-java11

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Kafka SQL/DataStream connector jar (matches Flink 1.18 + Kafka client 3.x)
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
    /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar

COPY jobs /opt/flink/jobs
