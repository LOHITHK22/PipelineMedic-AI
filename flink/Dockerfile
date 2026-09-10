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

# Kafka DataStream connector jar (org.apache.flink.connector.kafka.source/sink.*
# classes used by KafkaSource/KafkaSink in the PyFlink job) plus its kafka-clients
# dependency. NOTE: flink-sql-connector-kafka (the "sql" jar) only relocates/shades
# the Table/SQL API classes and does NOT provide the DataStream KafkaSource/KafkaSink
# classes -- using it alone fails at job-graph init with
# "ClassNotFoundException: org.apache.flink.connector.kafka.source.KafkaSource".
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar \
    /opt/flink/lib/flink-connector-kafka-3.1.0-1.18.jar
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
    /opt/flink/lib/kafka-clients-3.4.0.jar
# Docker ADD from a URL creates the file as root:root mode 0600, which the
# non-root "flink" user (the container's runtime user) cannot read -- this
# silently manifests as ClassNotFoundException for the connector classes at
# job-graph init time, not as a permissions error. Fix ownership/mode.
RUN chown flink:flink /opt/flink/lib/flink-connector-kafka-3.1.0-1.18.jar /opt/flink/lib/kafka-clients-3.4.0.jar && \
    chmod 644 /opt/flink/lib/flink-connector-kafka-3.1.0-1.18.jar /opt/flink/lib/kafka-clients-3.4.0.jar

COPY jobs /opt/flink/jobs
