FROM flink:1.18-scala_2.12-java11

# Install Python and PyFlink
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink
RUN pip3 install apache-flink==1.18.1

# Copy Kafka connector JAR
COPY lib/*.jar /opt/flink/lib/

# Copy PyFlink job
COPY iot_aggregation.py /opt/flink/

WORKDIR /opt/flink

CMD ["python", "iot_aggregation.py"]
