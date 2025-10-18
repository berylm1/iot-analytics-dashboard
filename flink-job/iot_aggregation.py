"""
IoT Telemetry Aggregation Job using PyFlink
Reads from Kafka, aggregates in 1-minute windows, writes back to Kafka
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col
import os


def create_kafka_source_table(table_env):
    """Create Kafka source table for raw telemetry data"""
    source_ddl = """
        CREATE TABLE telemetry_source (
            device_id STRING,
            temperature DOUBLE,
            humidity DOUBLE,
            latitude DOUBLE,
            longitude DOUBLE,
           `timestamp` TIMESTAMP(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'iot-telemetry-raw',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'pyflink-consumer-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """
    table_env.execute_sql(source_ddl)
    print("✓ Created Kafka source table: telemetry_source")


def create_kafka_sink_table(table_env):
    """Create Kafka sink table for aggregated results"""
    sink_ddl = """
        CREATE TABLE telemetry_aggregated (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            avg_temperature DOUBLE,
            avg_humidity DOUBLE,
            min_temperature DOUBLE,
            max_temperature DOUBLE,
            device_count BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'iot-telemetry-aggregated',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json'
        )
    """
    table_env.execute_sql(sink_ddl)
    print("✓ Created Kafka sink table: telemetry_aggregated")


def create_aggregation_query(table_env):
    """Create and execute the aggregation query"""
    aggregation_query = """
        INSERT INTO telemetry_aggregated
        SELECT
            TUMBLE_START(`timestamp`, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(`timestamp`, INTERVAL '1' MINUTE) AS window_end,
            AVG(temperature) AS avg_temperature,
            AVG(humidity) AS avg_humidity,
            MIN(temperature) AS min_temperature,
            MAX(temperature) AS max_temperature,
            COUNT(DISTINCT device_id) AS device_count
        FROM telemetry_source
        GROUP BY TUMBLE(`timestamp`, INTERVAL '1' MINUTE)
    """
    
    print("✓ Executing aggregation query...")
    print("  - Reading from: iot-telemetry-raw")
    print("  - Window size: 1 minute (tumbling)")
    print("  - Writing to: iot-telemetry-aggregated")
    
    # Execute the query (returns a TableResult)
    table_result = table_env.execute_sql(aggregation_query)
    return table_result


def main():
    print("=" * 60)
    print("Starting IoT Telemetry Aggregation Job (PyFlink)")
    print("=" * 60)
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)  # Match our TaskManager slots
    
    # Create table environment with streaming mode
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Add Kafka connector JAR to the classpath
    # The JAR should be in /opt/flink/lib or specified via --jarfile
    kafka_jar = '/opt/flink/lib/flink-sql-connector-kafka-1.18.1.jar'
    if os.path.exists(kafka_jar):
        table_env.get_config().set(
            "pipeline.jars",
            f"file://{kafka_jar}"
        )
        print(f"✓ Loaded Kafka connector: {kafka_jar}")
    else:
        print(f"⚠ Warning: Kafka connector JAR not found at {kafka_jar}")
        print("  Make sure to provide it via --jarfile when submitting")
    
    # Create tables
    create_kafka_source_table(table_env)
    create_kafka_sink_table(table_env)
    
    # Execute aggregation
    table_result = create_aggregation_query(table_env)
    
    print("=" * 60)
    print("✓ Job submitted successfully!")
    print("✓ Monitor progress at: http://localhost:8081")
    print("=" * 60)
    
    # Wait for the job to complete (it runs indefinitely for streaming)
    table_result.wait()


if __name__ == '__main__':
    main()
