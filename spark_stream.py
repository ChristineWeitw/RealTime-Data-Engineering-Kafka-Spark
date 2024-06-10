import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.market_data (
        id UUID PRIMARY KEY,
        symbol TEXT,
        price FLOAT,
        timestamp TIMESTAMP
    );
    """)


def insert_data(session, **kwargs):
    print("inserting data...")

    id = kwargs.get('id')
    symbol = kwargs.get('symbol')
    price = kwargs.get('price')
    timestamp = kwargs.get('timestamp')

    try:
        session.execute("""
            INSERT INTO spark_streams.market_data(id, symbol, price, timestamp)
                VALUES (%s, %s, %s, %s)
        """, (id, symbol, price, timestamp))
        logging.info(f"Data inserted for ID: {id}, Symbol: {symbol}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')



def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn=None):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'stock_prices') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None



def create_selection_df_from_kafka(spark_df):
    if spark_df is None:
        raise ValueError("The spark_df is None. Ensure Kafka connection is established properly.")

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("timestamp", TimestampType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        print('spark_df:', spark_df)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session is not None:
                create_keyspace(session)
                create_table(session)

                logging.info("Streaming is being started...")

                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'market_data')
                                .start())

                streaming_query.awaitTermination()
        else:
            logging.error("Failed to create kafka dataframe.")
    else:
        logging.error("Failed to create spark connection.")
