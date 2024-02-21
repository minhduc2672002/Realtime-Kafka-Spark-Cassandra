import logging
from datetime import  datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col, udf
from pyspark.sql.types import StructType, StructField, StringType
import  uuid
import  os


# spark_version = '3.4.1'
# os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version} pyspark-shell '

def create_cassandra_connection():
    cass_session = None
    try:
        cluster = Cluster(['localhost'])
        cass_session = cluster.connect()
        return  cass_session
    except Exception as e:
        logging.error(f'Could not create cassandra connection because {e}')
        return None
def create_spark_connection():
    spark_conn = None
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        print("connect ok")
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_conn


def read_data_from_kafka(spark):
    spark_df = None
    try:
        spark_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users_create2") \
            .option('startingOffsets', 'earliest') \
            .load()

        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.error(f"kafka dataframe could not be created because: {e}")

    return spark_df


def format_spark_df(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
    spark_df_format = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*").withColumn("id", uuidUdf())

    print("Format DataFrame Succesfully")

    return spark_df_format


#create keyspace( same database) in Cassandra
def create_keyspace(session):
    #create keyspace here
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class' : 'SimpleStrategy','replication_factor' : '1'};
    """)

    print("Keyspace created successfully")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        postcode TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("Table created successfully!")
def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

if __name__ == '__main__':
    spark_conn = create_spark_connection()
    if spark_conn is not None:

        spark_df = read_data_from_kafka(spark_conn)
        spark_df_format = format_spark_df(spark_df)
        cass_session = create_cassandra_connection()

        if cass_session is not None:
            create_keyspace(cass_session)
            create_table(cass_session)

            logging.info("Streaming is being started...")

            streaming_query = (spark_df_format.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

        streaming_query.awaitTermination()
        # spark_df_format.writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .start() \
        #     .awaitTermination()