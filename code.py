from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import  from_unixtime,col,substring,round,sum,when
from pyspark.sql.types import StructField,StringType,StructType

def getRawSchema(columns: list, includeCorruptField = True):
    schema = []
    for column in columns:
        schema.append(StructField(column, StringType(), True))

    if includeCorruptField:
        schema.append(StructField("_corrupt_record", StringType(), True))
    return StructType(schema)

def transform(df: DataFrame):
    """
    :param df: Spark data frame
    :return:Spark data frame
    """
    df = df.withColumn("timestamp", from_unixtime(substring(col("timestamp"),0,10),"yyyy-MM-dd HH:mm:ss"))\
        .withColumn("latitude",round("latitude",4))\
        .withColumn("longitude",round("longitude",4))
    return df

def read_csv(path, schema):
    """
    :param path: path
    :param schema: Spark data frame schema
    :return:Spark data frame
    """
    df = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").csv(
    path=path, encoding="utf-8", sep=",", header=False, schema=schema)
    return df

def dedup_count(df: DataFrame, columns: list):
    """
    :param df: Spark data frame
    :param columns: list of columns
    :return:Spark data frame
    """
    count=df.groupBy(columns)\
    .count().where(col('count')>1)\
    .select(sum('count'))
    return count

def to_parquet(df: DataFrame, path: str):
    """
    :param df: Spark data frame
    :param path: folder path
    """
    df.write.mode("overwrite").parquet(path)

def cleanup(df: DataFrame):
    """
    :param df: Spark data frame
    :return:Spark data frame
    """
    for i in df.columns:
        df.withColumn(i, \
            when(col(i)=="" ,None) \
            .otherwise(col(i)))
    return df

if __name__ == '__main__':
    ######################################################
    # Create SparkSession 
    ######################################################
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkETL") \
        .getOrCreate() 
    spark.sparkContext.setLogLevel('ERROR')
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger('SparkETL')

    input_path = r"C:\Users\Harish_Kasa\Downloads\test\input\*"
    output_path = r"C:\Users\Harish_Kasa\Downloads\test\output.parquet"
    dedup_columns = ['device_id','latitude','longitude','timestamp']

    ######################################################
    # Start Processing 
    ######################################################
    locationColumns = ['device_id','id_type','latitude','longitude','horizontal_accuracy','timestamp','ip_address','device_os','os_version',
					  'user_agent','country','source_id','publisher_id','app_id','location_context','geohash']
    df = read_csv(input_path,getRawSchema(locationColumns))
    df = transform(df)
    df = cleanup(df)
    # to_parquet(df,output_path)
    count = dedup_count(df, dedup_columns)
    count.show()

    df.registerTempTable("events")

    ##Daily Average Users  
    sql1 = '''
        select 
        count(distinct device_id),
        date(timestamp) 
        from events group by date(timestamp)
    '''
    df1=spark.sql(sql1)
    df1.show()

    ### Completeness Percentage of each of the attributes. 
    ###  Explanation : Percentage of events with filled values.

    sql2 = '''
    select (100 * (select count(device_id) from events where device_id is not null)/
    (select count(1) from events )) as cp_device_id,
    (100 * (select count(ip_address) from events where ip_address is not null)/
    (select count(1) from events )) as cp_ip_address,
    (100 * (select count(os_version) from events where os_version is not null)/
    (select count(1) from events )) as cp_os_version
    '''

    df1=spark.sql(sql2)
    df1.show()

    ### Events per User per Day. 
    ### Explanation : Average of total number of events for each unique user seen in a day. 

    sql3 = '''
        select
        count(device_id)/count(distinct device_id) as average_events ,
        date(timestamp) 
        from events group by date(timestamp)
    '''
    df1=spark.sql(sql3)
    df1.show()

    ### Distribution of Horizontal Accuracy. 
    ### Explanation : Calculate the average horizontal accuracy for each User and sort them into  the following bins.  

    sql4 = '''
        select HA_Range,
        total_events,
        percentage,
        sum(percentage) OVER (order by HA_Range) as cumulative_percentage 
        from (select HA_Range, count(*) as total_events, 100 * count(*) /(select count(*) from events) as percentage
        from
        (
        select case when horizontal_accuracy  between 0  and 5 then "  0 to 5"
        when horizontal_accuracy  between 6  and 10 then "  6 to 10"
        when horizontal_accuracy  between 11 and 25 then " 11 to 25"
        when horizontal_accuracy  between 26 and 50 then " 26 to 50"
        when horizontal_accuracy  between 51 and 100 then " 51 to 100"
        when horizontal_accuracy  between 101 and 500 then "101 to 500"
        when horizontal_accuracy  >= 501 then "over 501"
        end as HA_Range
        from events
        )
        group by HA_Range having HA_Range is not null order by HA_Range) 
    '''

    df1=spark.sql(sql4)
    df1.show()
