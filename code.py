from pyspark.sql import SparkSession
from pyspark.sql.functions import  from_unixtime,col,substring,round
from pyspark.sql.types import StructField,StringType,StructType

locationColumns = ['device_id','id_type','latitude','longitude','horizontal_accuracy','timestamp','ip_address','device_os','os_version',
					  'user_agent','country','source_id','publisher_id','app_id','location_context','geohash']

def getRawSchema(includeCorruptField = True):
    schema = []
    for column in locationColumns:
        schema.append(StructField(column, StringType(), True))
	
    if includeCorruptField:
        schema.append(StructField("_corrupt_record", StringType(), True))
    return StructType(schema)

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 
spark.sparkContext.setLogLevel('ERROR')


input_path = r"C:\Users\Harish_Kasa\Downloads\20220125_103125_00086_b9kns_bucket-00000"
output_path = r"C:\Users\Harish_Kasa\Downloads\20220125_103125_00086_b9kns_bucket-00000"
dedup_columns = ['device_id','latitude','longitude','timestamp']

# .withColumn("timestamp_ms", concat_ws(".",from_unixtime(substring(col("timestamp"),0,10),"yyyy-MM-dd HH:mm:ss"),substring(col("timestamp"),-3,3)))
def transform(dataframe):
    dataframe = dataframe.withColumn("timestamp", from_unixtime(substring(col("timestamp"),0,10),"yyyy-MM-dd HH:mm:ss"))\
        .withColumn("latitude",round("latitude",4))\
        .withColumn("longitude",round("longitude",4))
    return dataframe

def read_csv(path,schema):
    df = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").csv(
    path=path, encoding="utf-8", sep=",", header=False, schema=schema)
    return df

def dedup_count(dataframe,columns):
    count=dataframe.groupBy(columns)\
    .count().where(col('count')>1).count()
    return count

def to_parquet(dataframe,path):
    dataframe.write.mode("overwrite").parquet(path)

df = read_csv(input_path,getRawSchema())
df = transform(df)
# to_parquet(df,output_path)
# print(dedup_count(df,dedup_columns))

df.registerTempTable("events")

##Daily Average Users  
sql1 = '''
select count(distinct device_id),date(timestamp) from events group by date(timestamp)
'''
df1=spark.sql(sql1)
df1.show()

### Completeness Percentage of each of the attributes. 
###  Explanation : Percentage of events with filled values.

sql2 = '''
select ((select count(*) from events where 
   device_id is not null 
and id_type is not null
and latitude  is not null
and longitude is not null
and horizontal_accuracy is not null
and timestamp is not null
and ip_address is not null
and device_os is not null
and os_version is not null
and user_agent is not null
and country is not null
and source_id is not null
and publisher_id is not null
and app_id is not null
and location_context is not null
and geohash is not null
) * 100/(select count(*) from events)) as Completeness_Percentage
'''
df1=spark.sql(sql2)
df1.show()

### Events per User per Day. 
### Explanation : Average of total number of events for each unique user seen in a day. 

sql3 = '''
select count(device_id)/count(distinct device_id) as average_events ,date(timestamp) from events group by date(timestamp)
'''
df1=spark.sql(sql3)
df1.show()

### Distribution of Horizontal Accuracy. 
### Explanation : Calculate the average horizontal accuracy for each User and sort them into  the following bins.  

sql4 = '''select HA_Range,total_events,percentage,sum(percentage) OVER (order by HA_Range) as cumulative_percentage from (select HA_Range, count(*) as total_events, cast(100 * count(*) /(select count(*) from events where horizontal_accuracy  between 0  and 100) as numeric(10, 2)) as percentage
from
(
 select case when horizontal_accuracy  between 0  and 5 then " 0 to 5"
 when horizontal_accuracy  between 6  and 10 then " 6 to 10"
 when horizontal_accuracy  between 11 and 25 then "11 to 25"
 when horizontal_accuracy  between 26 and 50 then "26 to 50"
 when horizontal_accuracy  between 51 and 100 then "51 to 100"
 end as HA_Range
 from events
)
group by HA_Range having HA_Range is not null order by HA_Range) '''

df1=spark.sql(sql4)
df1.show()
