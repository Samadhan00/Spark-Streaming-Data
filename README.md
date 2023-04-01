# Spark-Streaming-Data
The program is used to run multiple server data


from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
host="jdbc:mysql://mysqldb.cbaszrfvzjbi.ap-south-1.rds.amazonaws.com:3306/mysqldb"   // create from EC2
driver="com.mysql.jdbc.Driver"   // driver for JDBC
spark = SparkSession.builder.master("local[*]").config("spark.driver.allowMultipleContexts", "true").appName("test").getOrCreate()  
#spark.driver.allowMultipleContexts = it is used to run multiple server.

spark.conf.set("spark.driver.allowMultipleContexts", "true")
ssc = StreamingContext(spark.sparkContext,10)  // 10 is used for to store data after 10 seconds.
#spark.sparkContext.setLogLevel("ERROR")
#10 is batch intervel wait 10 seconds within 10 sec how much data u got that data make as a batch
# Create a DStream that will connect to hostname:port, like localhost:9999
#somethign server generate live data from specified (1234) port number, read dataf from server create dstream
#sockTextStream .. spark read data from terminal/console

lines = ssc.socketTextStream("ec2-3-111-53-67.ap-south-1.compute.amazonaws.com",1122) // the path taken from EC2 and port number created in Security groups.
#lines.pprint()  // pprint() command uesd in Pyspark
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]
#process that live data
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        df = rdd.map(lambda x:x.split(",")).toDF(['name','age','city'])
        df.show()
        delh=df.where(col("city")=="del") // creating a seperate table delh to store only delh data
        hyd=df.where(col("city")=="hyd")  // creating a seperate table hyd to store only hyd data
        delh.write.mode("append").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword").option("dbtable","delhirecords").option("driver",driver).save()
        hyd.write.mode("append").format("jdbc").option("url", host).option("user", "myuser").option("password","mypassword").option("dbtable", "hyderabadrecords").option("driver", driver).save()


    except:
        pass

lines.foreachRDD(process)
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate


#Context
#spark streaming context only entry point to connect streaming data, it generate Dstream api
#sparkContext it's entryy poin to do anything in spark. it's create rdd api
#sparkSesssion is entry point to process structure semi strucre or unstructure data, it create datafrmae and dataset


processing__ = '''
what is Streaming?
Data is moving state, means data always moving from one place to another place called streaming data.
but spark by default support only batch data (data is in not moving state) like hadoop.
Thats y .. spark internally use one lib called spark streaming lib.
if any streaming data, wait few seconds (5, 10 sec) within this time u ll get some amount/ 10 MB data, that 10MB data goes to memory and process immediately.
means it's 10 sec old data, not live data...it's called micro batch processing or near streaming data processing.'''


# nc -lk 1122 == 'nc -lk' command open in ec2 ubuntu terminal  and  1122 is port number in security group created in a EC2
# it is mandatoty to use port number while updating in a data
