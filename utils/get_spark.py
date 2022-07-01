from pyspark.sql import SparkSession
import os 
def get_spark(env,appName,port):
    if os.environ.get('ENV')=="DEV":

        spark=SparkSession.builder.appName(appName).master('local').config('spark.ui.port',str(port)).enableHiveSupport().getOrCreate()

        return spark
    elif os.environ.get('ENV')=='PROD':
        spark=SparkSession.builder.appName(appName).master('yarn').config('spark.ui.port',str(port)).enableHiveSupport().getOrCreate()

        return spark
