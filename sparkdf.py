

from pyspark.sql.functions import pandas_udf,PandasUDFType,udf,to_date,col,date_format
import time

def sparkdf(spark,sc,movies_path,users_path,ratings_path):
    print('''
    These are queries we need solve using Spark DATFRAME Api
    1. Prepare Movies data: Extracting the Year and Genre from the Text
    2. Prepare Users data: Loading a double delimited csv file
    3. Prepare Ratings data: Programmatically specifying a schema for the data frame
    4. Import Data from URL: Scala
    5. Save table without defining DDL in Hive
    6. Broadcast Variable example
    7. Accumulator example
    ''')
    movies1=spark.read.csv(movies_path,sep='::').toDF('movie_id','title','genre')
    from pyspark.sql.functions import array
    @udf
    def get_array(str1):
        return str1.split('|')
    @udf('string')
    def getting_Year(str1):
        return str1.split()[-1]
    movies1.select(date_format(to_date(getting_Year(col('title')),'(yyyy)'),'yyyy').alias('year'),col('title').alias('title'),'movie_id',get_array(col('genre')).alias('genre')).show(50,False)
    print('__________________Prepare Movies data: Extracting the Year and Genre from the Text_____________________')
    print('done with extracting year and genre')
    print('____________________________________________________________________________________________________________________________')
    time.sleep(5)
    print('_________________Prepare Users data: Loading a double delimited csv file__________')
    from pyspark.sql.types import StructType,StructField,IntegerType,StringType
# user_id,gender,age,occupation,zip
    schema=StructType([StructField('user_id',IntegerType(),True),StructField('gender',StringType(),True),StructField('age',IntegerType(),True),StructField('occupation',IntegerType(),True),StructField('zip_code',IntegerType(),True)])
    users1=spark.read.csv(users_path,sep='::',schema=schema)
    users1.show(50,False)
    time.sleep(5)
    print('_________Prepare Ratings data: Programmatically specifying a schema for the data frame_______')
    ratings_schema=StructType([StructField('user_id',IntegerType(),True),StructField('movie_id',IntegerType(),True),StructField('rating',IntegerType(),True),StructField('timestamp',StringType(),True)])
# user_id,movie_id,rating,timestamp
    ratings_=spark.read.csv(ratings_path,sep='::',schema=ratings_schema)
# import data from url 
    ratings_.createOrReplaceTempView('ratings_1')
    print('This is the schema')
    print(ratings_schema)
    time.sleep(5)
    print('_______________________________loaidng data from url____________________________________')
    url = "https://raw.githubusercontent.com/Thomas-George-T/Movies-Analytics-in-Spark-and-Scala/master/Movielens/users.dat"
    from pyspark import SparkFiles
    #spark.sparkContext.addFile(url)


    #spark.read.csv("file://"+ SparkFiles.get("users.dat"),sep='::').show(10)
    
    print(spark.catalog.currentDatabase())
    print(spark.catalog.listTables())
    time.sleep(5)
    print('create a tbale without using ddl in Hive')
    time.sleep(5)
    print('We can create a table in Hive without using DDl by spark.catalog.createTable')
    time.sleep(5)
    spark.sql('drop table if exists ddl_table')
    #spark.read.csv(ratings_path,sep='::').write.mode('overwrite').parquet('/user/itv003220/ratings_p')

    spark.catalog.createTable(tableName='ddl_table',schema=ratings_schema,source='parquet')

    print('table has been created without hive ddl')
    time.sleep(5)
    print('as you can see below')
    print(spark.catalog.listTables())
    time.sleep(5)
    ratings_.write.mode('overwrite').insertInto('ddl_table')
    spark.sql('select * from ddl_table').show(10)
    print('Example of broadcast variable')
    time.sleep(5)
    dic={
     0:  "other", 
     1:  "academic/educator",
     2:  "artist",
     3:  "clerical/admin",
      4:  "college/grad student",
    5:  "customer service",
    6:  "doctor/health care",
    7:  "executive/managerial",
    8:  "farmer",
    9:  "homemaker",
    10:  "K-12 student",
    11:  "lawyer",
    12:  "programmer",
    13:  "retired",
    14:  "sales/marketing",
    15:  "scientist",
    16:  "self-employed",
    17:  "technician/engineer",
    18:  "tradesman/craftsman",
    19:  "unemployed",
    20:  "writer"}
    broad_cast=sc.broadcast(dic)
    @udf
    def get_occu(int1):
        return broad_cast.value[int1]
    print('using broadcast to to get occupation')
    time.sleep(3)
    users_=spark.read.csv(users_path,sep='::',schema=schema)
    users_accu=users_.select(get_occu(col('occupation')).alias('actual_occupation'),col('user_id'))
    users_accu.show(20,False)
    time.sleep(5)
    print('example of accumulators')
    accu_pro=sc.accumulator(0)

    @udf
    def getting_progr(str1):
        if str1=="programmer":
            accu_pro.add(1)
            return True
        else:
            return False
    users_accu.filter(getting_progr(col('actual_occupation'))==True).show()
    print('print the acumulator value')
    time.sleep(3)
    print(accu_pro.value)
        
        








