import time


def sql(spark,movies_path,ratings_path,users_path):
    print('''
    These are the queries we are solve using spark sql
    1. Create tables for movies.dat, users.dat and ratings.dat: Saving Tables from Spark SQL
    2. Find the list of the oldest released movies.
    3. How many movies are released each year?
    4. How many number of movies are there for each rating?
    5. How many users have rated each movie?
    
    
    ''')
    time.sleep(5)
    spark.sql('drop database if exists movies_ cascade')
    spark.sql('create database if not exists movies_ location "/user/itv003220/warehouse/"')
    spark.sql('use movies_')
    spark.sql('''
        create  table ratings (user_id string ,movie_id string,ratings string,time_stamp string) 
        row format 
        delimited fields terminated by '::'
            stored as textfile
            location "/user/itv003220/warehouse/movies_"''')


    spark.sql('''
    create table movies (movie_id string,title STRING,genre STRING)
    row format
        delimited fields terminated by "::"
            stored as textfile
            location "/user/itv003220/warehouse/movies_"
    ''')

# we have user_id,gender,age,OCcupation ,zip_code

    spark.sql('''
    create table users (user_id string,gender STRING,age string,ouccupation string,zip_code STRING)
    row format
        delimited fields terminated by "::"
            stored as textfile
            location "/user/itv003220/warehouse/movies_"
    ''')
    print(spark.catalog.currentDatabase())
    print('____________the above you see the data base we are using____________')
    print('_____________________________________________________________________')
    print('___________________Here are the tables we created using spark sql______________________')
    print('________________________________________________________________________________________')
    print(spark.catalog.listTables())
    time.sleep(5)
    spark.read.csv(movies_path,sep='::').write.insertInto('movies')
    spark.read.csv(users_path,sep='::').write.insertInto('users')
    spark.read.csv(ratings_path,sep='::').write.insertInto('ratings')
    print('__________________________data has been inserted in to tables _______________________')
    print('______________________________________________________________________________________')
    time.sleep(5)
    from pyspark.sql.functions import pandas_udf,PandasUDFType,udf,to_date,col,date_format
    @udf('string')
    def getting_Year(str1):
        return str1.split()[-1]


    print('_________________list oldest movies _____________________')

    moviesM=spark.read.table('movies')
    moviesM.select(date_format(to_date(getting_Year(col('title')),'(yyyy)'),'yyyy').alias('year'),'title').filter(col('year')>1).orderBy(col('year')).show(20,False)
    print('________________________________________________________________________________________________________________________')
    time.sleep(5)
    print('___________________________How many movies are released each year?________________________')
    moviesM.select(date_format(to_date(getting_Year(col('title')),'(yyyy)'),'yyyy').alias('year')).groupBy(col('year')).count().show(50,False)
    print('__________________________________________________________________________________________________________________________________________________')
    time.sleep(5)
    print('____________________how many movies for each rating_____________________')
    ratings=spark.read.csv(ratings_path,sep='::').toDF('user_id','movie_id','ratings','timestamp')
    ratings.createOrReplaceTempView('ratings_')
    spark.sql('select count(avg_ratings) as no_of_ratings,avg_ratings from (select round(avg(ratings)) as avg_ratings,movie_id from ratings_ group by movie_id) group by avg_ratings').show(100,False)
    print('__________________________________________________________________________________________________________________________')

    time.sleep(5)
    print('_________________________________No of ratings for each movie_______________________________')
    movies=spark.read.csv(movies_path,sep='::').toDF('movie_id','title','genre')
    movies.createOrReplaceTempView('movies_')
    spark.sql('''
        select b.title,a.no_of_ratings from (select movie_id,count(movie_id) as no_of_ratings from ratings_ group by movie_id) as a left join movies_ as b on
                a.movie_id=b.movie_id
                ''').show(50,False)
    print('____________________________________________________________________________________________________________________________________')
# no of ratings for each movie
    


# how many movies for each ratings




