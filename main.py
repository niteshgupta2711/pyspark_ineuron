from utils.get_spark import get_spark
from rdd import solving_queries_rdd
from sparksql import sql
from sparkdf import sparkdf
import os

def main():
    spark=get_spark(os.environ.get('ENV'),'movie_Analytics',12998)
    sc=spark.sparkContext
    print(os.environ.get('ENV'))
    solving_queries_rdd(sc,movies_path=os.environ.get('MOVIES_PATH'),ratings_path=os.environ.get('RATINGS_PATH'))
    sql(spark,os.environ.get('MOVIES_PATH'),os.environ.get('RATINGS_PATH'),os.environ.get('USERS_PATH'))
    sparkdf(spark,sc,os.environ.get('MOVIES_PATH'),os.environ.get('USERS_PATH'),os.environ.get('RATINGS_PATH'))
    spark.stop()
    print('SPARK SESSION HAS BEEN STOPPED')


    




if __name__  == '__main__':
    main()