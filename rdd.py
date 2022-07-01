import time




def solving_queries_rdd(sc,ratings_path,movies_path):
    print('queries to be solved by using rdd')
    print(''' 
    1. What are the top 10 most viewed movies?
    2. What are the distinct list of genres available?
    3. How many movies for each genre?
    4. How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?
    5. List the latest released movies
    
    ''')
    time.sleep(5)
    top_10=sc.textFile(ratings_path).map(lambda x:x.split('::')).map(lambda x:(x[1],1)).map(lambda x:(int(x[0]),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
    movie_title=sc.textFile(movies_path).map(lambda x:x.split('::')).map(lambda x:(int(x[0]),x[1]))
    all_top_movies=movie_title.join(top_10)
    all_top=all_top_movies.sortBy(lambda x:x[1][1],ascending=False).toDF()
    all_top.show(10)

    print('_____________________TOP 10 MOST VIEWED MOVIES___________________________')
    time.sleep(5)
    print('What are the distinct list of genres available?')
    time.sleep(5)
    genres=sc.textFile(movies_path).map(lambda x:x.split('::')[2].split('|')).flatMap(lambda x:x)
    print(set(genres.collect()))
    print('_________________distinct list of genres______________________')
    time.sleep(5)
    print('how many movies for each genre')
    time.sleep(5)
    no_of_movies_each_genre=genres.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).toDF(schema='Genre string,no int')
    no_of_movies_each_genre.show()
    time.sleep(5)
    
    import re

    pattern_n=re.compile('^\d+')

    def movie_name_(str):
        if len(pattern_n.findall(str))>0:
            return ('startsWithDigit',1)
        else: return ('startsWithLetter',1)
    

    print('How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?')
    time.sleep(5)
    movie_name=sc.textFile(movies_path).map(lambda x:x.split('::')[1]).map(lambda x : movie_name_(x)).reduceByKey(lambda x,y:x+y).toDF()
    movie_name.show()
    time.sleep(5)
    print('List the latest released movies')
    latest=sc.textFile(movies_path).map(lambda x:x.split("::")).map(lambda x:x[1]).map(lambda x:(x,int(re.findall('\d+',x.split()[-1])[0])))
    latest.sortBy(lambda x:x[1],ascending=False).toDF().show(20)
    time.sleep(5)
    





