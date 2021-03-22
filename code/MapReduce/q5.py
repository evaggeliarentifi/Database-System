from pyspark.sql import SparkSession
from io import StringIO
import csv,time


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

t1 = time.time()

movies = sc.textFile("hdfs://master:9000/movies.csv") \
    .map(lambda line: split_complex(line)) \
    .map(lambda line: (int(line[0]), [line[0], line[1],line[7]]))  # ta morfopoiw gia to join

movies2 = movies

genres = sc.textFile("hdfs://master:9000/movie_genres.csv") \
    .map(lambda line: line.split(',')) \
    .map(lambda line: (int(line[0]), line[1]))

ratings = sc.textFile("hdfs://master:9000/ratings.csv") \
    .map(lambda line: line.split(',')) \
    .map(lambda line: (int(line[1]), line[0]))
#  movieid   userid
# (110, '1')
# (147, '1')

movie_genres = movies.join(genres)
# movieid   movieid   moviename    movie_popularity    genre
# (45325, (['45325', 'Tom and Huck' ,  10.85212], 'Action'))
# (45325, (['45325', 'Tom and Huck' ,   6.25849], 'Kati allo'))


genres_with_best_reviewer_with_number_of_reviews = ratings.join(movie_genres) \
    .map(lambda line: ((line[1][0], line[1][1][1]), 1)) \
    .reduceByKey(lambda x, y: x + y)
# edw kane reduce me key to userId wste na brw gia kathe userId posa reviews exei gia kathe genre p uparxei
# userid  genre       times_reviewed_in_that_genre
# (('71', 'Crime'), 2)
# (('176', 'Mystery'), 4)
#movieId, matchingGenre



genres_with_best_reviewer_with_number_of_reviews = genres_with_best_reviewer_with_number_of_reviews \
    .map(lambda line: (line[0][1], [line[0][0], line[1]])) \
    .reduceByKey(lambda x, y: x if x[1] > y[1] else y)\
    .sortByKey()
# edw kanw reduce me key to Genre gia na epileksw ton user p exei ta perissotera reviews
# genre         user_id   number_of_reviews
# ('TV Movie', ['45811', 38])
# ('Horror', ['45811', 350])

############### APO EDW KAI PANW SWSTA
# Exoume brei ton user gia kathe genre pou exei ta perissotera reviews
# twra prepei na broume gia kathe user, tin agapimeni tou tainia kai tin pio mi agapimeni tou
# apo to genre pou autos exei ta perissotera reviews


userId_genre_pairs = genres_with_best_reviewer_with_number_of_reviews\
    .map(lambda line:  (  int(line[1][0])  , line[0] ) )
#userId, Genre

# Apo ton pinaka me to genre kai ton xristi pou exei ta perissotera reviews
# pairnoume mono ta unique users
only_unique_users = genres_with_best_reviewer_with_number_of_reviews\
    .map(lambda line:  int(line[1][0]))\
    .distinct()\
    .map(lambda line: (line, 0 ))

# Den to xrisimopoioume
genres_with_best_reviewer2 = userId_genre_pairs\
    .map(lambda line : (line, 0 ))

# diabazoume to arxeio me ta ratings
ratings_of_only_the_best_reviewers = sc.textFile("hdfs://master:9000/ratings.csv") \
    .map(lambda line: line.split(',')) \
    .map(lambda line: (int(line[0]), (line[1],line[2] )  ))\

# den mas endiaferoun oloi oi xristes p exei to rating.csv, opote kratame mono gia autous pou niazomaste
# dld autous p jeroume oti se kathe genre exoun ta perissotera reviews --> only_unique_users (variable)
ratings_of_only_the_best_reviewers = only_unique_users.join(ratings_of_only_the_best_reviewers)\
    .map(lambda line : (  int(line[1][1][0])  ,  [      line[0],  float(line[1][1][1])   ]  ))
#movieId, [userid , rating]


# Apla diabazoume ta genres kai ta morfopoioume gia na ginei to join
genres2 = sc.textFile("hdfs://master:9000/movie_genres.csv")\
    .map(lambda line:line.split(','))\
    .map(lambda line : (int(line[0]),line[1]))

# Opote twra exoume ola ta reviews MONO twn xristwn pou mas endiaferei
# opote, tha kanoume join to panw table, wste gia kathe review, na mathoume to genre
# pou anoikei i tainia. Efoson 1 movie mporei na exei perissotera genres, tote tha
# exoume gia ena review, polla reviews to ena gia kathe genre!
ratings_of_only_the_best_reviewers_with_movie_genre = ratings_of_only_the_best_reviewers.join(genres2)\
    .map(lambda line: [line[1][0][0], line[0] , line[1][1], line[1][0][1]])
# Opote kataligoume se touples tis morfis ---> userId, movieId, genre, rating
# dld exoume gia kathe review polles N touples an N o arithmos ton genres p upokeitai i tainia tou review
# Upopsin an user me id =1, exei ta perissotera reviews gia genre = Action, tote o parapanw pinakas
# periexei kai reviews tou user me id = 1, ta reviews p exoun kai genre diaforetiko tou Action,

# tha kanoume ena
ratings_of_only_the_best_reviewers_with_movie_genre = ratings_of_only_the_best_reviewers_with_movie_genre.\
    map(lambda line: (line[1], line))
# twra einai tis morfis ( moveiID, [userId, movieId, genre, rating] )

ratings_of_only_the_best_reviewers_with_movie_genre = ratings_of_only_the_best_reviewers_with_movie_genre\
    .join(movies)\
    .map(lambda line : [ line[1][0][0], line[0], line[1][0][2], line[1][0][3], float(line[1][1][2]), line[1][1][1]  ]  )
# exoume userId, movieId, genre, rating, popularity, movieName


ratings_of_only_the_best_reviewers_with_movie_genre = ratings_of_only_the_best_reviewers_with_movie_genre\
    .map(lambda line :   ((line[0],  line[2]) , [line[1],line[3], 0 ,line[4], line[5] ]   ))\
# twra einai (userId, genre) , (movieId, rating, flag_has_same_review_for_more_than_one_movie, popularity ,movieName)  # gia kathe rating!!

resultForBestMovie = ratings_of_only_the_best_reviewers_with_movie_genre\
    .reduceByKey(lambda before,after : [after[0],after[1],0,after[3],after[4]] if before[1] < after[1] else \
                 [before[0], before[1],before[2],before[3],before[4]] if before[1] > after[1] else \
                 [after[0],after[1],1,after[3],after[4]] if before[3] < after[3] else \
                 [before[0],before[1],1,before[3],before[4]])
# Opote twra exw gia kathe user p me endiaferi tin agapimeni tou tainia GIA KATHE GENRE!!!
# Opote apla menei na kanw ena join me to userId_genre_pairs wste na kratisw auta p thelw mono
userId_genre_pairs2 = userId_genre_pairs.map(lambda line : (line, 0))
resultForBestMovie = resultForBestMovie.join(userId_genre_pairs2)\
    .map(lambda line: (line[0][1] , [ line]))\
    .join(genres_with_best_reviewer_with_number_of_reviews)\
    .sortByKey(True)\
    .map(lambda line : (line[0] ,  line[1][0][0][0][0],  line[1][0][0][1][0][4], line[1][0][0][1][0][1], line[1][1][1]    )  )


resultForWorstMovie = ratings_of_only_the_best_reviewers_with_movie_genre\
    .reduceByKey(lambda before,after : [after[0],after[1],0,after[3],after[4]] if before[1] > after[1] else \
                 [before[0], before[1],before[2],before[3],before[4]] if before[1] < after[1] else \
                 [after[0],after[1],1,after[3],after[4]] if before[3] < after[3] else \
                 [before[0],before[1],1,before[3],before[4]])
# Opote twra exw gia kathe user p me endiaferi tin agapimeni tou tainia GIA KATHE GENRE!!!
# Opote apla menei na kanw ena join me to userId_genre_pairs wste na kratisw auta p thelw mono
userId_genre_pairs2 = userId_genre_pairs.map(lambda line : (line, 0))
resultForWorstMovie = resultForWorstMovie.join(userId_genre_pairs2)\
    .map(lambda line: (line[0][1] , [ line]))\
    .join(genres_with_best_reviewer_with_number_of_reviews)\
    .sortByKey(True)\
    .map(lambda line : (line[0] ,  line[1][0][0][0][0],  line[1][0][0][1][0][4], line[1][0][0][1][0][1], line[1][1][1]    )  )



print(" ************************* ")


# for i in resultForBestMovie.take(300):
#     print(i)
resultForBestMovie = resultForBestMovie.take(300)
resultForWorstMovie = resultForWorstMovie.take(300)

print("%%%%%%%%%%%%%%%%%%%%%")

# for i in resultForWorstMovie.take(300):
#     print(i)

print("%%%%%%%%%%%%%%%%%%%%%")

for i in range(0, len(resultForBestMovie)):
    #         genre             userId, numberOfReviews  ,bestMovieName, bestMovieRating, worstMovieName, worstMovieRating
    t = (resultForBestMovie[i][0],  resultForBestMovie[i][1], resultForBestMovie[i][4], resultForBestMovie[i][2],resultForBestMovie[i][3],resultForWorstMovie[i][2],resultForWorstMovie[i][3] )
    print(t)


# print("####################")
# for i in final.take(900):
#     print(i)

t2 = time.time()
print("**************")
print("Total time: ", t2-t1)
