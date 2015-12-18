from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
import sys


if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "Dataset explorer")
    input = sc.textFile("/home/subhash/amazonreviews/movies-transformed.csv")
    header = input.take(1)[0]
    lines = input.filter(lambda line: line != header)

    reviews = lines.map(lambda line: line.split(",")).cache()
    # Basic stats

    num_reviews = reviews.count()
    num_movies = reviews.map(lambda fields:fields[0]).distinct().count()
    num_users = reviews.map(lambda fields:fields[1]).distinct().count()
    review_scores = reviews.map(lambda line:line[2]).cache()
    max_rating = review_scores.reduce(lambda x,y: max(x,y))
    min_rating = review_scores.reduce(lambda x,y: min(x,y))

    print "Total reviews: %d, Num Users: %d, Num Movies: %d" % (num_reviews, num_users, num_movies)
    print (max_rating, min_rating)

    countByRating = review_scores.map(lambda rating: (rating,1)).reduceByKey(lambda x,y: x+y)
    print "Count of Ratings by Score"
    scores = countByRating.take(10)
