from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.ml.feature import Tokenizer
# import matplotlib.pyplot as plt
import re
import csv
import StringIO
import sys
from math import sqrt

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes

split_regex = r'\W+'

def isANumber(str):
    try:
        float(str)
        return True
    except ValueError:
        return False

def loadRecord(line):
    """
    Parses the records using CSV reader and returns as dict fields
    :param line:
    :return:
    """
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, fieldnames=["productId", "userId", "summary", "text", "score"])
    rec =reader.next()
    # print ("rec:", rec, len(rec.get("productId")), len(rec.get("userId")), len(rec.get("summary")), len(rec.get("text")))

    if rec is not None:
        if (rec.get("productId", "") is None \
            or rec.get("userId", "") is None \
            or rec.get("summary", "") is None \
            or rec.get("text", "") is None \
            or rec.get("score", "") is None):
        # if (len(rec) != 5):
            return (rec, 0)
        else:
            if isANumber(rec.get("score", "")):
                return (rec, 1)
            else:
                return (rec, 0)
    else:
        return (rec, 0)

def removeStopWords(tokenList):
    stopWords = ['a', 'the', 'of', 'and', 's', 'this', 'is', 'it', 'i', 'to', 'my', 'on', 'you', 'for']
    return [w for w in tokenList if not w in stopWords]


def tokenizer(string):
    """ A simple tokenization implementaion
        break the string into tokens based on the regex splitter
    Args:
        string (str): input string
    Returns:
        list: a list of tokens
    """
    return removeStopWords([x for x in re.split(split_regex, string.lower()) if x != ''])


if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "Sentiment Classifier")

    # read the dataset and create an RDD excluding the header line
    input = sc.textFile("amazonreviews/movies-reviews.csv")
    header = input.take(1)[0]
    lineslarge = input.filter(lambda line: line != header)

    # Create a smaller dataset from the large sample for Test and experimentation purposes
    lines = lineslarge.sample(False,0.1)

    # parse and load the records, filtering out bad records
    #   create reviews rdd with label and text
    parsedRecords = lines.map(lambda line: loadRecord(line.encode('utf-8'))).cache()
    reviews = parsedRecords.filter(lambda s: s[1] == 1).map(lambda s: s[0]).cache()
    badrecs = parsedRecords.filter(lambda s: s[1] == 0).map(lambda s: s[0])

    print ("Good recs:" ,reviews.count())
    # print ("Bad recs:" ,badrecs.count())


    # split the dataset to training and test data
    trainingRDD, testRDD = reviews.randomSplit([8, 2], seed=0L)

    # Create a trainingRDD with the labels, and the review text
    labelsTrainingRDD = trainingRDD.map(lambda fields:fields.get("score")).map(lambda score: (1 if float(score) >= 3 else 0 ))
    reviewTextTokensTrainingRDD = trainingRDD.map(lambda fields:fields.get("summary")).map(lambda text: tokenizer(text))

    # Create a testRDD with the labels, and the review text
    labelsTestRDD = testRDD.map(lambda fields:fields.get("score")).map(lambda score: (1 if float(score) >= 3 else 0 ))
    reviewTextTokensTestRDD = testRDD.map(lambda fields:fields.get("summary")).map(lambda text: tokenizer(text))

    # #### 3.a. Explore the parsed dataset
    # lets look at a couple of reviews
    reviewTextTokensTrainingRDD.take(5)

    # how many unique tokens
    tokenCounts = reviewTextTokensTrainingRDD.flatMap(lambda text: text).map(lambda token: (token, 1)).reduceByKey(
        lambda x, y: x + y)
    print tokenCounts.count()

    # what are the top 10 tokens
    tokenCounts.sortBy(lambda x: -x[1]).take(30)

    # lets look at the words most appearing in positive sentiments and words most appearing in negative sentiments
    positiveTokens = labelsTrainingRDD.zip(reviewTextTokensTrainingRDD).flatMapValues(lambda x: x).filter(
        lambda x: x[0] == 1)
    negativeTokens = labelsTrainingRDD.zip(reviewTextTokensTrainingRDD).flatMapValues(lambda x: x).filter(
        lambda x: x[0] == 0)
    positiveTokenCounts = positiveTokens.map(lambda l: (l[1], 1)).reduceByKey(lambda x, y: x + y)
    negativeTokenCounts = negativeTokens.map(lambda l: (l[1], 1)).reduceByKey(lambda x, y: x + y)

    print "Top Positive tokens:", positiveTokenCounts.sortBy(lambda x: -x[1]).take(10)
    print "Top Negative tokens:", negativeTokenCounts.sortBy(lambda x: -x[1]).take(10)


    # Create TF-IDF model features on the training review text data
    tfTrain = HashingTF().transform(reviewTextTokensTrainingRDD)
    idfTrain = IDF().fit(tfTrain)
    tfidfTrain = idfTrain.transform(tfTrain)

    # Combine the labels with the TF-IDF feature vectors
    training = labelsTrainingRDD.zip(tfidfTrain).map(lambda x: LabeledPoint(x[0], x[1]))

    # Now train a naive bayes classifier, using the TF-IDF feature set
    model = NaiveBayes.train(training)

    # Create TF-IDF model features on the test review text data
    tfTest = HashingTF().transform(reviewTextTokensTestRDD)
    # idfTest = IDF().fit(tfTest)
    tfidfTest = idfTrain.transform(tfTest)

    #Lets predict out accuracy
    # predictedRDD =  labelsTrainingRDD.zip(model.predict(tfidfTrain)).map(lambda x: {"actual": x[0], "predicted": x[1]})

    predictedRDD =  labelsTestRDD.zip(model.predict(tfidfTest)).cache()
    squaredError = predictedRDD.map(lambda x: (x[0] - x[1])**2).reduce(lambda x,y: x+y)
    sampleCount =  predictedRDD.count()

    accuracy = 1.0 * predictedRDD.filter(lambda x: x[0] == x[1]).count() / sampleCount
    print ("Accuracy:", accuracy, ", Predicted on the test set of count:", sampleCount)

    # ### 8. Explore the inaccuracies

    # In[89]:

    incorrectPredictions = predictedRDD.zip(testRDD.map(lambda fields: fields.get("summary"))).filter(
        lambda x: x[0][0] != x[0][1])

    # In[93]:

    print "Total incorrect predictions:", incorrectPredictions.count()
    incorrectPredictions.map(
        lambda x: {"Actual sentiment": x[0][0], "Predicted sentiment": x[0][1], "Review text": x[1]}).take(30)
