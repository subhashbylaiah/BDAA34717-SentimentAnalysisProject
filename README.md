**Introduction**

For this Project we will build a model to perform Sentiment analysis of user provided reviews on a given topic or subject. Sentiment analysis, also referred to as "Opinion mining", refers to the application of computational methodologies (such as statistical modeling techniques, text analysis, natural language processing techniques, machine learning models, computational linguistics) to the purposes of classifying text to identify and predict the overall sentiment of a given text input.

**Approach**

For the purposes of this project we have used the Amazon movie reviews dataset available on the Stanford data repository http://snap.stanford.edu/data/web-Movies.html](http://snap.stanford.edu/data/web-Movies.html)). The input dataset has over a 7 million review texts and summary, from over 800,000 users, for about 250,000+ movies.

To build a classifier model we use the Naïve Bayes Classifier implementation available as part of the Apache Spark MLLib module. The Naïve Bayes model is a probabilistic classification model based on the Bayes rule which predicts the probability of the Class of an input based on the conditional probabilities of the individual features observed in the input. The Naïve'ness about the algorithm is that it makes a strong assumption that the input features are independent of each other to enable application of the Bayes rule.

We split the dataset into training and validation sets. We first preprocess the dataset to perform any cleanup and transform the data to necessary formats from which the features can be built. Then we use this data to train a NBC classifier, which will then be used to make predictions on newer data.

We split the data into a training set and test set at a ratio of 8:2, where we will use the training set to train the model. We will evaluate the trained model


**A little bit about Naïve Bayes Classifier**

Naive Bayes classifier is a linear classifier that works on the Bayes theorem, which states that the probability of a Hypothesis (H) based on a given set of Evidences (E) will be equal to the product of the Probability of the Hypothesis and the conditional Probability of the Evidences given the Hypothesis, divided by the probability of the Evidences.Naïve Bayes can be used for purposes of text classification and has been found to be quite efficient and fast.
Naïve Bayes theorem makes a strong assumption that the input attributes are independent of each other, and so the above probability can be written as the product of individual conditional probabilities

**A brief about Apache Spark**

Apache Spark is a Distributed execution engine for Big Data Processing that provides efficient abstractions for processing large datasets in memory. Spark is very efficient for workloads such iterative algorithms and interactive data mining; algorithms that need to reuse data in-between computations. Spark implements in-memory fault tolerant data abstractions in the form of RDDs (Resilient Distributed Datasets), which are parallel data structures stored in memory. RDDs provide fault-tolerance by tracking transformations (lineage) rather than changing actual data. In case a partition has to be recovered after loss, the transformations needs to be applied on just that dataset. This is far more efficient than replicating datasets across nodes for fault-tolerance, which claims to be 100x faster than Hadoop MR.

**The Pipeline in Detail**

**Preprocessing:**

Process raw movie reviews dataset which has one line per attribute for the reviews, and transform and save into a CSV format where each line corresponds to one review. For sentiment analysis we pick up only the review/summary, review/text, and Review Score). As the file is large, we will read line by line and write out to corresponding output file.

**Data cleanup and processing:**

We parse the input dataset and validate of the records have the necessary attributes, do basic sanity checks and classify the records as either good or bad. The good records are taken up for further processing.

**Data preparation and transformations**

We then split the dataset into a Training set and Test set. We will create training RDD and Test RDD which will contain the labeled data points and the review texts (which comes from the summary). During this process we also tokenize the dataset to split the input text into tokens, convert into lower case and also remove some of the stop words based on a list we create. We then create a weighted bag-of-words model using TF-IDF. TF (Term Frequency) is weighted at the document level (or to the individual input text) such that it assigns greater weights to tokens that appear more frequently within the document. IDF (Inverse Document Frequency) is weighted to at the corpus level and assigns more weights to tokens that appear less frequently. IDF kind of normalizes the commonly occurring words, so the stop words get lesser weights

**Model Evaluation**

We will use the simple accuracy of prediction to determine the accuracy of the model which is calculated as a percentage of the Number of predicted classes to the Number of

**Application and Environment configuration**

The program is written in Python and the spark application will be submitted to the framework using the spark-submit script, such as below.

/opt/spark/bin/spark-submit --master yarn-client --num-executors 4 --conf spark.driver.memory=4g --conf spark.executor.memory=4g share/sparkscripts/SentimentClassifier.py

**Running the application  -**

sbylaiah@ubuntu:~$ wget [https://snap.stanford.edu/data/movies.txt.gz](https://snap.stanford.edu/data/movies.txt.gz)
sbylaiah@ubuntu:~$ gzip -d movies.txt.gz

**Source code Artifacts**

** PreProcessing the raw dataset **

sbylaiah@ubuntu:~$ python PreProcessReviewsData.py

This uses the extracted file shown before, produces an output file called movies-reviews.csv in the same path as the input file

Then the Sentiment classifier script can be run

sbylaiah@ubuntu:~$ /opt/spark/bin/spark-submit --master yarn-client --num-executors 4 --conf spark.driver.memory=4g --conf spark.executor.memory=4g share/sparkscripts/SentimentClassifier.py

uses the output from PreProcess stage

**NOTE: The location of the raw dataset is specified within the python scripts itself**

**MORE PREFERABLY THE SENTIMENT CLASSIFIER CAN BE RUN AS A IPYTHON NOTEBOOK** 

The Sentiment classifier can be run as IPython notebook, for a more interactive experience. The code is interspersed with instructions for a self-driven guided execution. The python notebook is SentimentClassifier.ipynb

