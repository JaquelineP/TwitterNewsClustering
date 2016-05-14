import sqlite3
import nltk
import re
from sklearn import cluster, datasets
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import matplotlib.pyplot as plt


database_name = "twitter.db"
unique_words = set()

def main():
    global unique_words
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    tweet_count = 0
    for row in cur.execute("SELECT text FROM tweets"):
        # strip URLs from text
        text = re.sub(r"(?:https?\://)\S+", "", row[0])
        tokens = nltk.word_tokenize(text)
        for token in tokens:
            unique_words.add(token)
        tweet_count += 1
   
    word_count = len(unique_words)
    print("finished retrieving unique word count")

    data = []
    unique_words = []
    count = 0
    for row in cur.execute("SELECT text FROM tweets"):
        text = re.sub(r"(?:https?\://)\S+", "", row[0])
        tokens = nltk.word_tokenize(text)
        tweet_data = [0] * word_count
        for token in tokens:
            if token in unique_words:
                index = unique_words.index(token)
            else:
                index = len(unique_words)
                unique_words.append(token)
            tweet_data[index] += 1
        data.append(tweet_data)
        count += 1

        if count % 100 == 0:
            print("%i tweets remaining" % (tweet_count-count))

    print("finished initializing data vector")
    k_means = cluster.KMeans(n_clusters=10)
    k_means.fit(data)
    print(k_means.labels_)

def clean_tweet(tweet):
    return re.sub(r"(?:https?\://)\S+", "", tweet)

def cluster_data():

    # fetch and clean tweets from DB
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    cur.execute("SELECT text FROM tweets")
    tweets = [clean_tweet(tweet_row[0]) for tweet_row in cur.fetchall()]


    # create feature vector per tweet
    vectorizer = TfidfVectorizer(max_df=0.5,
                                 min_df=2, stop_words='english',
                                 use_idf=True)

    vectors = vectorizer.fit_transform(tweets)

    # run k means
    k_means = cluster.KMeans(n_clusters=10)
    k_means.fit(vectors)
    k_means_labels = k_means.labels_
    k_means_cluster_centers = k_means.cluster_centers_
    k_means_labels_unique = np.unique(k_means_labels)

    print(k_means.labels_)


def get_tf_idf_vectors(tweets):
    vectorizer = TfidfVectorizer(max_df=0.5,
                                 min_df=2, stop_words='english',
                                 use_idf=True)


    return vectorizer.fit_transform(tweets)

if __name__ == "__main__":
    cluster_data()
