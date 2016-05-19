import sqlite3
import nltk
import re
import operator
from collections import Counter
from sklearn import cluster, datasets
from sklearn.decomposition import PCA, TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
from prettytable import PrettyTable
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer
from collections import Counter
import matplotlib.pyplot as plt

database_name = "twitter.db"
CLUSTER_COUNT = 75

class Clustering:
    def __init__(self):
        self.conn = sqlite3.connect(database_name)
        self.vectorizer = TfidfVectorizer(max_df=0.5,
                                          min_df=2, stop_words='english',
                                          use_idf=True)
        self.stemmer = nltk.PorterStemmer()

    def preprocess_tweet(self, tweet):
        return self.stem_text(self.clean_tweet(tweet))

    def clean_tweet(self, tweet):
        # filter out URLs and mentions (@[...])
        return re.sub(r"(https?\S+| htt?p?s?…$|@\S+)", "", tweet)

    def stem_text(self, text):
        result = ""
        for word in text.split():
            result += self.stemmer.stem(word) + " "
        return result[:-1]

    def get_tweet_ids_with_text(self):

        cur = self.conn.cursor()
        cur.execute("SELECT id, text, k_means_cluster_id FROM tweets")
        tweet_ids, tweet_texts, cluster_ids, mapped_ids = [], [], [], []
        for row in cur:
            tweet = self.preprocess_tweet(row[1])
            if tweet in tweet_texts:
                index = tweet_texts.index(tweet)
                mapped_ids[index].append(row[0])
            else:
                tweet_ids.append(row[0])
                tweet_texts.append(tweet)
                cluster_ids.append(row[2])
                mapped_ids.append([])
        return tweet_ids, tweet_texts, cluster_ids, mapped_ids

    def update_db_cluster_labels(self, column_name, ids_with_labels):

        cur = self.conn.cursor()
        update_sql = '''
          UPDATE tweets
          SET %s=?
          WHERE id=?
        ''' % (column_name,)

        for tweet_id, label in ids_with_labels:
            cur.execute(update_sql, (int(label), tweet_id))

        self.conn.commit()

        # get cluster counts
        sql = '''
          SELECT %s as "Cluster ID", COUNT(*) "Cluster Size"
          FROM tweets
          GROUP BY %s
          ORDER BY COUNT(*) DESC
          LIMIT 10
        ''' % (column_name, column_name)

        out_table = PrettyTable(["Cluster ID", "Cluster Size"])
        for row in cur.execute(sql):
            out_table.add_row(row)

        print(out_table)

    def print_words(self, svd, k_means):

        #
        # retrieve most often occurring words of centroids of biggest clusters
        #

        cluster_ids_sorted_by_size = [tuple[0] for tuple in Counter(k_means.labels_).most_common()]

        # get the centroids in the original non-reduced space
        # each centroid is an array of length = vocabulary size
        #centroids = svd.inverse_transform(k_means.cluster_centers_)
        centroids = k_means.cluster_centers_

        # sort centroids by the number of elements in their cluster
        centroids_sorted = centroids[cluster_ids_sorted_by_size]

        # sort each centroid's tf-idf vector and get the indices --> the higher the tf-idf
        # value the more important the word
        word_indices_asc = centroids_sorted.argsort()
        word_indices_desc = word_indices_asc[:, ::-1]
        terms = self.vectorizer.get_feature_names()

        # print most often occurring words of 10 biggest clusters
        for i in range(10):
            print("Cluster %d:" % cluster_ids_sorted_by_size[i])
            centroid_word_indices = word_indices_desc[i]
            for index in centroid_word_indices[:10]:
                print('[term, tf-idf]: %s, %s' % (terms[index], centroids_sorted[i][index]))
            print()

    def plot(self, k_means, reduced_data):

        plt.figure(1)
        plt.clf()
        plt.scatter(reduced_data[:, 0], reduced_data[:, 1], c=k_means.labels_)
        min_x = -2
        max_x = 2
        min_y = -2
        max_y = 2
        plt.axis([min_x, max_x, min_y, max_y])
        plt.show()

    def evaluate_clusters(self):

        clusters = [[] for i in range(CLUSTER_COUNT)]
        mapped_count = [0] * CLUSTER_COUNT
        _, tweet_texts, cluster_ids, mapped_tweets = self.get_tweet_ids_with_text()
        for i in range(len(tweet_texts)):
            clusters[cluster_ids[i]].append(tweet_texts[i])
            mapped_count[cluster_ids[i]] += len(mapped_tweets[i])

        tweet_vectorizer = TfidfVectorizer(stop_words='english', use_idf=True)
        scores = {}
        for i in range(CLUSTER_COUNT):
            words = []
            score = 0
            cluster_size = len(clusters[i])
            for j in range(cluster_size):
                try:
                    tweet_vectorizer.fit([clusters[i][j]])
                except ValueError:
                    # tweet contains only stopwords
                    continue
                words += tweet_vectorizer.get_feature_names()
            word_count = len(words)
            words = Counter(words)

            similarity = 0
            for j in range(cluster_size):
                try:
                    tweet_vectorizer.fit([clusters[i][j]])
                except ValueError:
                    continue
                for word in tweet_vectorizer.get_feature_names():
                    similarity += words[word]
                similarity /= word_count
                score += similarity

            score /= cluster_size
            if cluster_size > 1:
                scores[i] = score

        scores = sorted(scores.items(), key=operator.itemgetter(1))
        scores.reverse()
        for i in range(CLUSTER_COUNT // 4):
            print("id: %i, score: %.2f, size: %i, duplicates: %i" 
                % (scores[i][0], scores[i][1], len(clusters[scores[i][0]]), mapped_count[scores[i][0]]))

    def fill_missing_cluster_ids(self):

        cur = self.conn.cursor()
        sql = "UPDATE tweets SET k_means_cluster_id = ? WHERE id = ?"

        _, _, cluster_ids, mapped_ids = self.get_tweet_ids_with_text()
        for i, cluster in enumerate(cluster_ids):
            for mapped_id in mapped_ids[i]:
                cur.execute(sql, (cluster, mapped_id))

        self.conn.commit()

    def run_k_means(self, tweet_ids, vectors):

        # PCA as alternative???
        # vectors_dense = vectors.todense()
        # reduced_data = PCA(n_components=3).fit_transform(vectors_dense)

        # reduce dimensionality of vectors with LSA
        svd = TruncatedSVD(n_components=2)
        normalizer = Normalizer(copy=False)
        lsa = make_pipeline(svd, normalizer)
        reduced_data = lsa.fit_transform(vectors)

        # run k-means
        k_means = cluster.KMeans(n_clusters=CLUSTER_COUNT)
        #k_means.fit(reduced_data)
        k_means.fit(vectors)

        print("K-Means Clustering")
        self.update_db_cluster_labels("k_means_cluster_id",
                                      zip(tweet_ids, k_means.labels_))

        self.print_words(svd, k_means)
        self.evaluate_clusters()
        self.fill_missing_cluster_ids()
        #self.plot(k_means, reduced_data)



    def run_DBSCAN(self, tweet_ids, vectors):

        # TODO: do dimension reduction here first
        dbscan = cluster.DBSCAN(eps=0.3, min_samples=5, metric='euclidean')
        dbscan.fit(vectors)

        print("DBSCAN Clustering")
        self.update_db_cluster_labels("dbscan_cluster_id",
                                      zip(tweet_ids, dbscan.labels_))


    def run_algorithms(self):

        tweet_ids, tweet_texts, _, _ = self.get_tweet_ids_with_text()

        # create feature vector per tweet
        vectors_sparse = self.vectorizer.fit_transform(tweet_texts)

        # run clustering algorithms
        self.run_k_means(tweet_ids, vectors_sparse)


if __name__ == "__main__":
    clustering = Clustering()
    clustering.run_algorithms()
