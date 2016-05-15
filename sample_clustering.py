import sqlite3
import nltk
import re
from sklearn import cluster, datasets
from sklearn.decomposition import PCA, TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
from prettytable import PrettyTable
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer
from collections import Counter

database_name = "twitter.db"
unique_words = set()


class Clustering:
    def __init__(self):
        self.conn = sqlite3.connect(database_name)
        self.vectorizer = TfidfVectorizer(max_df=0.5,
                                          min_df=2, stop_words='english',
                                          use_idf=True)

    # def main(self):
    #     global unique_words
    #     cur = self.conn.cursor()
    #     tweet_count = 0
    #     for row in cur.execute("SELECT text FROM tweets"):
    #         # strip URLs from text
    #         text = re.sub(r"(?:https?\://)\S+", "", row[0])
    #         tokens = nltk.word_tokenize(text)
    #         for token in tokens:
    #             unique_words.add(token)
    #         tweet_count += 1
    #
    #     word_count = len(unique_words)
    #     print("finished retrieving unique word count")
    #
    #     data = []
    #     unique_words = []
    #     count = 0
    #     for row in cur.execute("SELECT text FROM tweets"):
    #         text = re.sub(r"(?:https?\://)\S+", "", row[0])
    #         tokens = nltk.word_tokenize(text)
    #         tweet_data = [0] * word_count
    #         for token in tokens:
    #             if token in unique_words:
    #                 index = unique_words.index(token)
    #             else:
    #                 index = len(unique_words)
    #                 unique_words.append(token)
    #             tweet_data[index] += 1
    #         data.append(tweet_data)
    #         count += 1
    #
    #         if count % 100 == 0:
    #             print("%i tweets remaining" % (tweet_count-count))
    #
    #     print("finished initializing data vector")
    #     k_means = cluster.KMeans(n_clusters=10)
    #     k_means.fit(data)
    #     print(k_means.labels_)

    def clean_tweet(self, tweet):
        return re.sub(r"(?:https?\://)\S+", "", tweet)

    def get_tweet_ids_with_text(self):

        cur = self.conn.cursor()
        cur.execute("SELECT id, text FROM tweets")
        tweet_rows = cur.fetchall()
        tweet_ids = [tweet_row[0] for tweet_row in tweet_rows]
        tweet_texts = [self.clean_tweet(tweet_row[1]) for tweet_row in
                       tweet_rows]

        return tweet_ids, tweet_texts

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
        k_means = cluster.KMeans(n_clusters=75)
        k_means.fit(reduced_data)

        print("K-Means Clustering")
        self.update_db_cluster_labels("k_means_cluster_id",
                                      zip(tweet_ids, k_means.labels_))

        #
        # retrieve most often occurring words of centroids of biggest clusters
        #

        cluster_ids_sorted_by_size = [tuple[0] for tuple in Counter(k_means.labels_).most_common()]

        # get the centroids in the original non-reduced space
        # each centroid is an array of length = vocabulary size
        centroids = svd.inverse_transform(k_means.cluster_centers_)

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

    def run_DBSCAN(self, tweet_ids, vectors):

        # TODO: do dimension reduction here first
        dbscan = cluster.DBSCAN(eps=0.3, min_samples=5, metric='euclidean')
        dbscan.fit(vectors)

        print("DBSCAN Clustering")
        self.update_db_cluster_labels("dbscan_cluster_id",
                                      zip(tweet_ids, dbscan.labels_))


    def run_algorithms(self):

        tweet_ids, tweet_texts = self.get_tweet_ids_with_text()

        # create feature vector per tweet
        vectors_sparse = self.vectorizer.fit_transform(tweet_texts)

        # run clustering algorithms
        self.run_k_means(tweet_ids, vectors_sparse)


if __name__ == "__main__":
    clustering = Clustering()
    clustering.run_algorithms()
