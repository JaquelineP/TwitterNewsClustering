import json
import sqlite3
import os
import streaming
from queue import Queue
from threading import Thread
import time

tweets_data_path = 'sample_stream_sample.json'
database_name = "twitter.db"
dirty = ["delete", "status_withheld", "limit"]
OUTPUT_THRESHOLD = 100

class DataParser():
    count = 0
    prepared_sql = ""
    q = None
    start_time = 0
    file = None

    def initialize_connection(self):
        self.file = open('twitter.dat', 'a')
        create_db = (not os.path.isfile(database_name))
        conn = sqlite3.connect(database_name)
        cur = conn.cursor()
        cur.execute("PRAGMA synchronous = OFF")
        cur.execute("PRAGMA journal_mode = MEMORY")
        self.initialize_table(conn, cur, create_db)
        self.q = Queue()

    def initialize_table(self, conn, cur, create_db):
        output_columns = ["id", "text", "user_id", "user_name", "followers_count", "hashtags", "urls", "timestamp", "mentions", "k_means_cluster_id","dbscan_cluster_id" ]
        int_columns = (0, 2, 4, 7, 9, 10)

        # generate insert statement
        self.prepared_sql = ("INSERT INTO tweets VALUES (" + "?, " * (len(output_columns)))[:-2] + ")"

        # create table if necessary
        if create_db:
            sql = "CREATE TABLE tweets ("
            for i, column in enumerate(output_columns):
                sql += "%s %s, " % (column, "INT" if i in int_columns else "VARCHAR(255)")
            sql += "PRIMARY KEY(id))"
            cur.execute(sql)
            conn.commit()

    def insert_tweet_data(self, cur, tweet):
        entities = ["hashtags", "urls", "user_mentions"]
        entity_keys = ["text", "expanded_url", "screen_name"]
        entities_result = {}
        for index, entity in enumerate(entities):
            entities_result[entity] = None
            for e in tweet.get("entities", {}).get(entity, {}):
                if entities_result[entity] is None:
                    entities_result[entity] = ""
                entities_result[entity] += e.get(entity_keys[index], "") + ","
            if entities_result[entity]:
                entities_result[entity] = entities_result[entity][:-1]

        data = {
            "id": tweet["id"],
            "text" : tweet.get("text"),
            "user_id": tweet.get("user", {}).get("id"),
            "user_name": tweet.get("user", {}).get("screen_name"),
            "followers_count": tweet.get("user", {}).get("followers_count"),
            "hashtags": entities_result["hashtags"],
            "urls": entities_result["urls"],
            "timestamp": tweet["timestamp_ms"],
            "user_mentions" : entities_result["user_mentions"]
        }
        # insert into db
        try:
            cur.execute(self.prepared_sql, (
                tweet["id"],
                tweet.get("text"),
                tweet.get("user", {}).get("id"),
                tweet.get("user", {}).get("screen_name"),
                tweet.get("user", {}).get("followers_count"),
                entities_result["hashtags"],
                entities_result["urls"],
                tweet["timestamp_ms"],
                entities_result["user_mentions"],
                None,
                None
            ))
            self.file.write(json.dumps(data)+"\n")
        except sqlite3.IntegrityError:
            pass

    def read_data_from_queue(self):
        conn = sqlite3.connect(database_name)
        cur = conn.cursor() 
        cur.execute("PRAGMA synchronous = OFF")
        cur.execute("PRAGMA journal_mode = MEMORY")
        while True:
            item = self.q.get()
            self.q.task_done()
            self.process_data(conn, cur, item)

    def data_callback(self, data):
        self.q.put(data)

    def process_data(self, conn, cur, data):
        tweet = json.loads(data)

        found_dirty = False
        for d in dirty:
            if d in tweet:
                found_dirty = True
                break
        if found_dirty:
            return

        self.insert_tweet_data(cur, tweet)
        self.count += 1
        if self.count % OUTPUT_THRESHOLD == 0:
            print("inserted %i tweets (%.2f/s)" % (self.count, self.count / (time.time() - self.start_time)))
            conn.commit()
            self.file.flush()

    def read_newspapers(self):
        result = []
        with open("newspapers.txt", "r") as f:
            for line in f:
                paper = line[line.find("@") + 1:].strip()
                result.append(paper)
        with open("keywords.txt", "r") as f:
            for line in f:
                result.append(line)
        with open("domains.txt", "r") as f:
            for line in f:
                result.append(line)
        return result

    def start_streaming(self):
        t = Thread(target=self.read_data_from_queue)
        t.daemon = True
        t.start()
        self.start_time = time.time()
        streaming.start_streaming(self.data_callback, self.read_newspapers())

def main():
    data_parser = DataParser()
    data_parser.initialize_connection()
    data_parser.start_streaming()

if __name__ == "__main__":
    main()