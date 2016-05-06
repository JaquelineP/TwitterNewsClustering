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

class DataParser():
    count = 0
    prepared_sql = ""
    q = None
    start_time = 0

    def initialize_connection(self):
        try:
            os.remove(database_name)
        except FileNotFoundError:
            pass
        create_db = (not os.path.isfile(database_name))
        conn = sqlite3.connect(database_name)
        cur = conn.cursor()
        cur.execute("PRAGMA synchronous = OFF")
        cur.execute("PRAGMA journal_mode = MEMORY")
        self.initialize_table(conn, cur, create_db)
        self.q = Queue()

    def initialize_table(self, conn, cur, create_db):
        output_columns = ["id", "text", "user_id", "hashtags", "urls", "lang"]
        int_columns = (0, 2)

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
        text = tweet.get("text")
        twitter_id = tweet["id"]
        user_id = tweet.get("user", {}).get("id")
        # geo = tweet.get("geo")
        # hash tags are dicts
        entities = ["hashtags", "urls"]
        entity_keys = ["text", "expanded_url"]
        entities_result = {}
        for index, entity in enumerate(entities):
            entities_result[entity] = None
            for e in tweet.get("entities", {}).get(entity, {}):
                if entities_result[entity] is None:
                    entities_result[entity] = ""
                entities_result[entity] += e.get(entity_keys[index], "") + ","
            if entities_result[entity]:
                entities_result[entity] = entities_result[entity][:-1]

        lang = tweet.get("lang")

        # insert into db
        try:
            cur.execute(self.prepared_sql, (twitter_id, text, user_id, entities_result["hashtags"], entities_result["urls"], lang))
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
        if self.count % 100 == 0:
            print("inserted %i tweets (%.2f/s)" % (self.count, self.count / (time.time() - self.start_time)))
            conn.commit()

    def start_streaming(self):
        t = Thread(target=self.read_data_from_queue)
        t.daemon = True
        t.start()
        self.start_time = time.time()
        streaming.start_streaming(self.data_callback)

def main():
    data_parser = DataParser()
    data_parser.initialize_connection()
    data_parser.start_streaming()

if __name__ == "__main__":
    main()