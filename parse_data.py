import json
import sqlite3
import os
import streaming

tweets_data_path = 'sample_stream_sample.json'
database_name = "twitter.db"
dirty = ["delete", "status_withheld", "limit"]

class DataParser():
    count = 0
    cur = None
    conn = None
    prepared_sql = ""

    def initialize_connection(self):
        try:
            os.remove(database_name)
        except FileNotFoundError:
            pass
        create_db = (not os.path.isfile(database_name))
        self.conn = sqlite3.connect(database_name)
        self.cur = self.conn.cursor()
        self.cur.execute("PRAGMA synchronous = OFF")
        self.cur.execute("PRAGMA journal_mode = MEMORY")
        self.initialize_table(create_db)

    def initialize_table(self, create_db):
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
            self.cur.execute(sql)
            self.conn.commit()

    def insert_tweet_data(self, tweet):
        text = tweet.get("text")
        try:
            if "id" not in tweet:
                print(tweet)
        except:
            pass
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
            self.cur.execute(prepared_sql, (twitter_id, text, user_id, entities_result["hashtags"], entities_result["urls"], lang))
        except sqlite3.IntegrityError:
            pass

    def process_data(self, data):
        tweet = json.loads(data)

        found_dirty = False
        for d in dirty:
            if d in tweet:
                found_dirty = True
                break
        if found_dirty:
            return

        insert_tweet_data(tweet)
        self.count += 1
        if self.count % 1000 == 0:
            print("Count %d" % self.count)
            self.conn.commit()

    def start_streaming(self):
        streaming.start_streaming(self.process_data)

def main():
    data_parser = DataParser()
    data_parser.initialize_connection()
    data_parser.start_streaming()

if __name__ == "__main__":
    main()