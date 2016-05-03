import json
import sqlite3
import os

tweets_data_path = 'sample_stream.json'
database_name = "twitter.db"

def create_table(conn, cur, create_db):
    output_columns = ["id", "text", "user_id", "hashtags", "urls", "lang"]
    int_columns = (0, 2)

    # generate insert statement
    prepared_sql = ("INSERT INTO tweets VALUES (" + "?, " * (len(output_columns)))[:-2] + ")"

    # create table if necessary
    if create_db:
        sql = "CREATE TABLE tweets ("
        for i, column in enumerate(output_columns):
            sql += "%s %s, " % (column, "INT" if i in int_columns else "VARCHAR(255)")
        sql += "PRIMARY KEY(id))"
        cur.execute(sql)
        conn.commit()

    return prepared_sql

def insert_tweet_data(cur, prepared_sql, tweet):
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
        cur.execute(prepared_sql, (twitter_id, text, user_id, entities_result["hashtags"], entities_result["urls"], lang))
    except sqlite3.IntegrityError:
        pass

def main():
    os.remove(database_name)
    create_db = (not os.path.isfile(database_name))
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    cur.execute("PRAGMA synchronous = OFF")
    cur.execute("PRAGMA journal_mode = MEMORY")

    # create table
    prepared_sql = create_table(conn, cur, create_db);

    tweets_data = []
    tweets_file = open(tweets_data_path, "r")
    count = 0
    dirty = ["delete", "status_withheld"]
    for line in tweets_file:
        tweet = json.loads(line)

        found_dirty = False
        for d in dirty:
            if d in tweet:
                found_dirty = True
                break
        if found_dirty:
            continue

        insert_tweet_data(cur, prepared_sql, tweet)
        tweets_data.append(tweet)
        count += 1
        if count % 1000 == 0:
            print("Count %d" % count)
            conn.commit()
    
    conn.commit()
    print("Count %d" % count)

if __name__ == "__main__":
    main()