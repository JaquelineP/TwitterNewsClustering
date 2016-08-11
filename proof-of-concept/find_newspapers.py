import json
import sqlite3
import os
import streaming
from queue import Queue
from threading import Thread
import time
import operator
import tweepy

dirty = ["delete", "status_withheld", "limit"]
OUTPUT_THRESHOLD = 5000

class DataParser():
    count = 0
    prepared_sql = ""
    q = None
    start_time = 0
    users = {}

    def read_data_from_queue(self):
        while True:
            item = self.q.get()
            self.q.task_done()
            self.process_data(item)

    def data_callback(self, data):
        self.q.put(data)

    def process_data(self, data):
        tweet = json.loads(data)

        found_dirty = False
        for d in dirty:
            if d in tweet:
                found_dirty = True
                break
        if found_dirty:
            return

        for user in tweet.get("entities", []).get("user_mentions",[]):
            user_name  = user.get("screen_name")
            if not user_name in self.users:
                self.users[user_name] = 1
            else:
                self.users[user_name] += 1
        self.count += 1

        if self.count % OUTPUT_THRESHOLD == 0:
            print("inserted %i tweets (%.2f/s)" % (self.count, self.count / (time.time() - self.start_time)))
            sorted_users = sorted(self.users.items(), key=operator.itemgetter(1))
            sorted_users.reverse()
            with open("users.txt", "w") as f:
                for user_name in sorted_users:
                    twitter_user = streaming.get_user(user_name)
                    f.write(twitter_user.followers_count + " |||| " + user_name)


    def start_streaming(self):
        t = Thread(target=self.read_data_from_queue)
        t.daemon = True
        t.start()
        self.start_time = time.time()
        streaming.start_streaming(self.data_callback, ["news"])

def main():
    data_parser = DataParser()
    data_parser.q = Queue()
    data_parser.start_streaming()

if __name__ == "__main__":
    main()