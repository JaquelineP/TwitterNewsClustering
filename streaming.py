from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

class StdOutListener(StreamListener):
    callback = None
    consumer_key, consumer_secret, access_token, access_token_secret = "", "", "", ""

    def __init__(self, callback):
        self.read_keys_from_config()
        self.callback = callback

    def read_keys_from_config(self):
        with open("config.txt", "r") as f:
            self.consumer_key = f.readline().strip()
            self.consumer_secret = f.readline().strip()
            self.access_token = f.readline().strip()
            self.access_token_secret = f.readline().strip()

    def on_data(self, data):
        self.callback(data)
        return True

    def on_error(self, status):
        print("Twitter API error: %i" % status)

def start_streaming(callback, track_array):
    l = StdOutListener(callback)
    auth = OAuthHandler(l.consumer_key, l.consumer_secret)
    auth.set_access_token(l.access_token, l.access_token_secret)

    stream = Stream(auth, l)
    #stream.sample()
    stream.filter(languages=["en"], track=track_array, async=True)
