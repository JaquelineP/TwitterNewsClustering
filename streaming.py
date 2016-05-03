from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="VhoVNjR0zQii7xHNboVPKOWbF"
consumer_secret="z5omn6VTZrrk4vWfHZDyv17ZUHE9pnVHAqgNk7RGhoR4aEIEZy"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="722334780549906432-hNorpjq46S1NmnhbNdX5NHPZyiJvLG4"
access_token_secret="U6Xhq03831bvSIHGRD2HGXulpcjLBnZ7lWVyRamtkYxuo"

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.

    """
    callback = None

    def __init__(self, callback):
        self.callback = callback


    def on_data(self, data):
        self.callback(data)
        return True

    def on_error(self, status):
        print(status)


def start_streaming(callback):
    l = StdOutListener(callback)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.sample()
    #stream.filter(languages=["en"], track=["a", "the", "i", "you", "u"]) 
