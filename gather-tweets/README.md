# **Data collection**

In order to be able to repeat the clustering on the same dataset, it can make sense to first gather a set of tweets for processing.

1. Get Twitter API key

    1. Create Twitter Account

    2. Create Twitter Application ([https://apps.twitter.com/](https://apps.twitter.com/))

    3. Create consumer key and secret

    4. Save keys in `config.txt` separated by line break in the following order:

        1. consumer_key

        2. consumer_secret

        3. access_token

        4. access_token_secret

2. Run script: `python parse_data.py`

    two new files are created containing the gathered tweets

    5. twitter.dat: tweets in JSON-format

    6. twitter.db: same tweets in sqlite database