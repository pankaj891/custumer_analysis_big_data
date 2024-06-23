import tweepy

consumer_key = 'QJ48ACpFpn3npZ8dKVQyJnPFw'
consumer_secret = 'gbMP9kaw6ItCGa63ACPD5sor9eCtXvHXjfKtpaCXxqPVnVWz0M'
access_token = '1705497537900404738-NHHpYHSHNbQSbMzwQSyS7IBJa9YH8L'
access_token_secret = 'd4WFidoxEwCh7P6sSuZsvMNzClofPiDEBJnmF1BVXMBHb'

auth = tweepy.OAuth1UserHandler(consumer_key,consumer_secret,access_token,access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

search_query = "'Elon Musk''fired'-filter:retweets AND -filter:replies AND -filter:links"
no_of_tweets = 5

try:
    tweets = api.search_tweets(q=search_query,lang="en",count=no_of_tweets,tweet_mode ='extended')
    print("tweets : ",tweets)
except BaseException as ex:
    print('status failed : ',str(ex))