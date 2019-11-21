import json
from kafka import SimpleProducer, KafkaClient
import tweepy
import configparser


class TweeterStreamListener(tweepy.StreamListener):
    """ Una clase que lee la entrada de Tweets y los analiza con kafka"""

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client, async = True,
                          batch_send_every_n = 11,
                          batch_send_every_t = 10)

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        msg = status.text.encode('utf-8')
        try:
            self.producer.send_messages("twitterstream", msg)
            print("SUCCESS: " + status.text)
        except Exception as exception:
            print(exception)
            return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer: " + str(status_code))
        return True

    def on_timeout(self):
        return True 




if __name__ == '__main__':
    
    consumer_key = 'kyPpKR9o9nFwn2FH1hjjeYFaK'
    consumer_secret = 'cJ6ftGEnzLB8y5NafZkr4GkpqVx4imiVdw9N36UeOnelEeGqCt'
    access_key = '1179774771585835009-JifLRRiF5D8vsgeG7UlnZRcARk331u'
    access_secret = 'XlT9EWzRGkmZ6I5aV8NsiIsQFEvzv3N2mMzoq5VBEtGgz'
    
    # Crea un objeto Auth
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    print("API", api)

    # Se pueden agregar mas "etiquetas para filtrar mas Tweets"
    try:
        stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))
        stream.filter(track=['soccer', 'soccer good', 'soccer bad', 'soccer game', 'soccer match',
                             'uefa', 'conmebol', 'copa libertadores', 'fifa', 'world cup', 'soccer win',
                             'soccer lose', 'soccer team' 'goal'], languages=['en'])
    except tweepy.TweepError as e:
        print(e)