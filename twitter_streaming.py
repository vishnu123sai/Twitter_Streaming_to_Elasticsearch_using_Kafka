from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer
import configparser
config = configparser.ConfigParser()
config.read('configurations.ini')


def connect_kafka_producer():
  _producer = None
  try:
    _producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', api_version=(0, 10))
  except Exception as e:
    print('Exception while connecting kafka.')
    print(str(e))
  return _producer

producer = connect_kafka_producer()

def connect_consumer(topic_name, auto_offset_reset='earliest', consumer_timeout_ms=1000):
  _consumer = None
  try:
    _consumer = KafkaConsumer(topic_name, auto_offset_reset=auto_offset_reset,
                              bootstrap_servers=['localhost:9092'], api_version=(0, 10),
                              consumer_timeout_ms=consumer_timeout_ms, enable_auto_commit=True)
  except Exception as e:
    print("Exception while connecting consumer.")
    print(str(e))

  return _consumer

def publish_message(producer, topic_name, value, key=""):
  try:
    value = bytes(str(value), encoding='utf-8')
    if key == "":
      producer.send(topic_name, value=value)
    else:
      key = bytes(key, encoding='utf-8')
      producer.send(topic_name, key=key, value=value)
    producer.flush()
    print("Message published successfully.")
  except Exception as e:
    print("Exception while publishing the message.")
    print(str(e))

class StdOutListener(StreamListener):

  def on_data(self, data):
    print(str(data))
    publish_message(producer, 'tweet_topics', str(data))


    return True

  def on_error(self, status):
    print(status)

if __name__ == '__main__':
  l = StdOutListener()
  auth = OAuthHandler(config['TOKENS']['api_key'], config['TOKENS']['api_secret_key'])
  auth.set_access_token(config['TOKENS']['access_token'], config['TOKENS']['access_token_secret'])
  stream = Stream(auth, l)
  stream.filter(track=['covid'],languages=["en"])


