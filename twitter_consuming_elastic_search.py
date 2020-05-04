# from twitter_streaming import connect_consumer
from kafka import KafkaConsumer, KafkaProducer
import time
import elasticsearch
from elasticsearch import Elasticsearch

def connect_consumer(auto_offset_reset='earliest', consumer_timeout_ms=1000):
  _consumer = None
  try:
      _consumer = KafkaConsumer(bootstrap_servers='127.0.0.1:9092',
                               consumer_timeout_ms=1000,
                                enable_auto_commit=True)
  except Exception as e:
    print("Exception while connecting consumer.")
    print(str(e))

  return _consumer


# elastic search configuration
host = "tweets-elastic-searc-6263708600.ap-southeast-2.bonsaisearch.net"
user= "j953yvs7z2"
passwd = "ts42zy8zng"

try:
    elasticsearch_obj = Elasticsearch(hosts=[host], http_auth=(user,passwd), scheme='https', port=443)
    print("Connected to Elasticsearch cluster")
except Exception as e:
    print("Exception while connecting to Elastic search cluster")
    print(str(e))

consumer = connect_consumer('tweet_topics')
consumer.subscribe(['tweet_topics'])
# consumer.subscribe(['tweet_topics'])
while True:
    for message in consumer:
        print(message)
        try:
            # message = bytes(str(message), encoding='utf-8')
            res = elasticsearch_obj.index(index='twitter',body=message.value)
            print(res['_id'])

        except Exception as e:
            print("Exception while inserting into index")
            print(str(e))

















# ConsumerRecord(topic='tweet_topics', partition=4, offset=2334, timestamp=1588571639382, timestamp_type=0, key=None,
# value=b'{"created_at":"Mon May 04 05:53:45 +0000 2020","id":1257186635260874754,"id_str":"1257186635260874754","text":"NCDC COVID19 numbers no longer adds up\\n#COVID\\u30fc19","source":"\\u003ca href=\\"https:\\/\\/mobile.twitter.com\\" rel=\\"nofollow\\"\\u003eTwitter Web App\\u003c\\/a\\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":1226179937452818432,"id_str":"1226179937452818432","name":"Chibueze","screen_name":"ibueze1","location":"Nigeria","url":null,"description":"To be at peace with God, Love my family & contribute\\u00a0\\nmy quota to humanity.\\u00a0Every other thing is negotiable.","translator_type":"none","protected":false,"verified":false,"followers_count":5,"friends_count":13,"listed_count":0,"favourites_count":66,"statuses_count":14,"created_at":"Sat Feb 08 16:24:30 +0000 2020","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":null,"contributors_enabled":false,"is_translator":false,"profile_background_color":"F5F8FA","profile_background_image_url":"","profile_background_image_url_https":"","profile_background_tile":false,"profile_link_color":"1DA1F2","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\\/\\/pbs.twimg.com\\/profile_images\\/1226556898356363264\\/Pw9eYvoq_normal.jpg","profile_image_url_https":"https:\\/\\/pbs.twimg.com\\/profile_images\\/1226556898356363264\\/Pw9eYvoq_normal.jpg","profile_banner_url":"https:\\/\\/pbs.twimg.com\\/profile_banners\\/1226179937452818432\\/1584815312","default_profile":true,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[{"text":"COVID\\u30fc19","indices":[39,48]}],"urls":[],"user_mentions":[],"symbols":[]},"favorited":false,"retweeted":false,"filter_level":"low","lang":"en","timestamp_ms":"1588571625901"}\r\n',
#
# headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=2156, serialized_header_size=-1)
