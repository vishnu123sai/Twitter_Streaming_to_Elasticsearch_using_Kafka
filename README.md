# Twitter_Streaming_to_Elasticsearch_using_Kafka


## Technologies Used
1. Apache Kafka
2. TwitterAPI
3. ElasticSerach

## Description

    Real time twitter covid tweets Streaming into elastic search using Apache kafka 
    
    producer : Twitter
    Consumer : Elasctic Search
    Apache Kafka stands between these two and effectively sends the data to consummers from producer.
    
    
    -> Used maintained and provisioned elastic search cluster with 3 nodes from Bonsai
    
     Use this link to provision your elastic search cluster "https://app.bonsai.io/clusters"
     
     
## Project Setup

  1. Install all dependency packages
  2. Create twitter developer account and generate access_toke, access_token_secret, api_key, api_key_secret
  3. Create the Elastic search cluster from Bonsai
  4. Install Apache Kafka (kafka needs to have zookeeper too)
      download kafka from "https://kafka.apache.org/downloads.html"
  5. Run twitter_streaming.py to stream tweets from twitter and publish to kafka topic
  6. Run twitter_consuming_elastic_search.py to consume and ingect the tweets to Elastic search 
     Go through elastic search python package doumentation to learn more 
     
     
     
     
 ## Note : 
 
    Make sure to replace the credentials in configurations.ini, :) my credentials won't work for you, I already regenerated my tokens LOL :)
