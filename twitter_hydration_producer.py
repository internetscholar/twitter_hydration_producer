import psycopg2
from psycopg2 import extras
import boto3
import json
import configparser
import os

if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['region_name']
    )
    sqs = aws_session.resource('sqs')

    # Connect to queue for Twitter credentials.
    twitter_credentials_queue = sqs.get_queue_by_name(QueueName='twitter-credentials')
    twitter_credentials_queue.purge()
    # Enqueue all Twitter credentials available on the database.
    cur.execute("""select * from twitter_credentials;""")
    credentials = cur.fetchall()
    for credential in credentials:
        message_body = json.dumps(credential)
        twitter_credentials_queue.send_message(MessageBody=message_body)

    tweet_ids_queue = sqs.get_queue_by_name(QueueName='tweet-ids')
    tweet_ids_queue.purge()

    cur.execute("SELECT * FROM query WHERE status = 'PHASE3'")
    phase3_queries = cur.fetchall()
    for phase3_query in phase3_queries:
        cur.execute("""select distinct tweet_id
                       from tweet_id
                       where query_alias = '{0}'
                       and not exists(
                           select *
                           from tweet
                           where tweet.query_alias = '{0}' and
                                 tweet_id.tweet_id = tweet.tweet_id);""".format(phase3_query['query_alias']))
        no_more_results = False
        while not no_more_results:
            tweet_ids = cur.fetchmany(size=900)
            if len(tweet_ids) == 0:
                no_more_results = True
            else:
                message_body = {
                    'query_alias': phase3_query['query_alias'],
                    'tweet_ids': [tweet_id['tweet_id'] for tweet_id in tweet_ids]
                }
                tweet_ids_queue.send_message(MessageBody=json.dumps(message_body))

    conn.close()