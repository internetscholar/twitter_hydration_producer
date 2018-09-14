import psycopg2
from psycopg2 import extras
import boto3
import json
import configparser
import os
import logging


def main():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    logging.info('DB parameters were read from config.ini')

    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info('Connected to database %s', config['database']['db_name'])

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=config['aws']['region_queues']
    )
    logging.info('Retrieved AWS credentials')

    sqs = aws_session.resource('sqs')

    # Connect to queue for Twitter credentials.
    twitter_credentials_queue = sqs.get_queue_by_name(QueueName='twitter_credentials')
    twitter_credentials_queue.purge()
    logging.info('Purged twitter_credentials queue')

    # Enqueue all Twitter credentials available on the database.
    cur.execute("""select * from twitter_credentials;""")
    credentials = cur.fetchall()
    for credential in credentials:
        message_body = json.dumps(credential)
        twitter_credentials_queue.send_message(MessageBody=message_body)
    logging.info('Enqueued all Twitter credentials')

    tweet_ids_queue = sqs.get_queue_by_name(QueueName='twitter_hydration')
    tweet_ids_queue.purge()
    logging.info('Purged twitter_hydration queue')

    cur.execute("SELECT * FROM project WHERE active")
    active_projects = cur.fetchall()
    logging.info('Queried all active projects')

    for active_project in active_projects:
        cur.execute("""
                    select distinct tweet_id
                    from
                         twitter_dry_tweet,
                         twitter_scraping_query
                    where
                        twitter_dry_tweet.query_alias = twitter_scraping_query.query_alias and
                        twitter_scraping_query.project_name = '{0}' and
                          not exists(
                                select *
                                from twitter_hydrated_tweet
                                where twitter_hydrated_tweet.project_name = '{0}' and
                                      twitter_hydrated_tweet.tweet_id = twitter_dry_tweet.tweet_id);
                                      """.format(active_project['project_name']))
        logging.info('Queried dry tweets for project %s', active_project['project_name'])

        no_more_results = False
        message_count = 1
        while not no_more_results:
            tweet_ids = cur.fetchmany(size=900)
            if len(tweet_ids) == 0:
                no_more_results = True
                logging.info('No more tweets')
            else:
                message_body = {
                    'project_name': active_project['project_name'],
                    'tweet_ids': [tweet_id['tweet_id'] for tweet_id in tweet_ids]
                }
                tweet_ids_queue.send_message(MessageBody=json.dumps(message_body))
                logging.info('Message %d sent with %d tweets', message_count, len(tweet_ids))
                message_count = message_count + 1

    conn.close()
    logging.info('End database connection')


if __name__ == '__main__':
    main()
