from airflow import DAG
import sys
import unittest
import os

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


############## DAG-Specific Imports ##############

from configparser import ConfigParser
import boto3
import json
import copy
import pprint


############## Basic DAG Info ##############

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 10),
    'email': ['none'],
    'email_on_failure': False,
    'email_on_retry': False,
}

if not os.environ.get('TEST_FLAG'):

    dag = DAG(
        dag_id='Load_Snowflake_Trigger',
        default_args=default_args,
        schedule_interval='*/5 * * * *',
        catchup=False
    )


############## Utils ##############


CONFIG = ConfigParser()

if not os.environ.get('RUN_LOCAL') :
    CONFIG.read('/usr/local/airflow/utilities/config.cfg')
else:
    # local config
    CONFIG.read('/usr/local/airflow/utilities/config.cfg')

PRETTY_PRINTER = pprint.PrettyPrinter(indent=4)
QUEUE_URL = 'https://sqs.eu-west-1.amazonaws.com/143873499099/Airflow_S3_Ingest'
SQS_CLIENT = boto3.client('sqs',
                          region_name='eu-west-1',
                          aws_access_key_id=CONFIG.get('AWSCreds', 'access_key_id'),
                          aws_secret_access_key=CONFIG.get('AWSCreds', 'secret_access_key'))


############## Code References ##############

def conditionally_trigger(context, dag_run_obj):
    """
        Task within DAG just to denote which trigger has fired
    """

    msgs = []

    # Multiple requests to gather all messages from different queue servers
    for i in range(0,5):

        crnt_msgs = SQS_CLIENT.receive_message(
            QueueUrl=QUEUE_URL,
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=20,
            WaitTimeSeconds=20,
        )

        # check if messages in queue
        if 'Messages' not in crnt_msgs:
            return

        for msg in crnt_msgs["Messages"]:

            # check message isn't already in queue
            if not any(msg['MessageId'] == x['MessageId'] for x in msgs):

                msg['JSON_Body'] = json.loads(msg['Body'])

                # skip message if message is malformed
                if  "Records" not in msg['JSON_Body']["Message"]:
                    continue

                msg['JSON_Body']['JSON_Message'] = json.loads(msg['JSON_Body']['Message'])

                # if it's this trigger's event and there is a file within the message
                if (msg['JSON_Body']['JSON_Message']["Records"][0]["s3"]["configurationId"] == context['params']['queue_event']
                    and len(msg['JSON_Body']['JSON_Message']["Records"][0]["s3"]["object"]["key"]) > 0):

                    msg_copy = copy.deepcopy(msg)
                    msgs.append(msg_copy)

    # sort chronologically
    msgs.sort(key=lambda msg: msg['JSON_Body']['JSON_Message']['Records'][0]['eventTime'])


    files = []
    for msg in msgs:
        files.append({
            's3_bucket':msg['JSON_Body']['JSON_Message']["Records"][0]["s3"]['bucket']['name'],
            's3_key':msg['JSON_Body']['JSON_Message']["Records"][0]["s3"]["object"]["key"]
        })

        # # delete messages from queue when setup response object properly
        # response = SQS_CLIENT.delete_message(
        #     QueueUrl=QUEUE_URL,
        #     ReceiptHandle=msg['ReceiptHandle'],
        # )

    # for msg in msgs:
    dag_run_obj.payload = {
        'files' : files
    }

    return dag_run_obj


############## DAG Operators ##############

if not os.environ.get('TEST_FLAG'):

    # TODO poll queue for messages - check if message matches one of the triggers
    # TODO if message matches a trigger, fire the trigger


    dag_triggers = {
        'load_snowflake_icecast':{
            'trigger_dag_id':'Load_Snowflake_Icecast',
            'queue_event':'S3_Ingest_Icecast',
            'on_start':'Icecast Ingestion Started'
        }
    }


    for dag_trigger_name, dag_trigger_info in dag_triggers.items():
        # create DAG triggers for each of the trigger object
        TriggerDagRunOperator(
            task_id=dag_trigger_name,
            trigger_dag_id=dag_trigger_info['trigger_dag_id'],
            python_callable=conditionally_trigger,
            params={
                'queue_event': dag_trigger_info['queue_event'],
                'init_message': dag_trigger_info['on_start']
            },
            dag=dag
        )




############## Test Runner ##############

class TestRun(unittest.TestCase):

    def test_message_from_queue(self):
        class Object(object):
            pass
        mockDAG = Object()

        mockContext = {
            'params' : {
                'queue_event': 'S3_Ingest_Icecast',
                'init_message': 'This is a test'
            }
        }


        self.assertNotEqual(
            conditionally_trigger(mockContext, mockDAG)
            , None)

