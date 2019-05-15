from airflow import DAG
import sys
import unittest
import os
from datetime import datetime, date, time, timedelta

from airflow.operators.python_operator import PythonOperator

############## DAG-Specific Imports ##############

from configparser import ConfigParser
import snowflake.connector


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
        dag_id='Load_Snowflake_Audience_One',
        default_args=default_args,
        schedule_interval=None
    )


############## Utils ##############

CONFIG = ConfigParser()

if not os.environ.get('RUN_LOCAL') :
    CONFIG.read('/usr/local/airflow/utilities/config.cfg')
    #CONFIG.read('config.cfg')
else:
    # local config
    CONFIG.read('/usr/local/airflow/utilities/config.cfg')

SNOWFLAKE_USER = CONFIG.get('SnowflakeCreds', 'User')
SNOWFLAKE_PASSWORD = CONFIG.get('SnowflakeCreds', 'Password')
SNOWFLAKE_ACCOUNT = CONFIG.get('SnowflakeCreds', 'Account')
#SNOWFLAKE_DB = CONFIG.get('SnowflakeCreds', 'Database')
#SNOWFLAKE_WAREHOUSE = CONFIG.get('SnowflakeCreds', 'Warehouse')




############## Code References ##############


def reprocess_minute_summary(**context):

    for file in context['dag_run'].conf['files']:
        s3Bucket = file['s3_bucket']
        s3Key = file['s3_key']

    file_date = datetime.strptime(s3Key.split('.access.log.')[1].split('_')[0],'%Y%m%d').date()
    from_date_string = (file_date - timedelta(days=5)).strftime('%Y-%m-%d')
    to_date_string = (file_date + timedelta(days=1)).strftime('%Y-%m-%d')

    print(from_date_string)
    print(to_date_string)

    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        autocommit=False
    )
    cs = ctx.cursor()

    ctx.cursor().execute("USE warehouse TABLEAU_SERVER_GDP")
    ctx.cursor().execute("USE SYS_AUDIENCE_ONE")

    try:
        cs.execute(open("dag-sql/Snowflake_Audience_One/1_Audience_One_Delete_Minute_Summary.sql","r").read()
                   ,{
                        'from_date_string': from_date_string,
                        'to_date_string': to_date_string
        })

        cs.execute(open("dag-sql/Snowflake_Audience_One/2_Audience_One_Insert_Minute_Summary.sql", "r").read()
                   , {
                        'from_date_string': from_date_string,
                        'to_date_string': to_date_string
                   })

        ctx.commit()
    except Exception as e:
        ctx.rollback()
        print (e)
        raise e
    finally:
        cs.close()
        ctx.close()


############## DAG Operators ##############


if not os.environ.get('TEST_FLAG'):

    t1 = PythonOperator(
        task_id='Load_Snowflake_Icecast',
        python_callable=reprocess_minute_summary,
        provide_context=True,
        dag=dag
    )
if __name__ == "__main__":
    class Object(object):
        pass


    mock_config = Object()
    mock_config.conf = {
        'files': [
            {
                's3_key': 'icecast_server_logs/raw/2019/04/22/thw-ice-0.access.log.20190422_202826.gz',
                's3_bucket': 'data-icecast'
            }
        ]
    }
    reprocess_minute_summary(dag_run=mock_config)


############## Test Runner ##############

class TestRun(unittest.TestCase):

    def test_load_snowflake(self):
        class Object(object):
            pass

        mock_config = Object()
        mock_config.conf = {
            'files' : [
                {
                    's3_key' : 'icecast_server_logs/raw/2019/04/22/thw-ice-0.access.log.20190422_202826.gz',
                    's3_bucket' : 'data-icecast'
                }
            ]
        }

        self.assertEqual(
            reprocess_minute_summary(dag_run=mock_config)
            , None)

