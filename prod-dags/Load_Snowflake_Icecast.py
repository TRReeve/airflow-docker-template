from airflow import DAG
import sys
import unittest
import os
from datetime import datetime, date, time, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# DAG-Specific Imports

from configparser import ConfigParser
import snowflake.connector


# Basic DAG Info

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
        dag_id='Load_Snowflake_Icecast',
        default_args=default_args,
        schedule_interval=None
    )


# Utils

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
SNOWFLAKE_DB = CONFIG.get('SnowflakeCreds', 'Database')
SNOWFLAKE_WAREHOUSE = CONFIG.get('SnowflakeCreds', 'Warehouse')




# Code References

# ---- Op1.A ---- #
# Load S3 data to Snowflake

def s3_to_snowflake(**context):

    for file in context['dag_run'].conf['files']:
        s3Bucket = file['s3_bucket']
        s3Key = file['s3_key']

    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        autocommit=False
    )
    cs = ctx.cursor()

    #ctx.cursor().execute("USE warehouse %(warehouse_name)s",{'warehouse_name':SNOWFLAKE_WAREHOUSE })
    #ctx.cursor().execute("USE %(db_name)s",{'warehouse_name':SNOWFLAKE_DB })
    cs.cursor().execute("USE warehouse TABLEAU_SERVER_GDP")
    cs.cursor().execute("USE DS_ICECAST")

    try:
        cs.execute(open("dag-sql/Snowflake_Icecast/1_Icecast_Delete_Raw.sql","r").read()
                   ,{
                        'file_name': s3Key
                   })
        #rows = cs.fetchall()
        #print('hello')
        cs.execute(open("dag-sql/Snowflake_Icecast/2_Icecast_Copy_Into.sql", "r").read()
                   , {
                       'file_name': s3Key
                   })

        ctx.commit()
    except Exception as e:
        ctx.rollback()
        print (e)
        raise e
    finally:
        cs.close()
        ctx.close()


def materialise_icecast_columns(**context):

    for file in context['dag_run'].conf['files']:
        s3Bucket = file['s3_bucket']
        s3Key = file['s3_key']

    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        autocommit=False
    )
    cs = ctx.cursor()

    #ctx.cursor().execute("USE warehouse %(warehouse_name)s",{'warehouse_name':SNOWFLAKE_WAREHOUSE })
    #ctx.cursor().execute("USE %(db_name)s",{'warehouse_name':SNOWFLAKE_DB })
    ctx.cursor().execute("USE warehouse TABLEAU_SERVER_GDP")
    ctx.cursor().execute("USE DS_ICECAST")

    try:
        cs.execute(open("dag-sql/Snowflake_Icecast/3_Icecast_Delete_Curated.sql","r").read()
                   ,{
                        'file_name': s3Key
                   })
        #rows = cs.fetchall()
        #print('hello')
        cs.execute(open("dag-sql/Snowflake_Icecast/4_Icecast_Insert_Curated.sql", "r").read()
                   , {
                       'file_name': s3Key
                   })

        ctx.commit()
    except Exception as e:
        ctx.rollback()
        print (e)
        raise e
    finally:
        cs.close()
        ctx.close()

def split_icecast_streams_to_minute(**context):

    for file in context['dag_run'].conf['files']:
        s3Bucket = file['s3_bucket']
        s3Key = file['s3_key']

    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        autocommit=False
    )
    cs = ctx.cursor()

    #ctx.cursor().execute("USE warehouse %(warehouse_name)s",{'warehouse_name':SNOWFLAKE_WAREHOUSE })
    #ctx.cursor().execute("USE %(db_name)s",{'warehouse_name':SNOWFLAKE_DB })
    ctx.cursor().execute("USE warehouse TABLEAU_SERVER_GDP")
    ctx.cursor().execute("USE DS_ICECAST")

    try:
        cs.execute(open("dag-sql/Snowflake_Icecast/5_Icecast_Delete_Date_Time.sql","r").read()
                   ,{
                        'file_name': s3Key
                   })
        #rows = cs.fetchall()
        #print('hello')
        cs.execute(open("dag-sql/Snowflake_Icecast/6_Icecast_Insert_Date_Time.sql", "r").read()
                   , {
                       'file_name': s3Key
                   })

        ctx.commit()
    except Exception as e:
        ctx.rollback()
        print (e)
        raise e
    finally:
        cs.close()
        ctx.close()

def set_minute_Summary_trigger(context, dag_run_obj):
    #pass on context files to next process
    dag_run_obj.payload = {
            'files': context['dag_run'].conf['files']
    }
    return dag_run_obj

def trigger_minute_summary(**context):
    TriggerDagRunOperator(
        task_id='Load_Snowflake_Audience_One',
        trigger_dag_id='Load_Snowflake_Audience_One',
        python_callable=set_minute_Summary_trigger,
        params={
            'queue_event': 'Load_Snowflake_Audience_One',
            'init_message': 'start minute summary load'
        },
        dag=dag
    )

############## DAG Operators ##############


if not os.environ.get('TEST_FLAG'):

    t1 = PythonOperator(
        task_id='Load_Snowflake_Icecast',
        python_callable=s3_to_snowflake,
        provide_context=True,
        dag=dag
    )

    t2 = PythonOperator(
        task_id='materialise_icecast_columns',
        python_callable=materialise_icecast_columns,
        provide_context=True,
        dag=dag
    )

    t3 = PythonOperator(
        task_id='split_icecast_streams_to_minute',
        python_callable=split_icecast_streams_to_minute,
        provide_context=True,
        dag=dag
    )

    t4 = PythonOperator(
        task_id='trigger_minute_summary',
        python_callable=trigger_minute_summary,
        provide_context=True,
        dag=dag
    )

    # set order of tasks inside DAG#
    t1 >> t2 >> t3 >> t4

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
    # s3_to_snowflake(dag_run=mock_config)
    # materialise_icecast_columns(dag_run=mock_config)
    # split_icecast_streams_to_minute(dag_run=mock_config)



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
            s3_to_snowflake(dag_run=mock_config)
            , None)

