import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from base_classes.EtlJob import EtlJob


etljob = EtlJob("/Users/tom.reeve/PycharmProjects/global_airflow/utilities/config.yml")

etljob.connect_snowflake()

