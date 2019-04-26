from airflow.operators.slack_operator import SlackAPIPostOperator
import os
import yaml

# Get the Slack Oauth Token that was set as an environment variable in Entrypoint.sh

def get_slack_token():

    with open(os.environ.get("CONFIG_LOCATION"), 'r') as ymlfile:

        return yaml.load(ymlfile, Loader=yaml.FullLoader)["slack"]["WEBHOOK_TOKEN"]


SLACK_TOKEN = get_slack_token()


def task_success_slack_alert(context):

    slack_msg = """
            :large_blue_circle: Task Succeeded! 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    success_alert = SlackAPIPostOperator(
        task_id="successnotification",
        channel="#bidata",
        username="Airflow",
        text=slack_msg,
        token=SLACK_TOKEN)

    return success_alert.execute()


def task_fail_slack_alert(context):

    slack_msg = """
            :red_circle: Task Failure 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    notification = SlackAPIPostOperator(
        task_id="successnotification",
        channel="#bidata",
        username="Airflow",
        text=slack_msg,
        token=SLACK_TOKEN)

    return notification.execute()
