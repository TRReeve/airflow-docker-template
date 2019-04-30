**README**

In airflow Workers, Schedulers and Webservers seem to require the same scripts and dependencies present on all of them for the async pattern to work. 
This docker compose files lets you bring up an airflow instance using Celery workers by configuring the volumes in the 
Airflow-Celery

**CONFIGURING**

Add your necessary volumes to the service definitions in Airflow-Celery.yml

*In particular*

Minimum configuration of the volume section should look somewhat like this at a minimum replacing the $PARAMETERS with the relevant filepaths

        volumes:
            - $YOUR_AIRFLOW_CONFIGURATION_FILE:/usr/local/airflow/airflow.cfg
            - $YOUR_DAG_DIRECTORY:/usr/local/airflow/dags
            - $YOUR_SCRIPTS_DIRECTORY:/usr/local/airflow/scripts
            - $YOUR_SECRETS_FILE:/usr/local/airflow/utilities/config.yml
            - $YOUR_REQUIREMENTS.TXT_FILE:/usr/local/airflow/requirements.txt

Notes:
Your config file must be in a yml format.
requirements.txt should contain all packages of all programmes you need to run on your airflow. When the container
boots it will run 

    $(which pip) install --user -r ~/requirements.txt
    
this will install all the packages in the requirements file so they're available to the workers. Provisioning non-python depenedencies should be done within the entrypoint.sh file (purely a personal opinion). In similar circumstances I'd run scala projects by pushing them to a seperate docker and then initialising a kubernetesDAG for now but perhaps some sort of .jar downloading could be one approach. 

**entrypoint.sh**

Runs all the bash commands you want to run at start-up of the container. If you have anything else that needs to start
then this is the place to run it.  

**RUNNING**

     docker-compose -f Airflow-Celery.yml up -d
 
add 

    --build
    
to your argument if you want to rebuild all containers from scratch. 

    docker-compose -f docker-compose-CeleryExecutor.yml scale worker=5
    
will run the same set up but with 5 worker units if you need to start scaling things up. 

Notes:
- airflow.cfgs "executor" parameter should be set to "CeleryExecutor"
- Be careful with what port your airflow webserver is running on versus where it is mapped to 
- Running it with non-python scripts might take some fiddling with DockerFile & entrypoint.sh to install dependencies etc. But if 
it can be triggered with bash then this can likely be done. 
- scripts involving file manipulation can present rights issues with mounted volums, use unix permissions to overcome these. 
