import sys
sys.path.append('/home/enricolm/Documents/Airflow_Extracao_Dados/airflow_pipeline')

from airflow.models import DAG,TaskInstance
from operators.Sprinklr_Operator import SprinklrOperator
from datetime import datetime
from os.path import join


with DAG(dag_id='Sprinklr_Api', start_date=datetime.now(), schedule_interval='@daily') as dag:
        SO = SprinklrOperator(path=join('/home/enricolm/Documents/Airflow_Extracao_Dados/airflow_pipeline/',
                                        'datalake/Sprinklr_Data',
                                        f"Dados_{datetime.now().date().strftime('%Y%m%d')}.csv"),task_id = 'Sprinklr_Api')        
        TI = TaskInstance(task=SO)
        SO.execute(TI.task_id)