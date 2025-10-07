from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'Don',
    'start_date': datetime(2024, 2, 12)
}

def titanic_processing():

    @dag(
        default_args=default_args,
        schedule_interval="@once",
        description="Simple Pipeline with Titanic",
        catchup=False,
        tags=['Titanic']
    )
    def _dag():

        start = EmptyOperator(task_id='start')

        @task
        def first_task():
            print("And so, it begins!")

        @task
        def read_data():
            df = pd.read_csv("https://raw.githubusercontent.com/donhighmsft/airflowexamples/main/dag_data/titantic.csv", sep=";")
            survivors = df.loc[df.Survived == 1, "Survived"].sum()
            survivors_sex = df.loc[df.Survived == 1, ["Survived", "Sex"]].groupby("Sex").count()
            return {
                'survivors_count': survivors,
                'survivors_sex': survivors_sex.to_dict()
            }

        @task
        def print_survivors(source):
            print(source['survivors_count'])

        @task
        def survivors_sex(source):
            print(source['survivors_sex'])

        last = BashOperator(
            task_id="last_task",
            bash_command='echo "This is the last task performed with Bash."',
        )

        end = EmptyOperator(task_id='end')

        first = first_task()
        downloaded = read_data()
        start >> first >> downloaded
        surv_count = print_survivors(downloaded)
        surv_sex = survivors_sex(downloaded)

        [surv_count, surv_sex] >> last >> end

    return _dag()

execution = titanic_processing()
