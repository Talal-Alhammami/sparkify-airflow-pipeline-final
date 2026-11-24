from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from helpers import SqlQueries

S3_BUCKET = Variable.get('s3_bucket')
SQL = SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='start_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket=S3_BUCKET,
        s3_key='log-data',
        json_path=f's3://{S3_BUCKET}/log_json_path.json',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket=S3_BUCKET,
        s3_key='song-data',
        json_path='auto ignorecase',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        sql=SQL.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        sql=SQL.user_table_insert,
        mode='truncate-insert',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        sql=SQL.song_table_insert,
        mode='truncate-insert',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        sql=SQL.artist_table_insert,
        mode='truncate-insert',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        sql=SQL.time_table_insert,
        mode='truncate-insert',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tests=[
            # No NULL keys
            {'sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL', 'expected': 0},
            {'sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL', 'expected': 0},
            {'sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL', 'expected': 0},
            {'sql': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL', 'expected': 0},
            {'sql': 'SELECT COUNT(*) FROM "time" WHERE start_time IS NULL', 'expected': 0},
            # Tables are not empty (1 = has rows)
            {'sql': 'SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM songplays', 'expected': 1},
            {'sql': 'SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM songs', 'expected': 1},
            {'sql': 'SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM artists', 'expected': 1},
        ],
    )

    end_operator = DummyOperator(task_id='end_execution')

    dims = [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> dims
    dims >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()