from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql="",
                 mode="truncate-insert",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.mode = mode

    def execute(self, context):
        if self.mode not in ("truncate-insert", "append"):
            raise ValueError(f"Invalid mode '{self.mode}'")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == "truncate-insert":
            self.log.info(f"Truncating {self.table}")
            redshift.run(f'TRUNCATE TABLE "{self.table}"')
        self.log.info(f"Loading dimension table {self.table}")
        redshift.run(f'INSERT INTO "{self.table}" {self.sql}')
        self.log.info(f"Dimension table {self.table} loaded")
