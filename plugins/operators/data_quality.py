from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tests=None,
        *args,
        **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        redshift = PostgresHook(
            postgres_conn_id=self.redshift_conn_id
        )

        for test in self.tests:
            records = redshift.get_records(test["sql"])

            if not records or not records[0]:
                raise ValueError(
                    f"Data quality check failed. No results for {test['sql']}"
                )

            actual = records[0][0]
            expected = test["expected"]

            if actual != expected:
                raise ValueError(
                    f"Data quality check failed: "
                    f"expected {expected}, got {actual}. "
                    f"SQL: {test['sql']}"
                )

            self.log.info(
                f"Data quality check passed: "
                f"SQL: {test['sql']} | "
                f"Expected: {expected} | "
                f"Actual: {actual}"
                )