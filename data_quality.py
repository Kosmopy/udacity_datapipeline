from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 li: [str],
                 query,
                 failure_value,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.li = li
        self.query = query
        self.failure_value = failure_value

    def execute(self, context):
        self.log.info("conducting data quality checks")
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        for e in self.li:
            query = self.query.format(f)
            res = redshift.get_first(query)[0]
            if res == self.failure_value:
                raise ValueError(f"query {query} failed")