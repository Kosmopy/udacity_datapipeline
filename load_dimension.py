from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_statement="",
                 table="",
                 app_data="", *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.app_data=app_data

    def execute(self, context):
        self.log.info("loading dimensional tables")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        if self.app_data == True:
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_statement)
            redshift.run(sql_statement)
        else:
            sql_statement = 'TRUNCATE TABLE %s;' % (self.table)
            sql_statement =sql_statement + 'INSERT INTO %s %s' % (self.table, self.sql_statement)
            redshift.run(sql_statement)
