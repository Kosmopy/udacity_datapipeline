from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    facts_sql_template = """
        SELECT
            DISTINCT sessionid from staging_events,
            songplay_id from public.songs,
            start_time from staging_events, 
            userid from staging_events,
            level from staging_events,
            song_id public.songs,
            artist_id from public.songs,
            location from staging_events,
            useragent from staging_events"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        self.log.info("Loading fact table")
        facts_sql=LoadFactOperator.facts_sql_template
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(facts_sql)
