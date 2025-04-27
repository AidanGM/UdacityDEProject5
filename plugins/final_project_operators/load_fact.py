from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id:str = "",
                 table:str = "",
                 sql:str = "",
                 truncate_table:bool = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.truncate_table = truncate_table

    def execute(self, context):
        conn = PostgresHook(postgres_conn_id=self.conn_id)
        if self.truncate_table:
            conn.run(f'TRUNCATE {self.table}')
            self.log.info(f'{self.table} truncated')
        conn.run(f'INSERT INTO {self.table} {self.sql}')
        self.log.info(f'{self.table} loaded')
