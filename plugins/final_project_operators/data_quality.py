from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id:str = None,
                 tables:list = None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        
        conn = PostgresHook(postgres_conn_id=self.conn_id)
        
        if self.tables:
            for table in self.tables:
                records = conn.get_records(f"SELECT COUNT(*) FROM {table}")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data Quality Check failed for {table} and returned no rows")
                else:
                    self.log.info(f"Data quality checks passed for {table}")
        else:
            raise ValueError("No tables included for data quality checks")