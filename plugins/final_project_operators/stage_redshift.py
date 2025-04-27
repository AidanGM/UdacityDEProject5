from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id:str = "",
                 aws_credentials_id:str = "",
                 table:str = "",
                 s3_bucket:str = "",
                 s3_key:str = "",
                 region:str = "",
                 json:str = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region # not used here but included for completeness
        self.json = json

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        conn = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f'Connection established')
        
        sql = f"""
        COPY {self.table}
        FROM 's3://{self.s3_bucket}/{self.s3_key}/'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        JSON '{self.json}'
        """

        conn.run(sql)
        self.log.info("Json data staged")





