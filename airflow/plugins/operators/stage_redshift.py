from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Load JSON files from S3 to Redshift staging table. 

    args:
        redshift_conn_id: redshift connection id airflow variable name
        aws_conn_id: aws connection id variable airflow name
        table: destination staging table
        s3_bucket: S3 bucket address
        s3_key: S3 bucket object key,
        json_format: set to auto or json_file_path,
    """

    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        FORMAT AS JSON '{json_format}'
        TIMEFORMAT AS 'epochmillisecs'
        REGION 'us-west-2'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format


    def execute(self, context):
        # instantiate AwsHook and assign aws credentials
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()

        # instantiate PostgresHook() object, passing redshift_conn_id
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Delete data from staging table before load
        self.log.info('Deleting data from destination Redshift staging table')
        redshift.run("DELETE FROM {}".format(self.table))
        
        # Copy data from S3 using parameterised sql
        self.log.info('Copying data from S3 to Redshift staging table')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            json_format=self.json_format
        )
        
        redshift.run(formatted_sql)