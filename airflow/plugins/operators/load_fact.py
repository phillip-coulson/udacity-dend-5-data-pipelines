from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Load Fact table from staging table. 

    args:
        redshift_conn_id: redshift connection id airflow variable name
        table: destination staging table
        sql: INSERT statements for load
    """    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",        
                 *args, **kwargs):
                    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
         # instantiate PostgresHook() object, passing redshift_conn_id
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Run SQL to INSERT data from staging to Fact table. Note: Will always append data to Fact tables
        self.log.info("Insert data from staging tables into {}".format(self.table))
        
        insert_statement = f"INSERT INTO {self.table} \n{self.sql}"
        self.log.info(f"Running sql: \n{insert_statement}")
        redshift.run(insert_statement)
        self.log.info(f"Successfully completed insert into {self.table}")
