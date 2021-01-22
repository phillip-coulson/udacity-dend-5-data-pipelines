from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load dimension table from staging table. 

    args:
        redshift_conn_id: redshift connection id airflow variable name
        table: destination staging table
        sql: INSERT statements for load
        append_only: Boolen to append from delete and insert
    """    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",  
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        # instantiate PostgresHook() object, passing redshift_conn_id
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # If append_only == False then delete existing data from dimension table
        if not self.append_only:
            self.log.info("Delete {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))    
        
        # Run SQL to INSERT data from staging to Dinemsion table
        self.log.info("Insert data from staging table into {}".format(self.table))
        
        insert_statement = f"INSERT INTO {self.table} \n{self.sql}"
        self.log.info(f"Running sql: \n{insert_statement}")
        redshift.run(insert_statement)
        self.log.info(f"Successfully completed insert into {self.table}")