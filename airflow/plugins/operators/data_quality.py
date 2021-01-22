from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs SQL data quality check and compares against an expected result 

    args:
        redshift_conn_id: redshift connection id airflow variable name
        dq_checks: list of checks and expected result in format:
                    ['check_sql': '<SQL1>', expected_result: <EXPECTED_RESULT1>,
                     'check_sql': '<SQL2>', expected_result: <EXPECTED_RESULT2>]
    """    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        # Check for data quality checks
        if len(self.dq_checks) <= 0:
            self.log.info("No data quality checks provided")
            return
        
        # instantiate PostgresHook() object, passing redshift_conn_id
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Initiate data quality check variable
        error_count = 0
        failing_tests = []
        
        # Loop through list of checks
        for check in self.dq_checks:
            check_sql = check.get('check_sql')
            expected_result = check.get('expected_result')
            # Try running check
            try:
                self.log.info(f"Running query: {check_sql}")
                records = redshift_hook.get_records(check_sql)[0]
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")
            # Compare against expected value
            if expected_result != records[0]:
                error_count += 1
                failing_tests.append(check_sql)
        # Log and raise failed checks
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info("All data quality checks passed")