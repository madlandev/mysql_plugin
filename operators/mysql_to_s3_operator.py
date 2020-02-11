from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from mysql_plugin.hooks.astro_mysql_hook import AstroMySqlHook

from airflow.utils.decorators import apply_defaults
import json
import logging


class MySQLToS3Operator(BaseOperator):
    """
    MySQL to S3 Operator

    NOTE: When using the MySQLToS3Operator, it is necessary to set the cursor
    to "dictcursor" in the MySQL connection settings within "Extra"
    (e.g.{"cursor":"dictcursor"}). To avoid invalid characters, it is also
    recommended to specify the character encoding (e.g {"charset":"utf8"}).

    NOTE: Because this operator accesses a single database via concurrent
    connections, it is advised that a connection pool be used to control
    requests. - https://airflow.incubator.apache.org/concepts.html#pools

    :param mysql_conn_id:           The input mysql connection id.
    :type mysql_conn_id:            string
    :param mysql_table:             The input MySQL table to pull data from.
    :type mysql_table:              string
    :param s3_conn_id:              The destination s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The destination s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The destination s3 key.
    :type s3_key:                   string
    :param package_schema:          *(optional)* Whether or not to pull the
                                    schema information for the table as well as
                                    the data.
    :type package_schema:           boolean
    :param s3_schema_key:           *(optional)* The destination s3 key for schema.
                                    Only required if package_schema=True
    :type s3_schema_key:            string
    :param incremental_key:         *(optional)* The incrementing key to filter
                                    the source data with. Currently only
                                    accepts a column with type of timestamp.
    :type incremental_key:          string
    :param start:                   *(optional)* The start date to filter
                                    records with based on the incremental_key.
                                    Only required if using the incremental_key
                                    field.
    :type start:                    timestamp (YYYY-MM-DD HH:MM:SS)
    :param end:                     *(optional)* The end date to filter
                                    records with based on the incremental_key.
                                    Only required if using the incremental_key
                                    field.
    :type end:                      timestamp (YYYY-MM-DD HH:MM:SS)
    """

    template_fields = ['start', 'end', 's3_key', 's3_schema_key']

    @apply_defaults
    def __init__(self,
                 mysql_conn_id,
                 mysql_table,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 package_schema=False,
                 s3_schema_key=None,
                 incremental_key=None,
                 start=None,
                 end=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.mysql_table = mysql_table
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.package_schema = package_schema
        self.s3_schema_key = s3_schema_key
        self.incremental_key = incremental_key
        self.start = start
        self.end = end

    def execute(self, context):
        hook = AstroMySqlHook(self.mysql_conn_id)
        self.get_records(hook)
        if all([self.package_schema, self.s3_schema_key]):
            self.get_schema(hook, self.mysql_table)
        if self.package_schema and not self.s3_schema_key:
            logging.warning('Schema s3 key was not provided, schema was not uploaded to S3.')

    def get_schema(self, hook, table):
        logging.info('Initiating schema retrieval.')
        results = list(hook.get_schema(table))
        output_array = []
        for i in results:
            new_dict = {}
            new_dict['name'] = i['COLUMN_NAME']
            new_dict['type'] = i['COLUMN_TYPE']

            if len(new_dict) == 2:
                output_array.append(new_dict)
        self.s3_upload(data=json.dumps(output_array), s3_key=self.s3_schema_key)

    def get_records(self, hook):
        logging.info('Initiating record retrieval.')
        logging.info('Start Date: {0}'.format(self.start))
        logging.info('End Date: {0}'.format(self.end))

        if all([self.incremental_key, self.start, self.end]):
            query_filter = """ WHERE {0} >= '{1}' AND {0} < '{2}'
                """.format(self.incremental_key, self.start, self.end)

        if all([self.incremental_key, self.start]) and not self.end:
            query_filter = """ WHERE {0} >= '{1}'
                """.format(self.incremental_key, self.start)

        if not self.incremental_key:
            query_filter = ''

        query = \
            """
            SELECT *
            FROM {0}
            {1}
            """.format(self.mysql_table, query_filter)

        # Perform query and convert returned tuple to list
        results = list(hook.get_records(query))
        logging.info('Successfully performed query.')

        # Iterate through list of dictionaries (one dict per row queried)
        # and convert datetime and date values to isoformat.
        # (e.g. datetime(2017, 08, 01) --> "2017-08-01T00:00:00")
        results = [dict([k.lower(), str(v)] if v is not None else [k.lower(), v]
                   for k, v in i.items()) for i in results]
        results = '\n'.join([json.dumps(i) for i in results])
        self.s3_upload(data=results, s3_key=self.s3_key)
        return results

    def s3_upload(self, data, s3_key):
        s3 = S3Hook(aws_conn_id=self.s3_conn_id)
        s3.load_string(
            string_data=data,
            bucket_name=self.s3_bucket,
            key=s3_key,
            replace=True
        )
        logging.info('File uploaded to s3')
