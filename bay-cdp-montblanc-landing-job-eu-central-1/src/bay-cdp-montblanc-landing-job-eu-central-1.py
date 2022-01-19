"""
This job reads data from the appflow folder in the landing bucket,
which will be converted to parquet format in S3 bucket landing.
It also has an API call for populating data for the objects where
appflow fails.
-----------------------------------------------------------------
__author__ : Cognizant
__file__ : bay-cdp-montblanc-landing-job-eu-central-1.py
__created_on__: January 17, 2022
------------------------------------------------------------------
__Change_Log__
v1.0.0 : Initial
"""

import sys
import copy
import json
import boto3
import base64
import logging
import requests
from awsglue.job import Job
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.sql import Row, SQLContext
from pyspark.context import SparkContext
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StringType, StructType, StructField

# sys.path.append(os.path.abspath("/tmp/cdp_commons.zip/cdp_commons/"))
from cdp_commons import cdpUtils
from cdp_commons import glueUtils
from cdp_commons import dynamoDbAllied
from cdp_commons.loggerUtils import Logger

# Initialize Logger
logger = Logger.get_console_logger('MontBlancLandingJobLog',
                                   level=logging.INFO)


class Landing:
    """
        Class Landing for processing montblanc landing job data
    """

    def __init__(self):
        args = getResolvedOptions(sys.argv,
                                  ['JOB_NAME', 'LOAD_ID', 'JOB_RUN',
                                   'JOB_COUNTRY',
                                   'JOB_REGION', 'SOURCE_NAME'])

        self.sc = SparkContext()
        glue_context = GlueContext(self.sc)
        self.spark = glue_context.spark_session
        self.job = Job(glue_context)
        self.job.init(args['JOB_NAME'], args)

        # Reading the job parameter values in self.job_context
        self.job_context = {'job_nm': args['JOB_NAME'],
                            'load_id': args['LOAD_ID'],
                            'jb_run': args['JOB_RUN'],
                            'job_country': args['JOB_COUNTRY'],
                            'job_region': args['JOB_REGION'],
                            'source_nm': args['SOURCE_NAME'],
                            'job_layer': 'Landing'}

        self.src_sys_nm = 'sourceSystemName'
        self.ent_nm = 'entityName'
        self.cntry_cd = 'countryCode'
        self.tnt = 'tenant'

        self.job_country = self.job_context['job_country']
        self.source_nm = self.job_context['source_nm']
        self.job_region = self.job_context['job_region']
        self.load_id = self.job_context['load_id']
        self.job_name = self.job_context['job_nm']
        self.pro_id = self.source_nm + '_' + self.job_context['job_region']

        self.job_context['pro_id'] = self.pro_id
        self.job_context['job_status'] = 'success'

        self.job_strt_time = datetime.utcnow().strftime(
            '%Y-%m-%dT%H:%M:%S')
        self.job_start_time = datetime.utcnow()
        self.job_context['job_strt_time'] = self.job_start_time

        self.glue_client = boto3.client('glue')
        self.s3client = boto3.client('s3')

        # Define and instantiate all configuration tables
        self.job_cnf_tab = dynamoDbAllied. \
            DynamoDbUtils(p_table_name='by-cdp-job-config-table',
                          p_job_context=self.job_context)

        job_id = self.job_cnf_tab.get_attrib('jobId', 'processId',
                                             self.pro_id)
        self.job_context['job_id'] = job_id
        for i in self.job_context['job_id']:
            job_n = self.job_cnf_tab.scan_table('jobName', 'processId',
                                                self.pro_id, 'jobId',
                                                i)
            if ''.join(job_n) == self.job_context['job_nm']:
                self.job_context['job_id'] = i

        self.src_meta_tab = dynamoDbAllied. \
            DynamoDbUtils(p_table_name='by-cdp-source-metadata-table',
                          p_job_context=self.job_context)

        self.pro_cng_tab = dynamoDbAllied. \
            DynamoDbUtils(p_table_name='by-cdp-process-config-table',
                          p_job_context=self.job_context)

        self.pro_log_tab = dynamoDbAllied. \
            DynamoDbUtils(p_table_name='by-cdp-process-log-table',
                          p_job_context=self.job_context)

        self.job_run_det_tab = dynamoDbAllied. \
            DynamoDbUtils(p_table_name='by-cdp-job-run-details-table',
                          p_job_context=self.job_context)

        self.appflow_config_tab = dynamoDbAllied. \
            DynamoDbUtils(p_table_name='by-cdp-appflow-config-table',
                          p_job_context=self.job_context)

        # Instantiate the Utils from cdp_commons
        self.glue_utils = glueUtils.GlueUtils(
            p_job_context=self.job_context)
        self.cdp_utils = cdpUtils.CommonUtils(
            p_job_context=self.job_context)

        response = self.glue_client.get_job_runs(
            JobName=self.job_context['job_nm'])

        if response['JobRuns'][0]['JobRunState'] == "RUNNING":
            self.job_context['job_run_id'] = response['JobRuns'][0][
                'Id']
            self.job_run_id = self.job_context['job_run_id']

    def get_format_info(self, file_type, ref_path):
        """
        This function copies data from the appflow bucket
        to the landing bucket
        :param file_type: file format
        :param ref_path: source prefix
        :return: list of format info and entity count
        """
        result = self.s3client.list_objects(Bucket=self.lnd_bkt,
                                            Prefix=ref_path.lower(),
                                            Delimiter="/")
        valid_formats = ["parquet"]
        if file_type not in valid_formats:
            self.cdp_utils.enriched_log("JobFailed")

            # Update process log details table
            error_message = 'Invalid file type:' + file_type
            self.pro_log_tab.update_process_log('Failed', error_message)
            sys.exit(0)

        format_info = {"input_format": "org.apache.hadoop.hive.ql.io.parquet."
                                       "MapredParquetInputFormat",
                       "output_format": "org.apache.hadoop.hive.ql.io.parquet."
                                        "MapredParquetOutputFormat",
                       "serde_info": {"Name": " ",
                                      "SerializationLibrary": "org.apache"
                                                              ".hadoop.hive"
                                                              ".ql.io"
                                                              ".parquet.serde"
                                                              ".ParquetHive"
                                                              "SerDe",
                                      "Parameters": {
                                          "serialization.format": "1"
                                      }
                                      }, 'Parameters': {}}

        format_info['Parameters']['classification'] = 'parquet'

        fname_list = []
        for o in result.get("CommonPrefixes"):
            fname_list.append(o["Prefix"])
        entity_count = []
        format_info_list = {}

        for i in fname_list:
            current_entity = i.rsplit("/", 2)[1].lower()
            table_location = 's3://' + self.lnd_bkt + '/' + i
            df = self.spark.read.parquet(table_location)
            data_types = df.dtypes
            entity_count.append(
                {"Entity": current_entity, "Count": df.count()})
            col_schema = []
            for data_type in data_types:
                col_schema.append({'Name': data_type[0], 'Type': data_type[1]})
            format_info["Columns"] = col_schema
            format_info['Location'] = table_location
            format_info_list[current_entity] = copy.deepcopy(format_info)

        return format_info_list, entity_count

    def get_entity_name(self, file, item):
        """
        This function gets the entity name
        :param file: file name
        :param item: appflow config items
        :return: entity name
        """

        config_appflow_prefix = item['appflow_prefix'][0]
        config_slash_count = config_appflow_prefix.count("/", 0,
                                                         config_appflow_prefix
                                                         .find("<entity>"))
        config_entity_part = config_appflow_prefix.split('/')[
            config_slash_count]
        config_dash_count = config_entity_part.count(">_", 0,
                                                     config_entity_part.find(
                                                         "<entity>"))

        entity_part = file.split('/')[config_slash_count]
        entity = entity_part.split('_', config_dash_count)[config_dash_count]
        return entity

    def get_file_names(self, last_run_time, page, current_time):
        """
        This function fetches the files names which are
        modified btw last run and present run
        :param last_run_time: time stamp of last job run
        :param page: S3 page
        :param current_time: current timestamp
        :return: list of file names
        """

        files = []
        for o in page.get('Contents'):
            file = o["Key"]
            last_modified_time = str(o["LastModified"])
            last_modified_time = "T".join(last_modified_time.split("+")[0].
                                          split())

            if not file.endswith('/') and (not file.endswith('$folder$')) and \
                    last_run_time < last_modified_time <= \
                    current_time:
                files.append(file)
            elif not file.endswith('/') and (not file.endswith('$folder$')):
                temp_path = file.rsplit('/', 1)[0]
                if temp_path.endswith(self.qp_entities):
                    files.append(file)
                    print('files: ', files)
        return files

    def copy_appflow_to_landing(self):
        """
        This function copies data from the appflow bucket
        to the landing bucket
        :param NA
        :return: True/False and last run time
        """

        try:
            current_time = "{0}-{1}-{2}T{3}:{4}:{5}".format(
                self.load_id[0:4], self.load_id[4:6], self.load_id[6:8],
                self.load_id[8:10], self.load_id[10:12], self.load_id[12:])

            appflow_config_items = self.appflow_config_tab.get_multi_attrib(
                ['appflow_bucket', 'appflow_prefix', 'appflow_search_prefix',
                 'cluster', 'landing_bucket', 'landing_prefix',
                 'source_system', 'last_run_start_time'],
                'appflow_source_cluster', self.appflow_src_cluster)

            if appflow_config_items is None:
                return False

            logger.info("Last run start timestamp %s",
                        appflow_config_items['last_run_start_time'])
            logger.info("Current run start timestamp %s", current_time)

            last_run_time = appflow_config_items['last_run_start_time'][0]

            conn = boto3.resource('s3')
            file_count = 0
            j = 10
            k = 10
            log = ""

            source_bucket = appflow_config_items['appflow_bucket'][0]
            target_bucket = appflow_config_items['landing_bucket'][0]

            source_system = appflow_config_items['source_system'][0]
            cluster = appflow_config_items['cluster'][0]

            appflow_search_prefix = appflow_config_items[
                'appflow_search_prefix'][0].replace("<source-system>",
                                                    source_system).replace(
                "<cluster>", cluster)

            s3paginator = self.s3client.get_paginator('list_objects')
            page_iterator = s3paginator.paginate(Bucket=source_bucket,
                                                 Prefix=appflow_search_prefix)

            for page in page_iterator:
                files = self.get_file_names(
                    appflow_config_items['last_run_start_time'][0], page,
                    current_time)

                for file in files:
                    copy_source = {
                        'Bucket': source_bucket,
                        'Key': file
                    }

                    entity = self.get_entity_name(file, appflow_config_items)
                    f_name = file.rsplit('/', 3)[3]

                    t_file = appflow_config_items['landing_prefix'][0].replace(
                        '<source-system>', source_system).replace(
                        '<cluster>', cluster).replace('<loadid>',
                                                      self.load_id).replace(
                        '<entity>',
                        entity).replace('<file>', f_name)

                    conn.meta.client.copy(copy_source, target_bucket, t_file)
                    log = "file " + f_name + " copied from " + source_bucket \
                          + "/" + file.rsplit("/", 1)[0] + " to " + \
                          target_bucket + "/" + t_file
                    file_count += 1

                    if file_count < 10:
                        logger.info(
                            "File count " + str(file_count) + ": " + log)
                    elif file_count == j:
                        logger.info(
                            "File count " + str(file_count) + ": " + log)
                        j += k
                        if k <= 100 and j // k == 10:
                            k *= 10

            logger.info("File count " + str(file_count) + ": " + log)
            if file_count:

                self.appflow_config_tab.update_table(
                    pkey_val=self.appflow_src_cluster,
                    p_key_list=['last_run_start_time'],
                    p_value_list=[current_time])

                logger.info("last_run_start_time updated in the config table "
                            "for key: " + self.appflow_src_cluster)
                logger.info(str(file_count) + " files copied from appflow to"
                                              " landing layer")
                return True, last_run_time
            else:
                return None, last_run_time
        except Exception as e:
            # Update process log details table
            error_message = 'Unexpected error in copy_appflow_to_landing:' \
                            + self.cdp_utils.cleanse(str(e)), "FailedTime:" \
                            + datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            self.pro_log_tab.update_process_log('Failed', error_message)
            sys.exit(0)

    def get_secret(self, secrets):
        """
        This function fetches the data stored in Secrets Manager.
        :param secrets: secret manager details
        :return: dict of credentials
        """

        secret_name = secrets["secret_name"]
        region_name = self.tenant_nm

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                raise e
            elif e.response['Error']['Code'] == \
                    'InternalServiceErrorException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return secret
            else:
                decoded_binary_secret = base64.b64decode(
                    get_secret_value_response['SecretBinary'])
                return decoded_binary_secret

    def api_call(self, spark, sessionid, api_url, e, appflow_prefix,
                 appflow_bucket, query_type, col_names=None):
        """
        This function performs the api call and writes the data into
        respective folder for the object in s3 appflow landing folder.
        :param spark:
        :param sessionid: authentication id
        :param api_url: api url
        :param e: entity name
        :param appflow_prefix: landing bucket prefix
        :param appflow_bucket: landing bucket name
        :param query_type: api query type (query or picklist)
        :param col_names: column names for the entity e
        :return: NA
        """

        appflow_prefix = appflow_prefix.split("<file>")[0]
        appflow_path = 's3://' + appflow_bucket + "/" + appflow_prefix.replace(
            "<source-system>", self.source_nm).replace("<cluster>",
                                                       self.job_country). \
            replace("<entity>", e).replace("<folder>", self.load_id)
        appflow_path = appflow_path.lower()

        request = requests.get(api_url, headers={"Authorization": sessionid})
        response = request.json()
        if ((response['responseStatus'] == 'FAILURE') or (
                response['responseStatus'] == 'EXCEPTION')):

            self.cdp_utils.enriched_log("JobFailed")

            # Update process log details table
            error_message = 'Error in obtaining data from MontBlanc API call' \
                            ' entity: ' + self.cdp_utils.cleanse(str(
                response['errors']))
            self.pro_log_tab.update_process_log('Failed', error_message)
            sys.exit(0)

        else:
            if query_type == "query":
                data = response["data"]

                next_page = response['responseDetails'].get('next_page', None)
                next_page_url = api_url.split('.com')[0] + ".com"
                while next_page:
                    api_url = next_page_url + next_page
                    r = requests.get(api_url,
                                     headers={"Authorization": sessionid})
                    response = r.json()
                    data.extend(response["data"])
                    next_page = response['responseDetails'].get('next_page',
                                                                None)
            else:
                # Update process log details table
                error_message = 'Invalid MontBlancTableType: ' + query_type
                self.pro_log_tab.update_process_log('Failed', error_message)
                sys.exit(0)

            logger.info("Data read from API call completed for entity: " + e)
            if len(data) > 0:
                struct = []
                for i in col_names:
                    struct.append(StructField(i, StringType(), True))
                schema = StructType(struct)
                rows = [Row(**x) for x in data]
                df1 = spark.createDataFrame(rows, schema=schema)

                logger.info(
                    "Starting to copy data to appflow for entity: " + e)
                df1.write.format("parquet").mode("overwrite").save(
                    appflow_path)
                logger.info("Data copied to appflow for entity: " + e)

            else:
                logger.info("No data received from API call for entity: " + e)

    def montblanc_api(self):
        """
        This function fetch data for objects cannot be ingested
        via appflow using an API call.
        :param NA
        :return: NA
        """
        self.spark = SQLContext(self.sc)

        logger.info('Starting montblanc_api')
        self.appflow_src_cluster = self.source_nm + "-" + self.job_country
        self.appflow_src_cluster = self.appflow_src_cluster.lower()

        out = self.appflow_config_tab.get_multi_attrib(['appflow_prefix',
                                                        'appflow_bucket'],
                                                       'appflow_source_cluster',
                                                       self.appflow_src_cluster)

        appflow_prefix = out['appflow_prefix'][0]
        appflow_bucket = out['appflow_bucket'][0]

        response_url = self.pro_cng_tab.get_multi_attrib(['sourceDetails',
                                                          'secrets'],
                                                         self.src_sys_nm,
                                                         self.source_nm)
        auth_url = response_url['sourceDetails'][0]['url_token']
        api_url = response_url['sourceDetails'][0]["url_retrieve"]
        secrets = response_url["secrets"][0]

        query_entity_list = self.src_meta_tab.scan_table('entityName',
                                                         self.src_sys_nm,
                                                         self.source_nm,
                                                         'MontBlancTableType',
                                                         'query')

        creds = json.loads(self.get_secret(secrets))
        params = {
            "username": creds[secrets['uname']],
            "password": creds[secrets['pwd']]
        }
        response_sessionid = requests.post(auth_url, params=params)

        if ((response_sessionid.json()['responseStatus'] == 'FAILURE') or (
                response_sessionid.json()['responseStatus'] == 'EXCEPTION')):
            self.cdp_utils.enriched_log("JobFailed")

            # Update process log details table
            error_message = 'Error in obtaining sessionId for MontBlanc API ' \
                            'call: ' + self.cdp_utils.cleanse(str(
                response_sessionid.json()['errors']))
            self.pro_log_tab.update_process_log('Failed', error_message)
            sys.exit(0)

        sessionid = response_sessionid.json().get("sessionId", None)
        if (sessionid):
            logger.info("Session Id received")

        logger.info("Starting to process Query type entities: " + str(
            query_entity_list))
        current_time = "{0}-{1}-{2}T{3}:{4}:{5}.000Z".format(
            self.load_id[0:4], self.load_id[4:6], self.load_id[6:8],
            self.load_id[8:10], self.load_id[10:12], self.load_id[12:])

        for e in query_entity_list:
            logger.info("Query Entity Name: " + e)
            query_url = api_url + "query?q=SELECT+"
            sourceMetadataId = self.job_country + "_" + e.lower()

            timestamp_field = self.src_meta_tab.get_attrib(
                'timestamp_fields', "sourceMetadataID", sourceMetadataId,
                "sourceSystemName", self.source_nm)

            response = self.src_meta_tab.get_multi_attrib(['columnMetadata',
                                                'MontBlancLastRunTimeStamp'],
                                                "sourceMetadataID",
                                                sourceMetadataId,
                                                "sourceSystemName",
                                                self.source_nm)
            montblanc_last_run_timestamp = response[
                'MontBlancLastRunTimeStamp'][0]

            col_names = []
            for i in response['columnMetadata'][0]:
                col_names.append(i["Name"])

            logger.info(col_names)

            col_names_joined = "%2C+".join(col_names)
            query_url = query_url + col_names_joined + "+FROM+" + e + \
                        "+WHERE+" + timestamp_field[0] + "+BETWEEN+'" + \
                        montblanc_last_run_timestamp.replace(':', '%3A') + \
                        "'+AND+'" + current_time.replace(':', '%3A') + "'"
            logger.info("QUERY URL: " + query_url)

            self.api_call(self.spark, sessionid, query_url, e, appflow_prefix,
                          appflow_bucket, "query", col_names)

            self.src_meta_tab.update_table(sourceMetadataId,
                        self.source_nm, ['MontBlancLastRunTimeStamp'],
                        [current_time])

            logger.info("MontBlanc lastRunTimestamp: " + current_time +
                        " updated for entity:" + e)

    def main(self):
        """
        This main function describes the process flow for montblanc
        landing job.
        :param NA
        :return: NA
        """

        if self.job_context['jb_run'] == "1":
            try:
                logger.info("This job is running for the source %s and the  "
                            "cluster %s and the job region %s for the "
                            "load_id  %s", self.source_nm, self.job_country,
                            self.job_region, self.load_id)

                logger.info(
                    'Job run id fetched :' + self.job_context['job_run_id'])

                out = self.src_meta_tab.get_multi_attrib(['subjectArea',
                                                          'entityName',
                                                          'entityStatus',
                                                          'dataProduct'],
                                                         self.src_sys_nm,
                                                         self.source_nm,
                                                         self.cntry_cd,
                                                         self.job_country)

                entity_list = []
                dp_list = []
                subject_area_list = []
                for status, entity, dp, subject in zip(out['entityStatus'],
                                                       out['entityName'],
                                                       out['dataProduct'],
                                                       out['subjectArea']):
                    if status == "1":
                        entity_list.append(entity)
                        dp_list.append(dp)
                        subject_area_list.append(subject)

                self.tenant_nm = self.pro_cng_tab.get_attrib('tenant',
                                                             self.src_sys_nm,
                                                             self.source_nm)

                # since job_region is GLOBAL
                self.tenant_nm = self.tenant_nm[0]

                out = self.pro_cng_tab.get_multi_attrib(["landingPrefix",
                                                         'sourceFormat',
                                                         'landingDatabase',
                                                         'landingBucket',
                                                         'sourcePrefix',
                                                         'landingCrawlers'],
                                                        self.src_sys_nm,
                                                        self.source_nm,
                                                        self.tnt,
                                                        self.tenant_nm)

                self.lnd_prx = out['sourcePrefix'][0]
                self.lnd_bkt = out['landingBucket'][0]
                self.file_type = out['sourceFormat'][0]
                self.db_name = out['landingDatabase'][0]
                self.db_name = self.db_name.replace("<source_system>",
                                                    self.source_nm).replace(
                    "<country>",
                    self.job_context['job_country'])

                self.cdp_utils.enriched_log("JobStarted")

                logger.info(
                    'Enriched job log entitylist:   ' + str(entity_list))

                # ----------------Restartability start-------------------
                filter_exp1 = 'layer = :r AND loadId = :ld AND processId = :' \
                              'pd  AND countryCode = :cd AND jobRunStatus = ' \
                              ':js'
                db_client = boto3.client('dynamodb')
                paginator = db_client.get_paginator('scan')
                response_iterator = paginator.paginate(
                    TableName='by-cdp-process-log-table',
                    FilterExpression=filter_exp1,
                    ExpressionAttributeValues={
                        ":r": {'S': 'Landing'},
                        ":ld": {'S': self.load_id},
                        ":pd": {'S': self.pro_id},
                        ":cd": {'S': self.job_country},
                        ":js": {'S': 'success'}
                    }
                )

                is_rerun = False
                for page in response_iterator:
                    for item in page['Items']:
                        is_rerun = True
                        break
                if is_rerun:
                    logger.info("This is a rerun for loadId:" + self.load_id)
                # ----------------Restartability end-------------------

                if not is_rerun:
                    self.montblanc_api()

                    self.qp_entities = []
                    response_query_entities = self.src_meta_tab.scan_table(
                        'entityName', self.src_sys_nm, self.source_nm,
                        'MontBlancTableType', 'query')

                    self.qp_entities.extend(response_query_entities)

                    self.qp_entities = tuple([f'{self.source_nm}'
                                              f'_{self.job_country}_{x}'
                                              f'/{self.load_id}'.lower() for x
                                              in
                                              self.qp_entities])

                    # Appflow file move
                    current_time = "{0}-{1}-{2}T{3}:{4}:{5}".format(
                        self.load_id[0:4], self.load_id[4:6],
                        self.load_id[6:8], self.load_id[8:10],
                        self.load_id[10:12], self.load_id[12:])

                    is_files_copied, lastruntime = \
                        self.copy_appflow_to_landing()

                    # in case no files are copied but the folder with
                    # the given load id is present in landing path and
                    # having files in it, delete and recreate the tables.
                    paginator = self.s3client.get_paginator('list_objects')
                    src_nm = self.source_nm.lower()
                    jb_cntry = self.job_country.lower()
                    appflow_search_prefix = src_nm + "/appflow/" + jb_cntry \
                                            + "/"
                    total_appflow = 0
                    pageiterator = paginator.paginate(Bucket=self.lnd_bkt,
                                                Prefix=appflow_search_prefix)

                    # to run the crawler again for the same load id
                    if lastruntime == current_time:
                        total_appflow = 1

                    for page in pageiterator:
                        for file in page['Contents']:
                            lastmodified = str(file["LastModified"])
                            lastmodified = "T".join(
                                lastmodified.split("+")[0].split())
                            if not file['Key'].endswith('/') and \
                                    lastruntime < lastmodified <= current_time:
                                total_appflow += 1
                else:
                    # For running crawler in case of rerun
                    total_appflow = 1
                    is_files_copied = 0

                logger.info("Status variables===> total_appflow:" + str(
                    total_appflow) + ", is_files_copied:" + str(
                    is_files_copied))
                if is_files_copied or total_appflow:
                    refpath = self.lnd_prx.replace("<country>",
                                                   self.job_country).replace(
                        "<loadid>", self.load_id)

                    try:
                        format_info_list, row_count_per_entity = \
                            self.get_format_info(self.file_type, refpath)

                        logger.info("Starting to create glue tables")

                        response = self.glue_utils.glue_get_tables(
                            self.db_name)

                        delete_flag = False
                        for i in response['TableList']:
                            table_name = i['Name']
                            self.glue_utils.delete_table(self.db_name,
                                                         table_name)
                            delete_flag = True

                        if delete_flag:
                            logger.info('Table deletion success')

                        for table_name in format_info_list.keys():
                            logger.info("Creating landing glue table for "
                                        "entity: " + table_name)
                            format_info = format_info_list[table_name]

                            partition_keys = []
                            self.glue_utils.glue_create_table(self.db_name,
                                                              table_name,
                                                              format_info[
                                                                  "Columns"],
                                                              format_info[
                                                                  "Location"],
                                                              partition_keys,
                                                              format_info)

                        logger.info("Glue tables created successfully")

                        job_end_time = datetime.utcnow().strftime(
                            '%Y-%m-%dT%H:%M:%S')
                        job_status = "success"
                        count_dict = {}
                        for items in row_count_per_entity:
                            self.job_run_det_tab.update_job_run_details(items[
                                                            'Entity'].lower(),
                                                            items['Count'], 0,
                                                            self.job_strt_time,
                                                            job_end_time,
                                                            job_status)

                            count_dict[items['Entity'].lower()] = items[
                                'Count']

                        for entity, subject, dp in zip(entity_list,
                                                       subject_area_list,
                                                       dp_list):
                            if entity in count_dict.keys():
                                count = count_dict[entity]
                            else:
                                continue

                            self.cdp_utils.enriched_log("EntityProcessed "
                                                        ": {}".format(
                                str(count)))

                        self.cdp_utils.enriched_log("JobSuccess")

                    except Exception as e:
                        self.cdp_utils.enriched_log("JobFailed")

                        # Update process log details table
                        error_message = 'Unexpected error in main function: ' \
                                        + self.cdp_utils.cleanse(str(e))
                        self.pro_log_tab.update_process_log('Failed',
                                                            error_message)
                        sys.exit(0)

                # if there is no files to be processed for the
                # new load ID , deleting the existing landing
                # tables
                elif total_appflow == 0:
                    try:
                        response = self.glue_utils.glue_get_tables(
                            self.db_name)

                        delete_flag = False
                        for i in response['TableList']:
                            table_name = i['Name']
                            self.glue_utils.delete_table(self.db_name,
                                                         table_name)
                            delete_flag = True

                        if delete_flag:
                            logger.info('Table deletion success')

                        logger.info('Enriched Job End')
                        self.cdp_utils.enriched_log('JobSuccess')
                        # Update process log details table
                        self.pro_log_tab.update_process_log('success')

                    except Exception as e:
                        self.cdp_utils.enriched_log("JobFailed")

                        # Update process log details table
                        error_message = 'Unexpected error in main ' \
                                        'function: ' + \
                                        self.cdp_utils.cleanse(str(e))
                        self.pro_log_tab.update_process_log('Failed',
                                                            error_message)
                        sys.exit(0)
                else:
                    self.cdp_utils.enriched_log('No files to process')
                    # Update process log details table
                    self.pro_log_tab.update_process_log('success')

            except Exception as e:
                self.cdp_utils.enriched_log("JobFailed")

                # Update process log details table
                error_message = 'Unexpected error in main function: ' + \
                                self.cdp_utils.cleanse(str(e))
                self.pro_log_tab.update_process_log('Failed', error_message)
                sys.exit(0)

        else:
            logger.info(
                'The job - ' + self.job_context['job_nm'] + 'will not be '
                                                            'executed. '
                                                            'Please reset '
                                                            'job parameter '
                                                            'JOB_RUN as 1 '
                                                            'and try '
                                                            'again...')
            self.cdp_utils.enriched_log("JobFailed",
                                        p_error_message="Please reset job "
                                                        "parameter JOB_RUN "
                                                        "as 1 and try again")

        self.job.commit()


if __name__ == '__main__':
    l = Landing()
    l.main()
