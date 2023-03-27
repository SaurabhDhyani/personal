from urllib.parse import quote_plus
import asyncio
import aiohttp
import base64
import boto3
import json
import pymysql
import os
import time
from log import Log
from datetime import datetime

# This is a service for the identication of logos in video, but it accepts video frames to this service for logo detections


class LogoDetectionInVideo:
    def __init__(self, **kwargs):
        """
        Class object accepts the keyword arguments
        """
        self.file_path = kwargs.get("file_path")
        self.headers = kwargs.get("headers")
        self.visua_url = kwargs.get("visua_url")
        self.working_bucket = kwargs.get("working_bucket")
        self.wait_duration = 15
        self.s3client = kwargs.get("s3client")
        self.sns_client = kwargs.get("sns_client")
        self.job_id = kwargs.get("job_id")
        self.final_json_filepath = None
        self.job_pipline_sns = kwargs.get("job_pipline_sns")
        self.db_connection = kwargs.get("db_connection")
        self.cursor = kwargs.get("cursor")
        self.time_gap = 3
        # self.logger = kwargs.get("logger") # It can be used in future temporarily commented
        # self.session = aiohttp.ClientSession()

    def convert_byte_img_to_base64(self, f):
        """
        Converts byte object to base64 format
        Arguments: f-> byte object
        return: -> base64 decoded string object
        """
        try:
            my_string = base64.b64encode(f)
            return my_string.decode("utf-8")
        except Exception as e:
            # self.logger.log('ERROR', f"Error at converting Images to base64:{e}", self.convert_byte_img_to_base64.__name__)
            print(f"Error at converting Images to base64:{e}")

    def upload_data_to_s3(self,detections=[]):
        """
        After getting the detections store in s3 bucket.
        return: boolean or dict str type
        """

        print(f"Found detections:: {detections}")
        if len(detections) == 0:
            return True
        
        try:
            file_path = '/'.join(self.file_path[:-2])+'/'
            # self.logger.log('DEBUG', f'Detections uploading file_path:{file_path}', self.upload_data_to_s3.__name__)
            print(f'Detections uploading file_path:{file_path}')
            # bucket = self.s3client.list_objects_v2(
            #     Bucket=self.working_bucket, Prefix=file_path)
            json_file = f"{file_path}{self.job_id}.json"
            try:
                fileObj = self.s3client.get_object(
                    Bucket=self.working_bucket, Key=json_file)
                filecontent = fileObj["Body"].read()
                filecontent = json.loads(filecontent)
                print(filecontent)
                if not isinstance(filecontent, dict):
                    print("Output json not a dictionary, file can't be processed")
                    return False
                if not 'logos' in filecontent.keys():
                    print("Creating LOGO list")
                    filecontent = {}

                else:
                    filecontent = filecontent['logos']

                logos_list = detections.keys()
                print("logos_list")
                print(logos_list)
                for logo in logos_list:
                    print(f"logo {logo}")
                    # print(detections[logo])
                    for detection in detections[logo]:
                        print("Adding detrcting to the logos")
                        print(detection)
                        print(filecontent)

                        if not len(filecontent) or not logo in filecontent.keys():
                            print("assigning empty list and adding")
                            filecontent[logo] = []
                            filecontent[logo].append(detection)
                            filecontent[logo].sort(key=lambda e: e['start_time'])
                            continue
                            
                        '''
                        Checking the duplicate with logo and start time
                        We will not get the complete job results a once
                        Updating output json with 1st set of resulsts
                        If there are any requests pending at vendor we will get next time
                        Due to that we are checking duplicates here and get_results_from_vendor() method
                        '''
                        print("Checking duplicates")
                        print(filecontent[logo])
                        duplicate_check = len(next((item for item in filecontent[logo] if item["start_time"] == detection['start_time']),{}))
                        print(f"Duplicate check {duplicate_check}")
                        if duplicate_check > 0:
                            print("Duplidate")
                            print(detection)
                            continue
 
                        print("Adding detection time to exiting logo")
                        filecontent[logo].append(detection)
                        filecontent[logo].sort(key=lambda e: e['start_time'])
                
                print("Updating the timings")
                filecontent = self.update_logo_end_time(filecontent)
                # print(filecontent)
                # filecontent['logos'] = logo_data
                print("Final data:")
                print(filecontent)
                print("Adding to Json")
                self.upload_json_to_deliveries(filecontent)
                return True

            except Exception as e:
                print(e)

                
        except Exception as e:
            print(e)
            # self.logger.log('ERROR', str(e), self.upload_data_to_s3.__name__)
            return e
    def upload_json_to_deliveries(self, logo_detections):
        file_path = '/'.join(self.file_path[:-2])+'/'
        print(f'Detections uploading file_path:{file_path}')
        output_storage_path = f"{file_path}{self.job_id}.json"
        
        s3 = boto3.client("s3")

        try:
            s3.head_object(Bucket=self.working_bucket, Key=output_storage_path)

            resource = boto3.resource('s3')
            content_object = resource.Object(self.working_bucket, output_storage_path)
            file_content = content_object.get()['Body'].read().decode('utf-8')
            final_data = []
            final_data = json.loads(file_content)
            # final_data['logos'].extend(logo_detections)
        except Exception as e:
            print(e)
            final_data = {}

        final_data['logos']=logo_detections
        
        print("Json Data:")
        print(final_data)
        
        local_temp_path = '/tmp/'+self.job_id+'.json'
        target_file = open(local_temp_path, 'w')
        target_file.write(json.dumps(final_data))
        target_file.close()
        s3.upload_file(local_temp_path, self.working_bucket, output_storage_path)
        return True

    '''
    If same logo occurring with a gap of N (time_gap) 
    then updating the 1st occurrence end_time with last occurrence of logo end_time
    Ex: {"NBC":[{"start_time":1, "end_time":1, "confidence":1},{"start_time":2, "end_time":2, "confidence":1},
    {"start_time":4, "end_time":4, "confidence":1},{"start_time":5, "end_time":10, "confidence":1}]
    
    final_data = {"NBC":[{"start_time":1, "end_time":2, "confidence":1}}, {"start_time":5, "end_time":10, "confidence":1}]
    '''
    def update_logo_end_time(self, logo_data):
        print("update_logo_end_time:START")
        final_data={}
        # final_data={}
        logos = logo_data.keys()
        try:
            for logo in logos:
                print(logo)
                previous_end_time = ''
                current_start_time = ''
                for detection in logo_data[logo]:
                    if not logo in final_data.keys():
                        final_data[logo] = []
                    # print(detection)
                    current_start_time = detection['start_time']
                    if previous_end_time != '' and current_start_time <= previous_end_time:
                        print("Updating previos end time")
                        print(detection)
                        # logo_data[logo].append(detection)
                        logo_data_length = len(final_data[logo])
                        # print(f"Len of list {logo_data_length}")
                        # print(f"current_start_time {current_start_time}")
                        final_data[logo][logo_data_length-1]['end_time'] = detection['end_time']
                        previous_end_time = detection['end_time']+int(self.time_gap)
                        # filecontent['logos'][logo].sort(key=lambda e: e['start_time'])
                    else:
                        print("Appending detection")
                        print(detection)
                        # print(f"Previous end time {previous_end_time}")
                        final_data[logo].append(detection)
                        previous_end_time = detection['end_time']+int(self.time_gap)
        except Exception as e:
            print("Failed to update the end timiings")
            return logo_data
        print(final_data)
        print("update_logo_end_time:END")
        return final_data   

    def publish_status_to_sns(self):
        """
        After completing the process update status.
        return: boolean or dict type
        """
        final_result = self.upload_data_to_s3()
        if final_result == True:
            self.cursor.execute(f"select id from service_progress where service_id =101 and job_id={self.job_id}")
            service_progress_id = self.cursor.fetchone().get("id")
            service_progress_update = f"UPDATE service_progress SET status = 'FINAL', output_source='{self.final_json_filepath}' WHERE id={service_progress_id}"
            print(service_progress_update)
            self.cursor.execute(service_progress_update)
            self.db_connection.commit()
            snsAttributes = {}
            snsAttributes['job_id'] = {
                    'DataType': 'String', 'StringValue': str(self.job_id)}
            response = self.sns_client.publish(
                    TargetArn=self.job_pipline_sns,
                    Subject='Operational conformance complete',
                    Message='Operational Conformance complete with job id ' +
                    str(self.job_id),
                    MessageStructure='string',
                    MessageAttributes=snsAttributes
                )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True
            else:
                return response
        else:
            return final_result

    def add_update_inprogress_jobs(self, file_name, req_job_session=[], new_file=False):
        print("updating*****")
        json_file = '/'.join(self.file_path[:-2])+'/'+file_name
        # json_file = f"{file_path}inprogress.json"
        
        
        final_data = []
        if not new_file:
            try:
                self.s3client.head_object(Bucket=self.working_bucket, Key=json_file)
                resource = boto3.resource('s3')
                content_object = resource.Object(self.working_bucket, json_file)
                file_content = content_object.get()['Body'].read().decode('utf-8')
                final_data = json.loads(file_content)
                # print(final_data)
            except Exception as e:
                print(e)
                pass
        # else:
        #     req_job_session=[]

        # print("TEST")
        final_data.extend(req_job_session)
        # Updating file with unique session ids
        unique_data = list({data['session_id']:data for data in final_data}.values())
        r = self.s3client.put_object(Bucket=self.working_bucket, Key=json_file, Body=json.dumps(unique_data, indent=2, default=str))
        print(f"Inprogress jobs updated to {json_file} :{r}")
        if r["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return True
        else:
            print("File creation failed")

    async def get_results_from_vendor(self, session_request_file_name):
        # return "TEST"
        session_request_file = '/'.join(self.file_path[:-2])+'/'+session_request_file_name
        # return session_request_file
        resource = boto3.resource('s3')
        content_object = resource.Object(self.working_bucket, session_request_file)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        # return file_content
        req_sessions_ids = json.loads(file_content)
        if(len(req_sessions_ids) == 0):
            return False
        # return json.loads(json.dumps(file_content, default=str))
        # return sessions_list
        final_data = {}
        data_set = []
        # inprogress_requests = []
        print("Sleep START")
        await asyncio.sleep(self.wait_duration)
        print("Sleep END")
        print(f"Request session ids created in vendor:: {req_sessions_ids}")
        # temp_req_sessions_ids = req_sessions_ids.copy()
        completed_requestes = []
        try:
            async with aiohttp.ClientSession() as session:
                print("req_sessions_ids")
                print(len(req_sessions_ids))
                for index in range(len(req_sessions_ids)):
                    v_url = f"{self.visua_url}/{req_sessions_ids[index].get('session_id')}/response"
                    print(v_url)
                    async with session.get(v_url, headers=self.headers) as response:
                        status_code = response.status
                        if status_code == 202:
                            print("202")
                            print(req_sessions_ids[index])
                            continue
                        elif status_code == 200:
                            print("200")
                            # print(index)
                            print(req_sessions_ids[index])
                            data = await response.json()
                            print(f"Data{data}")
                            for detections in data['data']['detections']:
                                start_time = float(req_sessions_ids[index].get("image_time"))
                                if float(detections['confidence']) >= 0.8:
                                    # if any(content['element'] == 'NBC' and content["data"][0]["start_time"] == 5.1 for content in filecontent['logos'])
                                    if not detections['name'] in final_data.keys():
                                        final_data[detections['name']] = []
                                    else:
                                        try:
                                            # Checking duplicates and skipping that
                                            duplicate_check=len(next((item for item in final_data[detections['name']] if item["start_time"] == start_time),{}))
                                        except Exception as e:
                                            duplicate_check = 0
                                            print("Duplicate check failed")
                                            print(e)
                                        if duplicate_check > 0:
                                            continue
                                    
                                    final_data[detections['name']].append({"start_time":float(req_sessions_ids[index].get("image_time")),
                                                                            "end_time":float(req_sessions_ids[index].get("image_time")),
                                                                            "confidence":detections["confidence"]})
                            completed_requestes.append(req_sessions_ids[index])
                            
                # print("final_data")
                # print(final_data)
                self.upload_data_to_s3(final_data)
                req_sessions_ids = [ele for ele in req_sessions_ids if ele not in completed_requestes]
                print("202 requestes")
                print(req_sessions_ids)
                self.add_update_inprogress_jobs(session_request_file_name, req_sessions_ids, new_file=True)
        except Exception as e:
            print('Error in reading detections'+str(e))
            
        return final_data

    async def buildRequest(self, session, file):
        fileObj = self.s3client.get_object(Bucket=self.working_bucket, Key=file)
        filecontent = fileObj["Body"].read()
        # v_url = self.visua_url
        payload = f'mediaBase64={quote_plus(self.convert_byte_img_to_base64(filecontent))}'
        async with session.post(self.visua_url, headers=self.headers, data=payload) as response:
            # async with session.get(api_end_point) as response:
            res = await response.json()
            # print(res)
            return res

    async def get_thumbnail_and_post_to_visua(self, jpg_files):
        """
        Read thumnails from s3 bucket then sends for base64 conversion after that the result will be converted to 
        URLencoded format next it will send to VISUA service
        Arguments: session-> object type
        return: list type
        """
        req_sessions_ids = []
        count=0

        frame_images = []
        session = aiohttp.ClientSession()
        # async with aiohttp.ClientSession() as session:
        for file in jpg_files:
            frame_images.append(asyncio.create_task(self.buildRequest(session, file)))
            # session.post(v_url, headers=self.headers, data=payload)))
        session.close()
        
        print('started posting thumbnails to visua')
        responses = await asyncio.gather(*frame_images)

        file_name = jpg_files[0].split('/')[-1]
        file_time = file_name.split('.')[0].lstrip('0')
        if(file_time == ''):
            file_time = 1
        file_time = int(file_time)
        for response in responses:
            if('data' in response.keys()):

                try:
                    image_time = file_time-1
                    req_sessions_ids.append({"session_id":response["data"]["sessionId"], "image_time":image_time})
                except KeyError as k:
                    # self.logger.log('ERROR', 'Error in reading session_ids'+str(k), self.get_session_ids.__name__)
                    print('Error in reading session_ids'+str(k))
                file_time = file_time+1
        print(req_sessions_ids)
        self.add_update_inprogress_jobs('session_request.json', req_sessions_ids)


    def get_all_images(self):

        self.add_update_inprogress_jobs('session_request.json', [], True) #creating empty json file to store request sessions

        file_path = '/'.join(self.file_path[:-1])+'/'
        # self.logger.log('DEBUG', f'Thumbnails folder path:{file_path}', self.get_thumbnail_and_post_to_visua.__name__)
        print(f'Thumbnails folder path:{file_path}')

        next_token = 'True'
        count = 1
        
        jpg_content = []

        frame_images = []
        time_count = 0
        while True:
            # jpg_files = []
            paginator = self.s3client.get_paginator('list_objects')
            operation_parameters = {'Bucket': self.working_bucket,
                        'Prefix': file_path, 'PaginationConfig':{'PageSize': 500,'StartingToken': next_token}}
            page_iterator = paginator.paginate(**operation_parameters)
            print(f"Page Iterator{page_iterator}")

            for page in page_iterator:
                jpg_files = []
            # next_token = page['Marker']
                # print(count)
                jpg_files.extend([x['Key'] for x in page['Contents'] if x['Key'].endswith('.jpg')])
                # print(jpg_files)
                print(f"PAGE:{page}")
                asyncio.run(self.get_thumbnail_and_post_to_visua(jpg_files))
                
            if(page["IsTruncated"] == False):
                break
        
        return jpg_content

def lambda_handler(event, context):
    job_id = event['Records'][0]['Sns']['MessageAttributes']['job_id']['Value']
    print(f"job_id:{job_id}")
    visua_url = os.environ['VISUA_URL']
    working_bucket = os.environ['WORKING_BUCKET']
    s3 = boto3.client('s3', region_name=os.environ['REGION'])
    sns = boto3.client('sns', region_name=os.environ['REGION'])
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'X-DEVELOPER-KEY': os.environ['VISUA_API_KEY']}
    alias = context.invoked_function_arn.split(':')[-1]
    if alias == context.function_name:
        alias = 'DEV'
    response = s3.get_object(
        Bucket=os.environ['CONFIG_BUCKET'],
        Key=str(alias) + '/config.json'
    )
    env = (json.loads(response['Body'].read()))
    try:
        con = pymysql.connect(host=env['RDS_HOST'], user=env['RDS_USERNAME'],
                              password=env['RDS_PASSWORD'], db=env['RDS_DATABASE'])
        cursor = con.cursor(pymysql.cursors.DictCursor)
        MAX_RETRY = 20
        while True:
            # logger.log('DEBUG', 'Waiting for AutmatedTranscode status to be FINAL', lambda_handler.__name__)
            print(f'Waiting for AutmatedTranscode status to be FINAL {job_id}')
            cursor.execute(
                f"select * from service_progress where service_id = 102 and job_id = {job_id}")
            # time.sleep(5)
            result = cursor.fetchone()
            if result is not None and result['status'] == "FINAL":
                file_path = result.get("output_source")
                file_path = file_path.split('/')
                obj = LogoDetectionInVideo(file_path=file_path, headers=headers, visua_url=visua_url,
                                           working_bucket=working_bucket, s3client=s3, job_id=job_id, sns_client=sns, job_pipline_sns=env['JOB_PIPELINE_SNS'], db_connection=con, cursor=cursor)
                # logger.log('INFO', 'Thumbnails are generated , status became final', lambda_handler.__name__)
                print('Thumbnails are generated , status became final')
                if "Generate Thumbnails" in file_path:
                    # asyncio.run(obj.loop_inprogress())
                    # final_status = obj.loop_inprogress()
                    print("********CALLING GET ALL IMAGES************")
                    obj.get_all_images()
                    print("********CALLING GET RESULTS FROM VENDOR************")
                    asyncio.run(obj.get_results_from_vendor('session_request.json'))
                    final_status = obj.publish_status_to_sns()
                    max_retry = 10
                    while True:
                        print(f"*** AGAIN CALLING*** {max_retry}")
                        res = asyncio.run(obj.get_results_from_vendor('session_request.json'))
                        max_retry = max_retry-1
                        if(res == False or max_retry <=0):
                            break
                    
                    if final_status == True:
                        return {"code": 200, "message": "Process completion successful, Status Upated to FINAL"}
                    else:
                        print(f"sns_status:{final_status}")
            else:
                time.sleep(5)
                MAX_RETRY = MAX_RETRY-1
                if MAX_RETRY <= 0:
                    break
                
        cursor.close()
        con.close()
    except Exception as e:
        # logger.log('ERROR', str(e), lambda_handler.__name__)
        print(e)
        return {"code": 500, "message": "Process Interrupted"}
