import json
import os
import time
from io import BytesIO
from pprint import pprint
from zipfile import ZipFile

import pandas as pd
import requests


class QualtricsHook:

    base_url: str = "https://eu.qualtrics.com/API/v3"
    qualtrics_dump_dir: str = "qualtric_dump"
    
    base_headers = {
        'content-type': 'application/json',
        'x-api-token': None
    }


    def __init__(self, token):
        self.token: str = token 
        self.base_headers["x-api-token"] = self.token
        self._get_conn()

    def _get_conn(self):
        self.session = requests.Session()
        self.session.headers.update(self.base_headers)

    def _return_endpoint(self, path):
        return self.base_url + path

    
    def _get_survey_list_elements(self, response):
        return response.json().get("result").get("elements")

    def _get_survey_list_next_page(self, response):
        return response.json().get("result").get("nextPage")


    def get_survey_list(self):
        """
        Fetch all surveys open to the account. 
        """
        surveys_endpoint = self._return_endpoint("/surveys")
        response = self.session.get(surveys_endpoint)
        elements = self._get_survey_list_elements(response)
        next_page = self._get_survey_list_next_page(response)
        ### Handle pagination logic
        while next_page:
            response = self.session.get(next_page)
            elements += self._get_survey_list_elements(response)
            next_page = self._get_survey_list_next_page(response)
        return elements

    def get_survey_metadata(self, survey_id):
        """
        reference: https://api.qualtrics.com/guides/ZG9jOjg3NzY3Mw-managing-surveys
        """
        endpoint = self._return_endpoint(f"/surveys/{survey_id}")
        response = self.session.get(endpoint)
        return response
        
    
    def _get_progress_status_response(self, survey_id, progress_id) -> dict:
        """
        reference: https://api.qualtrics.com/guides/1179a68b7183c-retrieve-a-survey-response
        """
        endpoint = self._return_endpoint(f"/surveys/{survey_id}/export-responses/{progress_id}")
        response = self.session.get(endpoint)
        return response.json()

    def _request_survey_export(self, survey_id):
        """
        Make the initial request to download responses
        """
        
        data = {
            "format": "json",
            "compress": True,
        }

        endpoint = self._return_endpoint(f"/surveys/{survey_id}/export-responses/")
        response = self.session.post(endpoint, json.dumps(data))
        return response.json()["result"]["progressId"]

    def _get_survey_export_file_id(self, survey_id: str, progress_id: str) -> str:
        """
        Check the progress of the export generation and return the file ID, once complete
        See here:
        https://api.qualtrics.com/api-reference/b3A6NjEwNDE-get-response-export-progress
        """

        response = self._get_progress_status_response(survey_id, progress_id)

        while response["result"]["status"] == "inProgress":
            response = self._get_progress_status_response(survey_id, progress_id)
            print("In Progress, check again in 30...")
            time.sleep(30)

        if response["result"]["status"] == "failed":
            error_code = response["meta"]["error"]["errorCode"]
            error_message = response["meta"]["error"]["errorMessage"]
            exception_message = f"Qualtrics export report failed for {survey_id} progressId {progress_id}. " \
                                f"Error Code: {error_code}, Error Message {error_message}"
            raise Exception(exception_message)

        return response["result"]["fileId"]

    def _get_survey_report(self, survey_id, file_id):
        response = self.session.get(self._return_endpoint(f"/surveys/{survey_id}/export-responses/{file_id}/file"))
        return response.content

    def _export_survey_report(self, survey_content):
        zip_ref = ZipFile(BytesIO(survey_content))
        filename = "".join(zip_ref.namelist())
        export_filename = f"{self.qualtrics_dump_dir}/{filename}"
        zip_ref.extractall(self.qualtrics_dump_dir)
        zip_ref.close()
        return export_filename

    def get_survey_data(self, survey_id):
        """
        Function to fetch the responses as Zip file of a given survey
        Logic is to first request the file, it will trigger a file_id export job 
        We need to wait until the file is ready; then, we download it and save it locally
        """
        progress_id = self._request_survey_export(survey_id)
        file_id = self._get_survey_export_file_id(survey_id, progress_id)
        survey_data = self._get_survey_report(survey_id, file_id)
        filename = self._export_survey_report(survey_data)
        return filename


        

        


class QualtricCounter:
    root = "qualtric_dump"

    @classmethod
    def list_vw_files_from_root(cls):
        return [ x for x in os.listdir(cls.root) if (".json" in x)]

    def _parse_response_json(response_json: dict) -> dict:
        values = response_json.get("values")
        answer_dict = {
            "response_id": response_json.get("responseId"),
            "startDate" : values.get("startDate"),
            "finished": values.get("finished")
        }
        return answer_dict

    @classmethod
    def _load_json_as_df(cls, survey_name: str) -> pd.DataFrame:
        f_json =  json.load(open(os.path.join(cls.root, survey_name), "rb"))
        return pd.DataFrame([cls._parse_response_json(x) for x in f_json.get("responses")])

    @classmethod
    def process_single_df(cls, survey_name) -> pd.DataFrame:
        
        df_ = cls._load_json_as_df(survey_name)

        if len(df_) == 0:
            print(f"ERROR - {survey_name} is empty....")
            return df_

        # print(df_.columns)
        
        if df_.shape[1] != 3:
            print(f"ERROR - {survey_name} has duplicate keys....")
            print("Incosistent schema, Skip it")
            print(df_.columns)
            print()
            return pd.DataFrame()

        return (
            df_
            .assign(
                survey_name = survey_name,
                date =  pd.to_datetime(df_.startDate).dt.date,
                survey_start_date = pd.to_datetime(df_.startDate).dt.date.quantile(0.05)
                )
            .pivot_table(
                index=['survey_name', 'survey_start_date'],
                columns = 'finished',
                values = 'response_id',
                aggfunc = 'count'
                )
            .reset_index()
        )


class PricingSurveyMetadata:

    def _filter_vw_question(question_text):
        # USE REGEX <b>a bargain</b>
        key_words = [
            "cheap"
            , "bargain"
            , "expensive"
        ]

        return any([x in question_text for x in key_words])

    @classmethod
    def get_survey_question(cls, survey_json):
        parent = survey_json.get("result").get("questions")
        if not parent:
            return {}
        # questions = {key:parent[key]["questionText"] for key in parent.keys() if _filter_vw_question(parent[key]["questionText"])}
        questions_id = [key for key in parent.keys() if cls._filter_vw_question(parent[key]["questionText"])]
        return ",".join(sorted(questions_id))

    def get_survey_embedded_data(survey_json):
        parent = survey_json.get("result").get("embeddedData")
        if not parent:
            return {}
        emb_data = [x["name"] for x in parent]
        return ",".join(sorted(emb_data))
        

    def get_response_count(survey_json):
        parent = survey_json.get("result").get("responseCounts")
        if not parent:
            return {}
        return parent['auditable']
