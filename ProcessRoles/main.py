import ast
import base64
import io
import json
import logging
import os
import time
import uuid

import PyPDF2
import docx
import openai
import requests
import azure.functions as func
from google.oauth2 import service_account
from googleapiclient.discovery import build
from requests import RequestException

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "'Role Trends'"
MICROSOFT_SCOPE = ["https://graph.microsoft.com/.default"]
TAB_NAME = "Role Trends Raw"  # The name of the specific tab in the sheet


def get_secrets():
    try:
        GREENHOUSE_API_KEY = os.getenv("GREENHOUSE_API_KEY")
        GREENHOUSE_BASE_URL = os.getenv("GREENHOUSE_BASE_URL")
        GREENHOUSE_API_KEY_ENCODED = base64.b64encode(
            f"{GREENHOUSE_API_KEY}:".encode("utf-8")
        ).decode("utf-8")
        SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
        OPEN_AI_KEY = os.getenv("OPEN_AI_KEY")
        USER_ID = os.getenv("USER_ID")
        service_account_base64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_BASE64")
        GOOGLE_SERVICE_ACCOUNT_JSON_DECODED = json.loads(
            base64.b64decode(service_account_base64)
        )
        print("GREENHOUSE_API_KEY_ENCODED", GREENHOUSE_API_KEY_ENCODED)
        return (
            GREENHOUSE_BASE_URL,
            GREENHOUSE_API_KEY_ENCODED,
            SPREADSHEET_ID,
            OPEN_AI_KEY,
            USER_ID,
            GOOGLE_SERVICE_ACCOUNT_JSON_DECODED,
        )
    except Exception as e:
        logging.error(f"Failed to retrieve secrets: {e}")
        raise


from dotenv import load_dotenv

mode = 'dev'
if mode == 'dev':
    load_dotenv()

(
    GREENHOUSE_BASE_URL,
    GREENHOUSE_API_KEY_ENCODED,
    SPREADSHEET_ID,
    OPEN_AI_KEY,
    USER_ID,
    GOOGLE_SERVICE_ACCOUNT_JSON_DECODED,
) = get_secrets()
print("Env Setup")


def authenticate_google_sheets():
    # Authenticate using the service account file and the defined scopes
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_SERVICE_ACCOUNT_JSON_DECODED, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=creds)
    return service


def write_to_google_sheet(service, json_strings):
    HEADERS = ["Role", "Education", "Relevant Experience", "Location", "Keywords"]
    start_row = find_first_empty_row(service)
    range_name = f"{TAB_NAME}!A{start_row}:E"
    # Prepare data to write
    rows = []
    for json_str in json_strings:
        # Parse JSON string into dictionary
        data = ast.literal_eval(json_str)
        # Create a row by mapping values according to HEADERS
        row = [
            ", ".join(data.get(key, [])) if key == "Keywords" and isinstance(data.get(key, []), list)
            else data.get(key, "")
            for key in HEADERS
        ]
        rows.append(row)
    # Prepare the data in the correct format for the Sheets API
    body = {
        "values": rows
    }
    # Use the Sheets API to append the data to the specified tab and range
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()
    print("Data written to Google Sheet successfully!")


def create_openai_client(OPEN_AI_KEY):
    openai_client = openai.OpenAI(api_key=OPEN_AI_KEY)
    return openai_client


def read_prompt_text(text_path):
    try:
        with open(text_path, "r") as file:
            return file.read()
    except FileNotFoundError:
        return "Flag as no prompt"



def batch_with_chatgpt(openai_client, merged_list):
    gpt_prompt_path = "data/gpt_prompt.txt"
    gpt_prompt = read_prompt_text(gpt_prompt_path)
    jsonl_lines = []
    for candidate in merged_list:
        prompt = {
            "custom_id": str(uuid.uuid4()),
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o",
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": gpt_prompt},
                    {"role": "user", "content": f"Candidate info: {candidate}"},
                ],
                "max_tokens": 2500,
                "n": 1,
                "stop": None,
                "temperature": 0.5,
            },
        }
        jsonl_lines.append(json.dumps(prompt).encode("utf-8"))
    json_memory_file = io.BytesIO()
    for line in jsonl_lines:
        json_memory_file.write(line + b"\n")
    json_memory_file.seek(0)
    batch_input_file = openai_client.files.create(
        file=json_memory_file, purpose="batch"
    )
    batch_input_file_id = batch_input_file.id
    batch = openai_client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "candidate data reporting generator"},
    )
    return batch


def check_gpt(openai_client, batch):
    # check status
    file_response = None
    retrieved_batch = openai_client.batches.retrieve(batch.id)
    if retrieved_batch.status == "completed" and retrieved_batch.output_file_id:
        file_response = openai_client.files.content(retrieved_batch.output_file_id)
        return file_response.content
    elif retrieved_batch.status == "completed" and retrieved_batch.error_file_id:
        file_response = openai_client.files.content(retrieved_batch.error_file_id)
        raise Exception(
            f"Batch processing failed. Error details: {file_response.content}"
        )
    elif retrieved_batch.status == "failed":
        raise Exception(f"Batch processing failed. Error details: {retrieved_batch}")
    else:
        return None


def poll_gpt_check(check):
    if isinstance(check, bytes):
        result = check
        memory_file = io.BytesIO()
        memory_file.write(result)
        memory_file.seek(0)
        memory_text = io.StringIO(memory_file.getvalue().decode("utf-8"))
        results = []
        # Read from the in-memory text stream
        for line in memory_text:
            json_object = json.loads(line.strip())
            results.append(json_object)
        return results


def validation_gpt_response(gpt_results):
    message_json = []
    for result in gpt_results:
        message = result['response']['body']['choices'][0]['message']['content']
        parsed_json_data = json.loads(message)
        message_json.append(str(parsed_json_data))
    return message_json


def find_first_empty_row(service):
    # Read all data from the sheet to find the first empty row
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{TAB_NAME}!A:A")
        .execute()
    )  # Read column A
    values = result.get("values", [])
    return len(values) + 1  # Returns the next empty row (1-based index)



def get_all_jobs():
    url = 'https://harvest.greenhouse.io/v1/jobs'
    headers = {
        "Authorization": f"Basic {GREENHOUSE_API_KEY_ENCODED}"
    }
    all_jobs = []
    per_page = 100
    page = 1
    max_retries = 5
    retry_delay = 1
    while True:
        try:
            params = {'page': page, 'per_page': per_page}
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                jobs = response.json()
                if not jobs:
                    break  # No more jobs to fetch
                all_jobs.extend(jobs)
                page += 1  # Move to the next page
                retry_delay = 1  # Reset retry delay after a successful request
            else:
                print(f"Failed to fetch jobs on page {page}: {response.status_code}")
                break
        except RequestException as e:
            print(f"RequestException on page {page}: {e}")
            if max_retries == 0:
                print("Max retries reached, aborting.")
                break
            else:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                max_retries -= 1
                print(f"Retrying... {max_retries} retries left")
    return all_jobs if all_jobs else None



def get_applications(created_after):
    url = f'https://harvest.greenhouse.io/v1/applications?created_after={created_after}'
    headers = {
        "Authorization": f"Basic {GREENHOUSE_API_KEY_ENCODED}"
    }
    filtered_applications = []
    per_page = 100
    page = 1
    max_retries = 5
    retry_delay = 1
    while True:
        try:
            params = {'page': page, 'per_page': per_page}
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                applications = response.json()
                if not applications:
                    break
                filtered_applications.extend(applications)
                page += 1  # Move to the next page
                retry_delay = 1  # Reset retry delay after a successful request
            else:
                print(f"Failed to fetch jobs on page {page}: {response.status_code}")
                break
        except RequestException as e:
            print(f"RequestException on page {page}: {e}")
            if max_retries == 0:
                print("Max retries reached, aborting.")
                break
            else:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                max_retries -= 1
                print(f"Retrying... {max_retries} retries left")
    return filtered_applications if filtered_applications else None

def merge_jobs_and_applications(all_jobs, filtered_applications):
    lookup_jobs_dict = {job['id']: job for job in all_jobs}
    merged_list = []
    for application in filtered_applications:
        job_id = application['jobs'][0]['id']
        if job_id in lookup_jobs_dict:
            job_match = {**lookup_jobs_dict[job_id], **application}
            merged_list.append(job_match)
    return merged_list


def download_resume_from_applications(filtered_applications):
    for application in filtered_applications:
        resume = next((attachment for attachment in application.get('attachments', []) if attachment['type'] == 'resume'), None)
        if resume:
            resume_url = resume['url']
            response = requests.get(resume_url)
            if response.status_code == 200:
                resume_content = io.BytesIO(response.content)
                application['resume_content'] = resume_content
    return filtered_applications


def process():
    try:
       pass # master process function to be completed
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)


#Test Steps
# created_after = '2024-11-12T00:00:00Z'
# filtered_applications = get_applications(created_after)
# filtered_applications = filtered_applications[0:100]
# resume_applications = download_resume_from_applications(filtered_applications)
# jobs = get_all_jobs()
# merged_list = merge_jobs_and_applications(jobs, filtered_applications)
# filtered_candidate_list = merged_list
#
# openai_client = create_openai_client(OPEN_AI_KEY)
# batch = batch_with_chatgpt(openai_client, filtered_candidate_list)
# check = check_gpt(openai_client, batch)
# gpt_results = poll_gpt_check(check)
#
# validated_json = validation_gpt_response(gpt_results)
#
# service = authenticate_google_sheets()
# write_to_google_sheet(service, validated_json)


# Notes
# Get applications
# Get Resumes - parse PDF to text
# Dictionary based on job title
# build GPT batch based on that
# array of returns saved to new dictionary with same keys as results
# validate (maybe do some counts before gpt)?
# write to sheets
# cache results from job id and application id - OR Date on application
# cronjob, read cache compare to jobs and application fetch
# if new add to new batch queue
# might need to get off function apps?




