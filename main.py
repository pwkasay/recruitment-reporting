import ast
import base64
import io
import json
import logging
import os
import time
import uuid
import datetime

import azure.functions as func
import olefile
import openai
import pdfplumber  # For alternative PDF parsing
import requests
from PyPDF2 import PdfReader
from docx import Document
from google.oauth2 import service_account
from googleapiclient.discovery import build
from requests import RequestException

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "'Role Trends'"
MICROSOFT_SCOPE = ["https://graph.microsoft.com/.default"]
TAB_NAME = "Role Trends Raw"


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

mode = "dev"
if mode == "dev":
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
    HEADERS = [
        "Candidate Id",
        "Company",
        "Applied Date",
        "Date Quarter",
        "Role",
        "Department",
        "Education",
        "Degree",
        "Schools",
        "Relevant Experience",
        "Location",
        "Country",
        "Source",
        "Previous Companies",
        "Previous Job Titles",
        "Keywords",
    ]
    start_row = find_first_empty_row(service)
    range_name = f"{TAB_NAME}!A{start_row}:G"
    # Prepare data to write
    rows = []
    for json_str in json_strings:
        # Parse JSON string into dictionary
        data = ast.literal_eval(json_str)
        # Create a row by mapping values according to HEADERS
        row = [
            ", ".join(data.get(key, []))
            if isinstance(data.get(key), list)
            else data.get(key, "")
            for key in HEADERS
        ]
        rows.append(row)
    # Prepare the data in the correct format for the Sheets API
    body = {"values": rows}
    # Use the Sheets API to append the data to the specified tab and range
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name,
        valueInputOption="RAW",
        body=body,
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
    success_json = []
    failed_messages = []
    for result in gpt_results:
        if result["response"]["body"]["choices"][0]["message"]["content"]:
            message = result["response"]["body"]["choices"][0]["message"]["content"]
            parsed_json_data = json.loads(message)
            success_json.append(str(parsed_json_data))
        else:
            failed_messages.append(result)
    return success_json, failed_messages


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


def parse_with_chatgpt(openai_client, candidate_data):
    gpt_prompt_path = "data/gpt_prompt.txt"
    gpt_prompt = read_prompt_text(gpt_prompt_path)
    try:
        messages = [
            {"role": "system", "content": gpt_prompt},
            {
                "role": "user",
                "content": f"Candidate Data: {candidate_data}",
            },
        ]
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            max_tokens=2500,
            n=1,
            stop=None,
            temperature=0.5,
        )
        return response.choices[0].message.content
    except Exception as e:
        print(e)
        return None


def get_all_jobs():
    url = "https://harvest.greenhouse.io/v1/jobs"
    headers = {"Authorization": f"Basic {GREENHOUSE_API_KEY_ENCODED}"}
    all_jobs = []
    per_page = 100
    page = 1
    max_retries = 5
    retry_delay = 1
    while True:
        try:
            params = {"page": page, "per_page": per_page}
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
    url = f"https://harvest.greenhouse.io/v1/applications?created_after={created_after}"
    headers = {"Authorization": f"Basic {GREENHOUSE_API_KEY_ENCODED}"}
    filtered_applications = []
    per_page = 100
    page = 1
    max_retries = 5
    retry_delay = 1
    while True:
        try:
            params = {"page": page, "per_page": per_page}
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
    lookup_jobs_dict = {job["id"]: job for job in all_jobs}
    merged_list = []
    for application in filtered_applications:
        if application["jobs"]:
            job_id = application["jobs"][0]["id"]
            if job_id in lookup_jobs_dict:
                job_match = {**lookup_jobs_dict[job_id], **application}
                merged_list.append(job_match)
    return merged_list


def extract_text_from_doc(file_bytes):
    try:
        ole = olefile.OleFileIO(io.BytesIO(file_bytes))
        if ole.exists("WordDocument"):
            stream = ole.openstream("WordDocument")
            data = stream.read()
            # Process the binary data (e.g., extract ASCII text)
            text = data.decode("utf-8", errors="ignore")
            return text
        else:
            print("No 'WordDocument' stream found in the .doc file.")
            return None
    except Exception as e:
        print(f"Error extracting text from .doc file: {e}")
        return None


def download_resume_from_applications(filtered_applications):
    failed = []
    for application in filtered_applications:
        resume = next(
            (
                attachment
                for attachment in application.get("attachments", [])
                if attachment["type"] == "resume"
            ),
            None,
        )
        if resume:
            resume_url = resume["url"]
            filename = resume["filename"]
            try:
                response = requests.get(resume_url, timeout=10)  # Added timeout
                response.raise_for_status()  # Raise error for HTTP issues
                file_bytes = response.content
                extracted_text = ""

                if filename.lower().endswith(".pdf"):
                    # Option 1: Using PyPDF2
                    try:
                        pdf_reader = PdfReader(io.BytesIO(file_bytes))
                        extracted_text = "\n".join(
                            page.extract_text() or "" for page in pdf_reader.pages
                        )
                    except Exception as e_pypdf2:
                        print(f"PyPDF2 failed for {filename}: {e_pypdf2}")
                        # Option 2: Fallback to pdfplumber
                        try:
                            with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                                extracted_text = "\n".join(
                                    page.extract_text() or "" for page in pdf.pages
                                )
                        except Exception as e_pdfplumber:
                            print(f"pdfplumber failed for {filename}: {e_pdfplumber}")
                            failed.append(application)
                            continue

                elif filename.lower().endswith(".docx"):
                    doc = Document(io.BytesIO(file_bytes))
                    extracted_text = "\n".join(
                        paragraph.text for paragraph in doc.paragraphs
                    )

                elif filename.lower().endswith(".doc"):
                    # Handle DOC files entirely in memory
                    try:
                        doc = extract_text_from_doc(file_bytes)
                        extracted_text = "\n".join(doc)
                    except Exception as e:
                        print(f"Failed to convert and process {filename}: {e}")
                        failed.append(application)
                        continue
                elif filename.lower().endswith(".txt"):
                    # Handle TXT files
                    try:
                        extracted_text = file_bytes.decode("utf-8", errors="ignore")
                    except Exception as e:
                        print(f"Failed to process .txt file {filename}: {e}")
                        failed.append(application)
                        continue

                else:
                    print(f"Unsupported file type for {filename}")
                    failed.append(application)
                    continue

                # Assign the extracted text to the application
                application["resume_content"] = extracted_text

            except requests.RequestException as e:
                print(f"Failed to download {filename}: {e}")
                failed.append(application)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                failed.append(application)
    return filtered_applications, failed


def process(created_after_date):
    try:
        jobs = get_all_jobs()

        created_after = created_after_date
        filtered_applications = get_applications(created_after)

        resume_applications, failed = download_resume_from_applications(filtered_applications)
        jobs_and_applications_list = merge_jobs_and_applications(jobs, resume_applications)

    except Exception as e:
        logging.error(f"An error occurred in the process function: {e}")
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)

    try:
        openai_client = create_openai_client(OPEN_AI_KEY)
        batch = batch_with_chatgpt(openai_client, jobs_and_applications_list)
    except Exception as e:
        logging.error(f"GPT exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)

    results = None
    validated_json = None
    try:
        while not results:
            check = check_gpt(openai_client, batch)
            if check:
                results = poll_gpt_check(check)
                validated_json, failed_messages = validation_gpt_response(results)

                print("Results returned")
            else:
                time.sleep(2)
    except Exception as e:
        logging.error(f"Poll GPT exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)

    try:
        service = authenticate_google_sheets()
        write_to_google_sheet(service, validated_json)
    except Exception as e:
        logging.error(f"Google Sheets exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)

# Test Steps
# jobs = get_all_jobs()
# created_after = "2023-12-31T00:00:00Z"
# filtered_applications = get_applications(created_after)
#
# resume_applications, failed = download_resume_from_applications(filtered_applications)
# merged_list = merge_jobs_and_applications(jobs, resume_applications)
# filtered_candidate_list = merged_list
#
# fl1 = filtered_candidate_list[0:11000]
# fl2 = filtered_candidate_list[11000:22000]
# fl3 = filtered_candidate_list[22000:]
# openai_client = create_openai_client(OPEN_AI_KEY)
# batch1 = batch_with_chatgpt(openai_client, fl1)
# batch2 = batch_with_chatgpt(openai_client, fl2)
# batch3 = batch_with_chatgpt(openai_client, fl3)
#
# check = check_gpt(openai_client, batch3)
# gpt_results = poll_gpt_check(check)
# validated_json, failed_messages = validation_gpt_response(gpt_results)
#
# json_data = []
# json_data.append(validated_json)
#
# from itertools import chain
# flattened = list(chain.from_iterable(json_data))
# validated_json = flattened
#
# service = authenticate_google_sheets()
# write_to_google_sheet(service, flattened)
#
# counts = count_keywords_from_sheet(service)
# write_keywords(service, counts)
#
# spot_check_candidate(candidate_id)
#
# candidate_id = 368916873
# merged_lookup = {item["candidate_id"]: item for item in merged_list}
# validated_dicts = [ast.literal_eval(item) for item in validated_json]
# validated_lookup = {item["Candidate Id"]: item for item in validated_dicts}
# initial_data = merged_lookup.get(candidate_id)
# processed_data = validated_lookup.get(candidate_id)
# print(f"--- Spot Check for Candidate ID: {candidate_id} ---\n")
# comparisons = [
#     ("Company", initial_data.get("name"), processed_data.get("Company")),
#     (
#         "Applied Date",
#         initial_data.get("applied_at", "N/A")[:10],
#         processed_data.get("Applied Date"),
#     ),
# ]
# for label, initial_value, processed_value in comparisons:
#     print(f"{label}:")
#     print(f"  Initial Data: {initial_value}")
#     print(f"  Processed Data: {processed_value}\n")
# initial_resume = initial_data.get("resume_content", [])
# processed_companies = processed_data.get("Previous Companies", [])
# print("Previous Companies:")
# print(f"  Initial Resume: {initial_resume}")
# print(f"  Processed Data: {processed_companies}\n")
