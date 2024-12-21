import ast
import base64
import io
import json
import logging
import os
import time
import uuid
import datetime
import asyncio

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
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_SERVICE_ACCOUNT_JSON_DECODED, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=creds)
    return service


def write_to_google_sheet(service, flattened_rows):
    HEADERS = [
        "Candidate Id",
        "Candidate Name",
        "Company",
        "Applied Date",
        "Date Quarter",
        "Role",
        "Department",
        "Education",
        "Degree",
        "Schools",
        "Relevant Experience",
        "City",
        "State/Province",
        "Country",
        "Source",
        "Previous Companies",
        "Previous Job Titles",
        "Resume Link"
    ]
    start_row = find_first_empty_row(service)
    range_name = f"{TAB_NAME}!A{start_row}:S"  # Adjust range as needed
    rows = []
    for row_data in flattened_rows:
        row = [row_data.get(header, "") for header in HEADERS]
        rows.append(row)
    body = {"values": rows}
    # Append to Google Sheets
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name,
        valueInputOption="RAW",
        body=body,
    ).execute()
    print("Data written to Google Sheet successfully!")


async def create_openai_client(OPEN_AI_KEY):
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
                "model": "gpt-4o-mini",
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


def validation_gpt_response(results):
    success_json = []
    failed_json = []
    for result in results:
        start_index = result.find("{")
        end_index = result.rfind("}") + 1
        json_string = result[start_index:end_index]
        if json.loads(json_string):
            success_json.append(json.loads(json_string))
        else:
            failed_json.append(json_string)
        for i in success_json:
            for key, value in i.items():
                if key == "Role" or key == "Company":
                    i[key] = value.strip()
    return success_json, failed_json


def validation_batch_response(gpt_results):
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
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{TAB_NAME}!A:A")
        .execute()
    )  # Read column A
    values = result.get("values", [])
    return len(values) + 1  # Returns the next empty row (1-based index)


async def parse_with_chatgpt(openai_client, candidate_data):
    gpt_prompt_path = "data/gpt_prompt.txt"
    gpt_prompt = read_prompt_text(gpt_prompt_path)
    def _call_openai():
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
    return await asyncio.to_thread(_call_openai)


async def get_all_jobs():
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


async def get_applications(created_after):
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


async def merge_jobs_and_applications(all_jobs, filtered_applications):
    lookup_jobs_dict = {job["id"]: job for job in all_jobs}
    for job_id, job_data in lookup_jobs_dict.items():
        job_data['job_name'] = job_data['name']
    merged_list = []
    for application in filtered_applications:
        if application["jobs"]:
            job_id = application["jobs"][0]["id"]
            if job_id in lookup_jobs_dict:
                job_match = {**lookup_jobs_dict[job_id], **application}
                merged_list.append(job_match)
    return merged_list



async def download_resume_from_applications(filtered_applications):
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
                    unprocessed_docs.append(application)
                    continue
                    # try:
                    #     docx_bytes = await convert_doc_to_docx(file_bytes)
                    #     doc = Document(io.BytesIO(docx_bytes))
                    #     extracted_text = "\n".join(
                    #         paragraph.text or "" for paragraph in doc.paragraphs
                    #     )
                    # except Exception as e:
                    #     print(f"Failed to convert {filename}: {e}")
                    #     failed.append(application)
                    #     continue

                        # need to use convert_doc_to_docx instead of:
                    # Handle DOC files entirely in memory
                    # try:
                    #     doc = extract_text_from_doc(file_bytes)
                    #     extracted_text = "\n".join(doc)
                    # except Exception as e:
                    #     print(f"Failed to convert and process {filename}: {e}")
                    #     failed.append(application)

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

                application["resume_content"] = extracted_text

            except requests.RequestException as e:
                print(f"Failed to download {filename}: {e}")
                failed.append(application)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                failed.append(application)
    return filtered_applications, failed


async def extract_text_from_doc(file_bytes):
    def _extract():
        try:
            ole = olefile.OleFileIO(io.BytesIO(file_bytes))
            if ole.exists("WordDocument"):
                stream = ole.openstream("WordDocument")
                data = stream.read()
                text = data.decode("utf-8", errors="ignore")
                return text
            else:
                print("No 'WordDocument' stream found in the .doc file.")
                return None
        except Exception as e:
            print(f"Error extracting text from .doc file: {e}")
            return None

    return await asyncio.to_thread(_extract)


async def process(created_after_date):
    try:
        jobs = await get_all_jobs()
        created_after = created_after_date
        filtered_applications = await get_applications(created_after)
        resume_applications, failed = await download_resume_from_applications(filtered_applications)
        jobs_and_applications_list = await merge_jobs_and_applications(jobs, resume_applications)
    except Exception as e:
        logging.error(f"An error occurred in the process function - greenhouse: {e}")
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)

    try:
        openai_client = await create_openai_client(OPEN_AI_KEY)
        results = await asyncio.gather(
            *(parse_with_chatgpt(openai_client, candidate_data) for candidate_data in jobs_and_applications_list)
        )
    except Exception as e:
        logging.error(f"An error occurred in the process function - gpt: {e}")
    try:
        validated_json, failed_messages = validation_gpt_response(results)
    except Exception as e:
        logging.error(f"An error occurred in the process function - validation: {e}")
        return func.HttpResponse(str(e), status_code=500)
    try:
        flattened_rows = normalize_candidates(validated_json)
    except Exception as e:
        logging.error(f"An error occurred in the process function - normalization: {e}")
        return func.HttpResponse(str(e), status_code=500)
    try:
        service = authenticate_google_sheets()
        write_to_google_sheet(service, flattened_rows)
        return func.HttpResponse("Processed to sheet successfully", status_code=200)
    except Exception as e:
        logging.error(f"Google Sheets exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)


def parse_candidate(item):
    # Convert from string to dict if necessary
    if isinstance(item, str):
        return ast.literal_eval(item)
    return item

def is_list_field(value):
    return isinstance(value, list)

def expand_candidate(candidate):
    """
    Given a candidate dictionary, produce multiple rows (dictionaries)
    if they have multiple values in any of their array fields.
    """
    # Identify which fields have lists
    list_fields = [k for k, v in candidate.items() if is_list_field(v)]
    # If no lists, just return the candidate as is
    if not list_fields:
        return [candidate]
    # Determine the number of rows needed = length of the longest list
    max_length = 1
    for field in list_fields:
        field_length = len(candidate[field]) if candidate[field] else 0
        if field_length > max_length:
            max_length = field_length
    rows = []
    for i in range(max_length):
        row = {}
        for k, v in candidate.items():
            if k in list_fields:
                # If this field has enough elements, use the i-th element
                # else, use an empty string
                if i < len(v):
                    row[k] = v[i]
                else:
                    row[k] = ""
            else:
                # Non-list fields remain the same for all rows
                row[k] = v
        rows.append(row)
    return rows

def normalize_candidates(candidate_data):
    """
    Given a list of candidate records (as dictionaries or dict-string),
    return a single list of rows with each candidate potentially expanded
    into multiple rows.
    """
    candidates = [parse_candidate(item) for item in candidate_data]
    all_rows = []
    for c in candidates:
        expanded = expand_candidate(c)
        all_rows.extend(expanded)
    return all_rows


# Batch for history
# Single for daily

# Notes
# @ validator function to run in azure
# Failed category

# Test Steps
# jobs = asyncio.run(get_all_jobs())
# created_after = "2024-12-01T00:00:00Z"
# filtered_applications = asyncio.run(get_applications(created_after))
#
# resume_applications, failed = asyncio.run(download_resume_from_applications(filtered_applications))
# merged_list = asyncio.run(merge_jobs_and_applications(jobs, resume_applications))
# filtered_candidate_list = merged_list
#
#
# openai_client = asyncio.run(create_openai_client(OPEN_AI_KEY))
#
# async def main():
#     results = await asyncio.gather(
#         *(parse_with_chatgpt(openai_client, candidate_data) for candidate_data in filtered_candidate_list)
#     )
#     # Print results or process them as needed
#     return results
#
# # Run the async function
# results = asyncio.run(main())
#
#
# pprint(filtered_applications[2340])
#
# job = None
# for j in jobs:
#     if 'Analytical Lab Technician' in j['name']:
#         job = j
#
# merged_items = []
# for m in merged_list:
#     if 'Analytical Lab Technician' in m['name']:
#         merged_items.append(m)
#
#
#
# openai_client = asyncio.run(create_openai_client(OPEN_AI_KEY))
# results = asyncio.gather(
#     *(parse_with_chatgpt(openai_client, candidate_data) for candidate_data in filtered_candidate_list)
# )
#
# results = []
# for candidate_data in merged_items:
#     result = parse_with_chatgpt(openai_client, candidate_data)
#     results.append(result)
#
#
#
# # Restructure the json to call out company explicitly
# for fc in filtered_candidate_list:
#     offices = fc.get('offices')
#     for office in offices:
#         fc['hiring_company_name'] = office['name']
#
# fl1 = filtered_candidate_list[0:10]
# fl2 = filtered_candidate_list[11000:22000]
# fl3 = filtered_candidate_list[22000:]
#
# fl0 = filtered_candidate_list[0:1]
#
# openai_client = create_openai_client(OPEN_AI_KEY)
# batch = batch_with_chatgpt(openai_client, fl1)
#
# batch2 = batch_with_chatgpt(openai_client, fl2)
# batch3 = batch_with_chatgpt(openai_client, fl3)
#
# results = None
# validated_json = None
# while not results:
#     check = check_gpt(openai_client, batch)
#     if check:
#         results = poll_gpt_check(check)
#         validated_json, failed_messages = validation_gpt_response(results)
#         print("Results returned")
#     else:
#         time.sleep(2)
#
# check = check_gpt(openai_client, batch)
# gpt_results = poll_gpt_check(check)
#
#
# validated_json, failed_messages = validation_gpt_response(gpt_results)
#
# flattened_rows = normalize_candidates(validated_json)
#
# service = authenticate_google_sheets()
# write_to_google_sheet(service, flattened_rows)
