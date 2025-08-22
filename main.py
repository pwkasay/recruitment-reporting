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
import openai
import pdfplumber
import requests
from pypdf import PdfReader
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
        "Resume Link",
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


def create_openai_client_batch(OPEN_AI_KEY):
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
            print(message)
            try:
                parsed_json_data = json.loads(message)
                success_json.append(str(parsed_json_data))
            except Exception as e:
                failed_messages.append(message)
                print(e)
                continue
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


async def get_applications(created_after, created_before):
    url = f"https://harvest.greenhouse.io/v1/applications?created_after={created_after}&created_before={created_before}"
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
        job_data["job_name"] = job_data["name"]
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
                    try:
                        pdf_reader = PdfReader(io.BytesIO(file_bytes))
                        extracted_text = "\n".join(
                            page.extract_text() or "" for page in pdf_reader.pages
                        )
                    except Exception as e_pypdf2:
                        print(f"PyPDF2 failed for {filename}: {e_pypdf2}")
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


# Todo: Keep thinking about the degree overfitting
async def process(created_after_date, created_before_date):
    try:
        jobs = await get_all_jobs()
        created_after = created_after_date
        created_before = created_before_date
        filtered_applications = await get_applications(created_after, created_before)
        resume_applications, failed = await download_resume_from_applications(
            filtered_applications
        )
        jobs_and_applications_list = await merge_jobs_and_applications(
            jobs, resume_applications
        )
    except Exception as e:
        logging.error(f"An error occurred in the process function - greenhouse: {e}")
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)

    try:
        openai_client = await create_openai_client(OPEN_AI_KEY)
        results = await asyncio.gather(
            *(
                parse_with_chatgpt(openai_client, candidate_data)
                for candidate_data in jobs_and_applications_list
            )
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


# import subprocess
# import requests
# import os
# from pypdf import PdfReader
# import time

# Need to manage a Libre Office Listener?
# def process_doc_applications(unprocessed_applications, download_retries=3, base_sleep=2):
#     """
#     Processes applications by downloading their first attachment (.doc),
#     converting it to PDF, extracting text from the PDF, and storing it
#     in the application['resume_content'] field.
#
#     :param unprocessed_applications: list of dicts with 'attachments' info
#     :param download_retries: number of times to retry downloading the file
#     :param base_sleep: initial wait time in seconds before retrying
#     :return: a tuple of (success_applications, failed_applications)
#     """
#
#     success_applications = []
#     failed_applications = []
#
#     for idx, application in enumerate(unprocessed_applications, start=1):
#         print(f"\nProcessing application {idx} of {len(unprocessed_applications)}...")
#
#         # Validate that attachments exist
#         if not application.get('attachments'):
#             print("No attachments found. Skipping this application.")
#             failed_applications.append(application)
#             continue
#
#         # For simplicity, assume the first attachment is always the doc
#         attachment = application['attachments'][0]
#         filename = attachment['filename']
#         url = attachment['url']
#         file_path = f"process/{filename}"
#
#         # Retry logic for downloading the file
#         success = False
#         attempt = 0
#
#         while attempt < download_retries and not success:
#             attempt += 1
#             try:
#                 print(f"Downloading '{filename}' from '{url}' (Attempt {attempt})...")
#                 response = requests.get(url, timeout=10)
#                 response.raise_for_status()  # Raise an HTTPError if 4xx/5xx
#                 file_bytes = response.content
#
#                 # Save the file to disk
#                 with open(file_path, 'wb') as f:
#                     f.write(file_bytes)
#
#                 print(f"File '{filename}' downloaded successfully.")
#                 success = True  # Download succeeded
#
#             except requests.exceptions.RequestException as e:
#                 # If a network or timeout error happened
#                 wait_time = base_sleep ** attempt  # simple exponential backoff
#                 print(f"Error downloading '{filename}': {e}")
#                 if attempt < download_retries:
#                     print(f"Retrying in {wait_time} seconds...")
#                     time.sleep(wait_time)
#                 else:
#                     print(f"Exceeded maximum retries ({download_retries}) for '{filename}'. Skipping this file.")
#                     # Mark this application as failed and skip it
#                     failed_applications.append(application)
#                     break
#
#         # If download was never successful, skip to the next application
#         if not success:
#             continue
#
#         # Convert the file to PDF via unoconv
#         try:
#             print(f"Converting '{file_path}' to PDF...")
#             output = subprocess.run(
#                 ['unoconv', '-f', 'pdf', file_path],
#                 check=True,
#                 stdout=subprocess.PIPE,
#                 stderr=subprocess.PIPE
#             )
#             print(f"Conversion of '{file_path}' to PDF completed.")
#         except subprocess.CalledProcessError as e:
#             # If unoconv fails, show the error and mark as failed
#             print(f"Error converting '{file_path}' to PDF.")
#             print(f"stdout: {e.stdout}")
#             print(f"stderr: {e.stderr}")
#             if os.path.exists(file_path):
#                 os.remove(file_path)
#             failed_applications.append(application)
#             continue
#
#         # Remove the original doc file to keep things tidy
#         if os.path.exists(file_path):
#             os.remove(file_path)
#             print(f"Removed the original file '{file_path}'.")
#
#         # Locate the new PDF in the "process" folder
#         pdf_filename = os.path.splitext(filename)[0] + ".pdf"
#         pdf_path = os.path.join("process", pdf_filename)
#
#         if not os.path.exists(pdf_path):
#             print(f"No PDF file found at {pdf_path}. Skipping text extraction.")
#             failed_applications.append(application)
#             continue
#
#         # Extract text from the PDF
#         try:
#             print(f"Extracting text from '{pdf_path}'...")
#             pdf_reader = PdfReader(pdf_path)
#             extracted_text = "\n".join(page.extract_text() or "" for page in pdf_reader.pages)
#             application["resume_content"] = extracted_text
#             print(f"Text extraction complete. Extracted {len(extracted_text)} characters.")
#         except Exception as e:
#             print(f"Error reading PDF '{pdf_path}': {e}")
#             application["resume_content"] = ""
#             failed_applications.append(application)
#             continue
#
#         # If everything worked, add to successful applications
#         success_applications.append(application)
#
#         # Optionally, remove the PDF if you don't need to keep it
#         # os.remove(pdf_path)
#
#     print("\nProcessing complete.")
#     print(f"Successfully processed: {len(success_applications)} application(s).")
#     print(f"Failed to process: {len(failed_applications)} application(s).")
#
#     # Return them so that the calling code can handle them further
#     return success_applications, failed_applications


# import pickle
# file_path = 'unprocessed_applications.pickle'
# with open(file_path, 'wb') as file:
#     pickle.dump(doc_files_process_later, file)
# with open(file_path, 'rb') as file:
#     unprocessed_applications = pickle.load(file)


# Test Steps
# jobs = asyncio.run(get_all_jobs())
# created_after = "2025-01-15T11:59:00Z"
# created_before = "2025-01-19T00:00:00Z"
# filtered_applications = asyncio.run(get_applications(created_after, created_before))
#
# doc_files_process_later = []
# applications_process_now = []
#
# for app in filtered_applications:
#     if app["attachments"]:
#         if app["attachments"][0]["filename"]:
#             if (
#                 ".doc" in app["attachments"][0]["filename"]
#                 and ".docx" not in app["attachments"][0]["filename"]
#             ):
#                 doc_files_process_later.append(app)
#             else:
#                 applications_process_now.append(app)
#
# a = []
# for app in filtered_applications:
#     if app["id"] == 362908158:
#         a.append(app)
#
#
# resume_applications, failed = asyncio.run(
#     download_resume_from_applications(applications_process_now)
# )
#
# merged_list = asyncio.run(merge_jobs_and_applications(jobs, resume_applications))
# filtered_candidate_list = merged_list


# openai_client = asyncio.run(create_openai_client(OPEN_AI_KEY))
# gpt_results = asyncio.gather(
#     *(parse_with_chatgpt(openai_client, candidate_data) for candidate_data in filtered_candidate_list)
# )

# # Restructure the json to call out company explicitly
# for fc in filtered_candidate_list:
#     offices = fc.get('offices')
#     for office in offices:
#         fc['hiring_company_name'] = office['name']

# fl1 = filtered_candidate_list[0:11000]
# fl2 = filtered_candidate_list[11000:22000]
# fl3 = filtered_candidate_list[22000:]
#
# fl0 = filtered_candidate_list[0:1]
#
# openai_client = create_openai_client_batch(OPEN_AI_KEY)
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
# check2 = check_gpt(openai_client, batch2)
# gpt_results2 = poll_gpt_check(check2)
# check3 = check_gpt(openai_client, batch3)
# gpt_results3 = poll_gpt_check(check3)
#
# validated_json, failed_messages = validation_batch_response(gpt_results)
#
# validated_json2, failed_messages2 = validation_batch_response(gpt_results2)
# validated_json3, failed_messages3 = validation_batch_response(gpt_results3)
#
# flattened_rows = normalize_candidates(validated_json)
#
# flattened_rows2 = normalize_candidates(validated_json2)
# flattened_rows3 = normalize_candidates(validated_json3)
#
# service = authenticate_google_sheets()
#
# write_to_google_sheet(service, flattened_rows)
#
# write_to_google_sheet(service, flattened_rows2)
# write_to_google_sheet(service, flattened_rows3)
#
#
# validated_json, failed_messages = validation_gpt_response(gpt_results)
# flattened_rows = normalize_candidates(validated_json)
# service = authenticate_google_sheets()
# write_to_google_sheet(service, flattened_rows)
