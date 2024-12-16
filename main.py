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
        "Keywords",
        "Resume Link"
    ]
    start_row = find_first_empty_row(service)
    range_name = f"{TAB_NAME}!A{start_row}:S"
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
            # degree = parsed_json_data['Degree']
            # standardized_degree_dict = standardize_degree(degree)
            # degree_str = standardized_degree_dict["fields"]
            # parsed_json_data["Degree Category"] = degree_str
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
        return func.HttpResponse("Processed to sheet successfully", status_code=200)
    except Exception as e:
        logging.error(f"Google Sheets exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)


# 


# import re
# from difflib import get_close_matches
#
# import re
# import json
# from rapidfuzz import fuzz, process
#
# # Degree level patterns:
# degree_patterns = [
#     (r"\bBachelor(?:'s|s)? of (?:Arts|Science)\b", "Bachelor's"),  # Matches "Bachelor of Arts", "Bachelors of Science"
#     (r"\bB\.?A\.?\b", "Bachelor's"),
#     (r"\bB\.?S\.?\b", "Bachelor's"),
#     (r"\bB\.Sc\.?\b", "Bachelor's"),
#     (r"\bBachelor(?:'s|s)?\b", "Bachelor's"),  # Matches "Bachelor's" and "Bachelors"
#     (r"\bBachelor(?:'s|s)? of [A-Za-z ]+\b", "Bachelor's"),  # Matches specific Bachelor degrees like Biology, Mechanical Engineering
#     (r"\bM\.?A\.?\b", "Master's"),
#     (r"\bM\.?S\.?\b", "Master's"),
#     (r"\bM\.Sc\.?\b", "Master's"),
#     (r"\bMaster(?:'s|s)? of [A-Za-z ]+\b", "Master's"),  # Matches "Master of Science in Business Analytics"
#     (r"\bMBA\b", "Master's"),
#     (r"\bMaster(?:'s|s)?\b", "Master's"),  # Matches "Master's" and "Masters"
#     (r"\bPh\.?D\.?\b", "Ph.D."),
#     (r"\bDoctor of Philosophy\b", "Ph.D."),
#     (r"\bPh\.D\.? [A-Za-z ]+\b", "Ph.D."),  # Matches "Ph.D. Chemistry"
#     (r"\bDiploma\b|\bCertificate\b|\bAdvanced Diploma\b", "Diploma/Certificate"),
#     (r"\bAssociate\b", "Associate's"),
#     (r"\bHigh school\b|\bGED\b", "High School"),
#     (r"\bM\.S\.\s?\u2013\s?[A-Za-z ]+\b", "Master's"),  # Matches M.S. – Paper Science and Engineering
#     (r"\bB\.S\.\s?[A-Za-z ]+\b", "Bachelor's"),  # Matches B.S. Mechanical Engineering
#     (r"\bMSc in [A-Za-z ]+\b", "Master's"),  # Matches MSc in Electrical and Computer Engineering
#     (r"\bB\.?A\.? in [A-Za-z ]+\b", "Bachelor's"),  # Matches B.A. in English, Concentration in Writing
#     (r"\bB\.Sc\. in [A-Za-z ]+\b", "Bachelor's"),  # Matches B.Sc. in Chemical Biology
#     (r"\bPhD and MBA\b", "Ph.D. and Master's"),
#     (r"\bFinance\b", "Bachelor's"),  # Matches Finance specifically
#     (r"\bOrganizational Leadership\b", "Master's")  # Matches Organizational Leadership
# ]
# field_map = {
#     "Business Administration": ["business administration", "business admin", "bba"],
#     "Mechanical Engineering": ["mechanical engineering", "mech eng", "m.s. – paper science and engineering"],
#     "Electrical Engineering": ["electrical engineering", "elec eng", "ee"],
#     "Civil Engineering": ["civil engineering", "civil eng"],
#     "Chemical Engineering": ["chemical engineering", "chem eng"],
#     "Software Engineering": ["software engineering", "soft eng"],
#     "Aerospace Engineering": ["aerospace engineering", "aero eng"],
#     "Computer Engineering": ["computer engineering", "comp eng"],
#     "Biomedical Engineering": ["biomedical engineering", "bme"],
#     "Information Technology": ["information technology", "it", "info tech"],
#     "Computer Science": ["computer science", "comp sci", "cs"],
#     "Data Science": ["data science", "master of science data science", "m.s. data science"],
#     "Artificial Intelligence": ["artificial intelligence", "ai"],
#     "Machine Learning": ["machine learning", "ml"],
#     "Business Management": ["business management", "management"],
#     "Business Analytics": ["business analytics", "master of science business analytics"],
#     "Marketing": ["marketing", "business administration marketing"],
#     "Finance": ["business administration finance", "finance"],
#     "Economics": ["economics"],
#     "Accounting": ["accounting"],
#     "Human Resources": ["human resources", "hr"],
#     "Organizational Leadership": ["organizational leadership", "ma organizational leadership"],
#     "Hospitality Management": ["hospitality management", "hotel management"],
#     "English": ["english", "b.a. in english concentration in writing"],
#     "Literature": ["literature"],
#     "Psychology": ["psychology", "b.s. psychology"],
#     "Sociology": ["sociology"],
#     "Political Science": ["political science", "polisci"],
#     "Biology": ["biology", "bachelor of science biology", "b.s. biology", "b.sc. in chemical biology"],
#     "Chemistry": ["chemistry", "ph.d. chemistry"],
#     "Physics": ["physics"],
#     "Mathematics": ["mathematics", "math"],
#     "Statistics": ["statistics"],
#     "Environmental Science": ["environmental science", "env sci"],
#     "Geography": ["geography"],
#     "Environment, Sustainability & Geography": ["environment", "sustainability", "geography"],
#     "Philosophy": ["philosophy"],
#     "History": ["history"],
#     "Anthropology": ["anthropology"],
#     "Art History": ["art history"],
#     "Education": ["education", "teaching"],
#     "Nursing": ["nursing", "rn", "bachelor of nursing"],
#     "Public Health": ["public health", "mph"],
#     "Pharmacy": ["pharmacy"],
#     "Law": ["law", "jd", "juris doctor"],
#     "Medicine": ["medicine", "md", "doctor of medicine"],
#     "Dentistry": ["dentistry", "dds", "doctor of dental surgery"],
#     "Veterinary Medicine": ["veterinary medicine", "dvm"],
#     "Social Work": ["social work", "msw"],
#     "Fine Arts": ["fine arts", "bfa", "mfa"],
#     "Music": ["music", "ba music", "bachelor of music"],
#     "Theatre": ["theatre", "drama", "performing arts"],
#     "Film Studies": ["film studies", "cinema studies"],
#     "Graphic Design": ["graphic design"],
#     "Architecture": ["architecture", "b.arch", "m.arch"],
#     "Urban Planning": ["urban planning"],
#     "Journalism": ["journalism", "mass communication"],
#     "Communications": ["communications", "comm"],
#     "Linguistics": ["linguistics"]
# }

# field_map = {
#     # Business / Management Fields
#     "Business Administration": [
#         "business administration", "business admin", "bba", "business studies",
#         "bsba", "business and administration"
#     ],
#     "Business Management": [
#         "business management", "management", "business leadership", "mngmt"
#     ],
#     "Business Analytics": [
#         "business analytics", "master of science business analytics", "msba", "mba analytics"
#     ],
#     "Marketing": [
#         "marketing", "business administration marketing", "marketing management"
#     ],
#     "Finance": [
#         "finance", "business administration finance", "financial studies", "financial management"
#     ],
#     "Economics": [
#         "economics", "economic sciences", "economic studies"
#     ],
#     "Accounting": [
#         "accounting", "accountancy", "cpa track", "accounting and finance"
#     ],
#     "Human Resources": [
#         "human resources", "hr", "human resource management", "hrm"
#     ],
#     "Organizational Leadership": [
#         "organizational leadership", "ma organizational leadership", "leadership studies"
#     ],
#     "Hospitality Management": [
#         "hospitality management", "hotel management", "hospitality and tourism",
#         "hospitality and culinary management"
#     ],
#
#     # STEM Fields
#     "Mechanical Engineering": [
#         "mechanical engineering", "mech eng", "mechanical eng", "mecheng"
#     ],
#     "Electrical Engineering": [
#         "electrical engineering", "elec eng", "ee", "electrical and computer engineering"
#     ],
#     "Civil Engineering": [
#         "civil engineering", "civil eng"
#     ],
#     "Chemical Engineering": [
#         "chemical engineering", "chem eng", "cheme"
#     ],
#     "Software Engineering": [
#         "software engineering", "soft eng", "software development"
#     ],
#     "Aerospace Engineering": [
#         "aerospace engineering", "aero eng"
#     ],
#     "Computer Engineering": [
#         "computer engineering", "comp eng"
#     ],
#     "Biomedical Engineering": [
#         "biomedical engineering", "bme", "bioengineering", "biomedical eng"
#     ],
#     "Information Technology": [
#         "information technology", "it", "info tech", "information systems", "information sciences"
#     ],
#     "Computer Science": [
#         "computer science", "comp sci", "cs", "computing", "informatics"
#     ],
#     "Data Science": [
#         "data science", "master of science data science", "m.s. data science", "datasci"
#     ],
#     "Artificial Intelligence": [
#         "artificial intelligence", "ai", "intelligent systems"
#     ],
#     "Machine Learning": [
#         "machine learning", "ml"
#     ],
#     "Cybersecurity": [
#         "cybersecurity", "cyber security", "information security", "infosec"
#     ],
#
#     # Sciences
#     "Biology": [
#         "biology", "biological sciences", "bio", "b.s. biology", "b.sc. biology",
#         "bachelor of science biology", "chemical biology"
#     ],
#     "Chemistry": [
#         "chemistry", "ph.d. chemistry", "chem", "chemical sciences"
#     ],
#     "Physics": [
#         "physics", "physical sciences", "astro physics"
#     ],
#     "Mathematics": [
#         "mathematics", "math", "mathematical sciences"
#     ],
#     "Statistics": [
#         "statistics", "statistical sciences", "stats"
#     ],
#     "Environmental Science": [
#         "environmental science", "env sci", "environmental studies", "environment and sustainability"
#     ],
#     "Geography": [
#         "geography", "geographical sciences"
#     ],
#     "Environment, Sustainability & Geography": [
#         "environment", "sustainability", "geography", "environmental sustainability"
#     ],
#
#     # Social Sciences / Humanities
#     "English": [
#         "english", "b.a. in english", "english literature", "english language studies"
#     ],
#     "Literature": [
#         "literature", "comparative literature", "literary studies"
#     ],
#     "Psychology": [
#         "psychology", "psych", "b.s. psychology"
#     ],
#     "Sociology": [
#         "sociology", "socio"
#     ],
#     "Political Science": [
#         "political science", "polisci", "politics", "public policy", "policy and program evaluation"
#     ],
#     "Philosophy": [
#         "philosophy", "philosophical studies"
#     ],
#     "History": [
#         "history", "historical studies"
#     ],
#     "Anthropology": [
#         "anthropology", "anthro"
#     ],
#     "Art History": [
#         "art history", "arthist"
#     ],
#
#     # Education / Health / Law
#     "Education": [
#         "education", "teaching", "educational leadership"
#     ],
#     "Nursing": [
#         "nursing", "rn", "bachelor of nursing", "bsn", "nurse"
#     ],
#     "Public Health": [
#         "public health", "mph", "community health"
#     ],
#     "Pharmacy": [
#         "pharmacy", "pharm", "pharmaceutical sciences"
#     ],
#     "Law": [
#         "law", "jd", "juris doctor", "legal studies"
#     ],
#     "Medicine": [
#         "medicine", "md", "doctor of medicine", "medical sciences"
#     ],
#     "Dentistry": [
#         "dentistry", "dds", "doctor of dental surgery"
#     ],
#     "Veterinary Medicine": [
#         "veterinary medicine", "dvm", "vet med"
#     ],
#     "Social Work": [
#         "social work", "msw", "social welfare"
#     ],
#
#     # Arts & Media
#     "Fine Arts": [
#         "fine arts", "bfa", "mfa", "visual arts"
#     ],
#     "Music": [
#         "music", "ba music", "bachelor of music", "musical studies"
#     ],
#     "Theatre": [
#         "theatre", "drama", "performing arts", "theater"
#     ],
#     "Film Studies": [
#         "film studies", "cinema studies", "film and media"
#     ],
#     "Graphic Design": [
#         "graphic design", "visual design", "digital design"
#     ],
#     "Architecture": [
#         "architecture", "b.arch", "m.arch", "architectural studies"
#     ],
#     "Urban Planning": [
#         "urban planning", "city planning", "urban and regional planning"
#     ],
#     "Journalism": [
#         "journalism", "mass communication", "media studies", "press and media"
#     ],
#     "Communications": [
#         "communications", "comm", "communication studies", "strategic communication"
#     ],
#     "Linguistics": [
#         "linguistics", "linguistic studies", "language science"
#     ]
# }
#
# # Flatten the field_map into a lookup dict:
# field_synonyms = {}
# for canonical, synonyms in field_map.items():
#     for s in synonyms:
#         field_synonyms[s.lower()] = canonical
#
# all_field_candidates = list(field_synonyms.keys())
#
# def normalize_field_string(field_str):
#     """Normalize the field string by removing common filler words, punctuation, etc."""
#     field_str = field_str.lower()
#     # Remove punctuation
#     field_str = re.sub(r"[.,;:!?\(\)\[\]\-–]+", " ", field_str)
#     # Remove common stopwords that are not part of fields
#     # Add or remove words as needed
#     stopwords = ["of", "in", "and", "with", "for", "on", "by", "to",
#                  "emphasis", "concentration", "specialization", "track", "program"]
#     pattern = r"\b(" + "|".join(stopwords) + r")\b"
#     field_str = re.sub(pattern, "", field_str)
#     # Remove extra spaces
#     field_str = re.sub(r"\s+", " ", field_str).strip()
#     return field_str
#
#
# def find_best_field_match(field_str, threshold=80):
#     """
#     Return the best field match using fuzzy scoring, or None if below threshold.
#     We use rapidfuzz's process.extractOne with a token_set_ratio for more flexible matching.
#     """
#     if field_str in field_synonyms:
#         return field_synonyms[field_str]
#     # Use fuzzy match against all_field_candidates
#     match = process.extractOne(field_str, all_field_candidates, scorer=fuzz.token_set_ratio)
#     if match is not None:
#         candidate, score = match
#         if score >= threshold:
#             return field_synonyms[candidate]
#     return None


# def find_best_field_match(field_str, fields, threshold=0.7):
#     """Return the best field match using fuzzy matching, or None if below threshold."""
#     field_str = field_str.lower().strip()
#     if field_str in field_synonyms:
#         return field_synonyms[field_str]
#     field_names = list(field_map.keys())
#     matches = get_close_matches(field_str, field_names, n=1, cutoff=0)
#     if matches:
#         from difflib import SequenceMatcher
#         best = matches[0]
#         ratio = SequenceMatcher(None, field_str, best.lower()).ratio()
#         if ratio >= threshold:
#             return best
#     return None

# def standardize_degree(degree_str):
#     """Extract and standardize the degree level and field(s) from a raw degree string."""
#     degree_str = degree_str.strip()
#     degree_level = None
#     for pattern, level in degree_patterns:
#         if re.search(pattern, degree_str, flags=re.IGNORECASE):
#             degree_level = level
#             degree_str = re.sub(pattern, "", degree_str, flags=re.IGNORECASE).strip()
#             break
#     if not degree_level:
#         degree_level = "Other"
#     degree_str = re.sub(r"\bemphasis in\b|\bemphasis\b|\bconcentration in\b", "", degree_str, flags=re.IGNORECASE)
#     degree_str = re.sub(r"\bin\b", "", degree_str, flags=re.IGNORECASE).strip()
#     possible_fields = re.split(r"\band\b", degree_str, flags=re.IGNORECASE)
#     possible_fields = [f.strip(",;: ").strip() for f in possible_fields if f.strip()]
#     matched_fields = []
#     for f_field in possible_fields:
#         match = find_best_field_match(f_field, field_map.keys())
#         if match:
#             matched_fields.append(match)
#     if not matched_fields:
#         matched_fields = ["Unknown"]
#     standardized = {
#         "level": degree_level,
#         "fields": matched_fields
#     }
#     return standardized


# def standardize_degree(degree_str):
#     """Extract and standardize the degree level and fields from a raw degree string."""
#     original_str = degree_str
#     degree_str = degree_str.strip()
#     degree_level = None
#     # Identify degree level
# for pattern, level in degree_patterns:
#     if re.search(pattern, degree_str, flags=re.IGNORECASE):
#         degree_level = level
#         # Remove matched pattern
#         degree_str = re.sub(pattern, "", degree_str, flags=re.IGNORECASE).strip()
#             break
#     if not degree_level:
#         degree_level = "Other"
#     # Normalize remaining field string
#     degree_str = normalize_field_string(degree_str)
#     # Split by common connectors like "and", "," to find multiple fields
#     possible_fields = re.split(r"\band\b|,", degree_str, flags=re.IGNORECASE)
#     possible_fields = [f.strip() for f in possible_fields if f.strip()]
#     matched_fields = []
#     for f_field in possible_fields:
#         match = find_best_field_match(f_field)
#         if match:
#             matched_fields.append(match)
#     if not matched_fields:
#         matched_fields = ["Unknown"]
#     standardized = {
#         "level": degree_level,
#         "fields": matched_fields,
#         "original": original_str
#     }
#     return standardized
#
#
# import re
# from difflib import get_close_matches
#
# def normalize_text(text):
#     return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
#
# def map_degree_to_field(degree_name, degree_patterns, field_map):
#     # Match and standardize degree (e.g., Master's, Bachelor's, Ph.D.)
#     for pattern, standard_name in degree_patterns:
#         if re.search(pattern, degree_name, re.IGNORECASE):
#             # Extract the field of study
#             field_match = re.sub(pattern, "", degree_name, flags=re.IGNORECASE).strip(" ,.-")
#             normalized_field = normalize_text(field_match)
#
#             # Attempt to match the field of study dynamically
#             for field, aliases in field_map.items():
#                 if normalized_field in aliases or get_close_matches(normalized_field, aliases, n=1, cutoff=0.8):
#                     return f"{standard_name} in {field}"
#
#             # Fallback to just the degree if no exact field match
#             return f"{standard_name} in {normalized_field.title()}" if normalized_field else standard_name
#
#     # If no degree matches, return 'Unknown'
#     return "Unknown"

# Group Education/Degrees


# Group Country/Location


# Notes
# @ validator function to run in azure
# Failed category

# Test Steps
# jobs = get_all_jobs()
# created_after = "2024-12-15T00:00:00Z"
# filtered_applications = get_applications(created_after)
#
# resume_applications, failed = download_resume_from_applications(filtered_applications)
# merged_list = merge_jobs_and_applications(jobs, resume_applications)
# filtered_candidate_list = merged_list
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
#
# check = check_gpt(openai_client, batch)
# gpt_results = poll_gpt_check(check)
# validated_json, failed_messages = validation_gpt_response(gpt_results)
#
# service = authenticate_google_sheets()
# write_to_google_sheet(service, validated_json)

#
#
# import pickle
# with open('data.pkl', 'wb') as file:
#     pickle.dump(gpt_results, file)
#
# import pickle
# with open('data.pkl', 'rb') as file:
#     gpt_results = pickle.load(file)



# # Single one off
# fl0 = filtered_candidate_list[0:10]
# openai_client = create_openai_client(OPEN_AI_KEY)
# gpt_result = parse_with_chatgpt(openai_client, fl0)
# start_index = gpt_result.find("{")
# end_index = gpt_result.rfind("}") + 1
# json_string = gpt_result[start_index:end_index]
# parsed_json_data = json.loads(json_string)
# gpt_results = [parsed_json_data]
# validated_json, failed_messages = validation_gpt_response(gpt_results)
#
#
#
#
# start_index = gpt_result.find("{")
# end_index = gpt_result.rfind("}") + 1
# json_string = gpt_result[start_index:end_index]
# parsed_json_data = json.loads(json_string)

# degree = parsed_json_data['Degree']
# standardized_degree_dict = standardize_degree(degree)
# degree_str = standardized_degree_dict["fields"]
# parsed_json_data["Degree Category"] = degree_str
#
#
# counts = count_keywords_from_sheet(service)
# write_keywords(service, counts)
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
