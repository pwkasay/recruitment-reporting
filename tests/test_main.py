import pytest
import json
import base64
from unittest.mock import AsyncMock, patch

@pytest.fixture
def setup_env(monkeypatch):
    """
    This fixture sets environment variables that `main.get_secrets()` reads.
    It runs before each test and reverts after the test finishes.
    """
    monkeypatch.setenv("GREENHOUSE_API_KEY", "fake-greenhouse-api-key")
    monkeypatch.setenv("GREENHOUSE_BASE_URL", "https://fake.greenhouse.io")
    monkeypatch.setenv("SPREADSHEET_ID", "fake-spreadsheet-id")
    monkeypatch.setenv("OPEN_AI_KEY", "fake-openai-key")
    monkeypatch.setenv("USER_ID", "fake-user-id")

    # Minimal valid JSON for a "service_account"
    fake_service_account = {"type": "service_account"}
    encoded_creds = base64.b64encode(json.dumps(fake_service_account).encode("utf-8")).decode("utf-8")
    monkeypatch.setenv("GOOGLE_SHEETS_CREDENTIALS_BASE64", encoded_creds)
    yield

@pytest.mark.asyncio
async def test_process_success(setup_env):
    """
    A "happy path" test where everything returns successfully.
    """
    # IMPORTANT: Import main AFTER environment is set up
    import main

    # Mock: get_all_jobs
    async_mock_get_all_jobs = AsyncMock(return_value=[
        {"id": 101, "name": "Test Job A"},
        {"id": 202, "name": "Test Job B"},
    ])
    # Mock: get_applications
    async_mock_get_applications = AsyncMock(return_value=[
        {"jobs": [{"id": 101}], "attachments": [], "candidate_name": "Alice"},
        {"jobs": [{"id": 202}], "attachments": [], "candidate_name": "Bob"},
    ])
    # Mock: download_resume_from_applications
    async_mock_download_resumes = AsyncMock(return_value=(
        [
            {"jobs": [{"id": 101}], "candidate_name": "Alice", "resume_content": "Alice Resume"},
            {"jobs": [{"id": 202}], "candidate_name": "Bob",   "resume_content": "Bob Resume"},
        ],
        []  # no failures
    ))
    # Mock: merge_jobs_and_applications
    async_mock_merge_jobs_apps = AsyncMock(return_value=[
        {"id": 101, "name": "Test Job A", "candidate_name": "Alice", "resume_content": "Alice Resume"},
        {"id": 202, "name": "Test Job B", "candidate_name": "Bob",   "resume_content": "Bob Resume"},
    ])
    # Mock: create_openai_client
    async_mock_create_openai_client = AsyncMock()
    async_mock_create_openai_client.return_value = "fake_openai_client"

    # Mock parse_with_chatgpt to return JSON strings
    async_mock_parse_with_chatgpt = AsyncMock(side_effect=[
        '{"Candidate Id": 111, "Role": "Engineer", "Company": "ACME Inc"}',
        '{"Candidate Id": 222, "Role": "Data Scientist", "Company": "BigCo"}'
    ])

    # We also patch the Google sheets calls
    mock_authenticate_sheets = patch.object(main, "authenticate_google_sheets", return_value="fake_sheets_service")
    mock_write_sheets        = patch.object(main, "write_to_google_sheet")

    # Now apply all those patches at once, inside a context manager
    with patch.object(main, "get_all_jobs", async_mock_get_all_jobs), \
         patch.object(main, "get_applications", async_mock_get_applications), \
         patch.object(main, "download_resume_from_applications", async_mock_download_resumes), \
         patch.object(main, "merge_jobs_and_applications", async_mock_merge_jobs_apps), \
         patch.object(main, "create_openai_client", async_mock_create_openai_client), \
         patch.object(main, "parse_with_chatgpt", async_mock_parse_with_chatgpt), \
         mock_authenticate_sheets as mock_auth_sheets, \
         mock_write_sheets as mock_write_ws:

        # Execute the function under test
        response = await main.process("2023-01-01", "2023-01-31")

        # Check results
        assert response.status_code == 200
        assert "Processed to sheet successfully" in response.get_body().decode()

        # Verify calls
        async_mock_get_all_jobs.assert_called_once()
        async_mock_get_applications.assert_called_once()
        async_mock_download_resumes.assert_called_once()
        async_mock_merge_jobs_apps.assert_called_once()
        async_mock_create_openai_client.assert_called_once()
        # parse_with_chatgpt gets called once per candidate => total 2
        assert async_mock_parse_with_chatgpt.call_count == 2

        # Google sheets
        mock_auth_sheets.assert_called_once()
        mock_write_ws.assert_called_once()

@pytest.mark.asyncio
async def test_process_failure(setup_env):
    """
    Test scenario where an exception occurs in one of the steps—
    for instance, parse_with_chatgpt raises an error, so we return HTTP 500.
    """
    import main

    # Patch minimal external calls
    async_mock_get_all_jobs = AsyncMock(return_value=[{"id": 999, "name": "Failing Job"}])
    async_mock_get_applications = AsyncMock(return_value=[{"jobs": [{"id": 999}], "attachments": []}])
    async_mock_download_resumes = AsyncMock(return_value=([{"jobs": [{"id": 999}], "resume_content": "..."}], []))
    async_mock_merge_jobs_apps = AsyncMock(return_value=[{"id": 999, "resume_content": "..."}])
    async_mock_create_openai_client = AsyncMock(return_value="fake_openai_client")

    # Force parse_with_chatgpt to raise an exception
    async_mock_parse_with_chatgpt = AsyncMock(side_effect=Exception("GPT error!"))

    with patch.object(main, "get_all_jobs", async_mock_get_all_jobs), \
         patch.object(main, "get_applications", async_mock_get_applications), \
         patch.object(main, "download_resume_from_applications", async_mock_download_resumes), \
         patch.object(main, "merge_jobs_and_applications", async_mock_merge_jobs_apps), \
         patch.object(main, "create_openai_client", async_mock_create_openai_client), \
         patch.object(main, "parse_with_chatgpt", async_mock_parse_with_chatgpt):

        # Call process; we expect a 500 due to the parse error
        response = await main.process("2023-01-01", "2023-01-31")

        assert response.status_code == 500


