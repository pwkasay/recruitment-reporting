# import pytest
# import requests_mock
# import json
# from unittest.mock import patch
# import pandas as pd
#
# # Mock DEAL_MAPPINGS, ACCOUNT_MAPPINGS, CONTACT_MAPPINGS and deal_owners_dict as they are used in your function
# DEAL_MAPPINGS = {
#     "Deal Name": "dealname",
#     "Round Size": "check_size",
#     "Priority": "priority",
#     "Referral/Ticket Type": "referral_type",
#     "Pipeline": "pipeline",
#     "Broad Category": "broad_category_updated",
#     "Subcategory": "subcategory",
#     "Passed on Initial Screen": "passed_on_initial_screen",
#     "Fund": "fund",
#     "Deal Stage": "dealstage",
#     "Source-General": "source_general",
#     "Source Detail": "source",
#     "Lead Owner": "hubspot_owner_id",
# }
#
# ACCOUNT_MAPPINGS = {
#     "Company Name": "name",
#     "Company Domain Name": "domain"
# }
#
# CONTACT_MAPPINGS = {
#     "CEO First Name": "firstname",
#     "CEO Last Name": "lastname",
#     "CEO Email": "email",
# }
#
# deal_owners_dict = {
#     'Software & Advanced Computing': ['Owner Name']
# }
#
# #update fixtures - also understand them - get a real mock of real data
# @pytest.fixture
# def parsed_info():
#     return '{"CEO First Name": "John", "CEO Last Name": "Doe", "Deal Name": "Test Deal", "Broad Category": "Software & Advanced Computing"}'
#
#
# @pytest.fixture
# def email_info():
#     return {
#         'subject': 'Test Email',
#         'body': 'Email body content',
#         'attachments': [],
#         'emails': [{'id': 'email1'}]
#     }
#
#
# @pytest.fixture
# def email():
#     return 'test@example.com'
#
#
# @patch('main.read_prompt_text', return_value='Mocked prompt text')
# @patch('main.read_deal_owners', return_value=pd.DataFrame({
#     'Broad Category': ['Software & Advanced Computing'],
#     'Person 1': ['Owner Name'],
#     'Person 2': [None],
#     'Person 3': [None],
#     'Person 4': [None],
#     'Person 5': [None]
# }))
# def test_create_hubspot_deal(mock_read_prompt_text, mock_read_deal_owners, requests_mock, parsed_info, email_info,
#                              email):
#     # Mock external functions
#     requests_mock.get('https://api.hubapi.com/crm/v3/owners',
#                       json={'results': [{'firstName': 'Owner', 'lastName': 'Name', 'id': 'owner_id'}]})
#     requests_mock.post('https://api.hubapi.com/crm/v3/objects/companies/search',
#                        json={'results': [{'id': 'company_id'}]})
#     requests_mock.post('https://api.hubapi.com/crm/v3/objects/contacts/search',
#                        json={'results': [{'id': 'contact_id'}]})
#     requests_mock.post('https://api.hubapi.com/crm/v3/objects/deals/search', json={'results': [], 'total': 0})
#     requests_mock.get('https://api.hubapi.com/crm/v3/properties/deals', json={'results': []})
#     requests_mock.post('https://api.hubapi.com/crm/v3/objects/deals', json={'id': 'deal_id'})
#
#     # Mock associations and attachments
#     requests_mock.post('https://api.hubapi.com/crm/v3/associations/contacts-to-companies/batch/create', json={})
#     requests_mock.post('https://api.hubapi.com/crm/v3/associations/deals-to-companies/batch/create', json={})
#     requests_mock.post('https://api.hubapi.com/crm/v3/associations/deals-to-contacts/batch/create', json={})
#
#     # Mock handle_attachments and create_email_engagement
#     requests_mock.post('https://api.hubapi.com/files/v3/files', json={'id': 'file_id'})
#     requests_mock.post('https://api.hubapi.com/engagements/v1/engagements', json={})
#
#     from main import create_hubspot_deal  # Import here after mocking
#
#     # Call the function
#     create_hubspot_deal(parsed_info, email_info, email)
#
#     # Validate the calls and responses
#     assert requests_mock.call_count == 10
#
#     # Check specific calls
#     search_company_call = requests_mock.request_history[1]
#     assert search_company_call.url == 'https://api.hubapi.com/crm/v3/objects/companies/search'
#
#     search_contact_call = requests_mock.request_history[2]
#     assert search_contact_call.url == 'https://api.hubapi.com/crm/v3/objects/contacts/search'
#
#     search_deal_call = requests_mock.request_history[3]
#     assert search_deal_call.url == 'https://api.hubapi.com/crm/v3/objects/deals/search'
#
#     create_deal_call = requests_mock.request_history[6]
#     assert create_deal_call.url == 'https://api.hubapi.com/crm/v3/objects/deals'
#
#
# if __name__ == '__main__':
#     pytest.main()
