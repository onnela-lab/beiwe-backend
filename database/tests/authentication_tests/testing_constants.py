HOST = '0.0.0.0'
PORT = 54321

BASE_URL = f"http://{HOST}:{PORT}"
TEST_PASSWORD = "1"
TEST_STUDY_NAME = "automated_test_study"
TEST_STUDY_ENCRYPTION_KEY = '11111111111111111111111111111111'
TEST_USERNAME = "automated_test_user"


URLS = {
    'new_api_key': '/new_api_key',
    'disable_api_key': '/disable_api_key',
    'validate_login': "/validate_login",
}

URLS = {identifier: BASE_URL + url for identifier, url in URLS.items()}