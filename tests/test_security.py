from database.security_models import ApiKey
from tests.common import CommonTestCase


class ApiKeyTests(CommonTestCase):
    
    def test_generate(self):
        """ Test that `ApiKey.generate` produces an `ApiKey` object with valid credentials and
        one-time access to the secret key. """
        current_count = ApiKey.objects.count()
        api_key = ApiKey.generate(self.session_researcher)
        
        self.assertEqual(ApiKey.objects.count(), current_count + 1)
        self.assertTrue(api_key.access_key_id)
        self.assertTrue(api_key.access_key_secret)
        self.assertEqual(api_key.researcher, self.session_researcher)
        
        # Check that the secret key is accessible
        secret_key = api_key.access_key_secret_plaintext
        self.assertTrue(secret_key)
        
        # Check that the secret key is valid
        self.assertIs(api_key.proposed_secret_key_is_valid(secret_key), True)
    
    def test_access_key_secret_plaintext(self):
        """ Test that a newly generated `ApiKey` only allows access to the secret key once. """
        api_key = ApiKey.generate(self.session_researcher)
        
        secret_key = api_key.access_key_secret_plaintext
        self.assertTrue(secret_key)
        self.assertIsNone(api_key.access_key_secret_plaintext)
    
    def test_proposed_secret_key_is_valid(self):
        """ Test `ApiKey.proposed_secret_key_is_valid` """
        api_key = ApiKey.generate(self.session_researcher)
        secret_key = api_key.access_key_secret_plaintext
        
        self.assertTrue(secret_key)
        self.assertIs(api_key.proposed_secret_key_is_valid(secret_key), True)
        self.assertIs(api_key.proposed_secret_key_is_valid(f'not{secret_key}'), False)
