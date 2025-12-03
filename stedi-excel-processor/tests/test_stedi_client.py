import unittest
from unittest.mock import patch, MagicMock
from src.stedi_client import StediClient

class TestStediClient(unittest.TestCase):

    @patch('src.stedi_client.requests.post')
    def test_send_request_success(self, mock_post):
        # Arrange
        client = StediClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'success': True}
        mock_post.return_value = mock_response
        
        payload = {'data': 'test'}
        
        # Act
        response = client.send_request(payload)
        
        # Assert
        self.assertTrue(response['success'])
        mock_post.assert_called_once_with(client.api_url, json=payload)

    @patch('src.stedi_client.requests.post')
    def test_send_request_failure(self, mock_post):
        # Arrange
        client = StediClient()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {'success': False, 'error': 'Bad Request'}
        mock_post.return_value = mock_response
        
        payload = {'data': 'test'}
        
        # Act
        response = client.send_request(payload)
        
        # Assert
        self.assertFalse(response['success'])
        self.assertEqual(response['error'], 'Bad Request')
        mock_post.assert_called_once_with(client.api_url, json=payload)

if __name__ == '__main__':
    unittest.main()