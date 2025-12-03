import unittest
from src.processor import Processor

class TestProcessor(unittest.TestCase):

    def setUp(self):
        self.processor = Processor()

    def test_read_records(self):
        records = self.processor.read_records('NTX IV_11272025.xlsx')
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

    def test_build_payload(self):
        record = {'key': 'value'}
        payload = self.processor.build_payload(record)
        self.assertIn('key', payload)
        self.assertEqual(payload['key'], 'value')

    def test_process_records(self):
        records = [{'key': 'value1'}, {'key': 'value2'}]
        responses = self.processor.process_records(records)
        self.assertIsInstance(responses, list)
        self.assertEqual(len(responses), len(records))

if __name__ == '__main__':
    unittest.main()