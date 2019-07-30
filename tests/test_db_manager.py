import unittest
from fogcoap import db_manager
from datetime import datetime


class MyTestCase(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		cls._db_manager = db_manager.DatabaseManager('unit_tests')
	
	@classmethod
	def tearDownClass(cls):
		cls._db_manager.close()
	
	def test__parse_timestamp(self):
		"""
		Test that _parse_timestamp returns a proper datetime and raises exceptions when it should
		"""
		print('Testing _parse_timestamp')
		datetime_obj = datetime(2019, 6, 5, 10, 0, 0)
		timestamp = 1564444070
		iso_string = "2019-05-03 10:00:00"
		something_wrong = ['stuff']
		invalid_timestamp = -100
		improper_string = '2019-05-NOT_VALID 10:00:00'
		
		print('Testing valid datetime returns')
		self.assertIsInstance(self._db_manager._parse_timestamp(datetime_obj), datetime)
		self.assertIsInstance(self._db_manager._parse_timestamp(timestamp), datetime)
		self.assertIsInstance(self._db_manager._parse_timestamp(iso_string), datetime)
		
		print('Testing invalid format')
		self.assertRaises(db_manager.InvalidData, self._db_manager._parse_timestamp, something_wrong)
		
		print('Testing invalid ISO string')
		self.assertRaises(db_manager.InvalidData, self._db_manager._parse_timestamp, improper_string)
		
		print('Testing invalid timestamp')
		self.assertRaises(OSError, self._db_manager._parse_timestamp, invalid_timestamp)
	

if __name__ == '__main__':
	unittest.main()
