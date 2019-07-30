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
		
		# Valid parameters
		datetime_obj = datetime(2019, 6, 5, 10, 0, 0)
		timestamp = 1564444070
		iso_string = '2019-05-03 10:00:00'
		
		# Invalid parameters
		something_wrong = ['stuff']
		invalid_timestamp = -100
		improper_string = '2019-05-NOT_VALID 10:00:00'

		for parameter in [datetime_obj, timestamp, iso_string]:
			self.assertIsInstance(self._db_manager._parse_timestamp(parameter), datetime)

		self.assertRaises(db_manager.InvalidData, self._db_manager._parse_timestamp, something_wrong)

		self.assertRaises(db_manager.InvalidData, self._db_manager._parse_timestamp, improper_string)

		self.assertRaises(OSError, self._db_manager._parse_timestamp, invalid_timestamp)
		
	def test__setup_date_filter(self):
		"""
		Test that _setup_date_filter properly checks everything and returns a valid filter
		Doesn't check the timestamp itself as it is treated (and tested) by _parse_timestamp
		"""
		print('Testing _setup_date_filter')
		
		# Valid parameters
		valid_datetime = (datetime(2018, 5, 1, 10, 0, 0), datetime(2019, 5, 1, 10, 0, 0))
		valid_timestamp = (1564444070, 1564464070)
		valid_isostring = ('2018-05-01 10:00:00', '2019-05-03 10:00:00')
		valid_missing_first = (None, 1564464070)
		valid_missing_second = ('2018-05-01 10:00:00', None)
		
		# Invalid parameters
		wrong_length = (1, 2, 3)
		both_none = (None, None)

		for parameter in [valid_datetime, valid_timestamp, valid_isostring]:
			result = self._db_manager._setup_date_filter(parameter)
			self.assertIn('$gte', result['datetime'])
			self.assertIn('$lte', result['datetime'])
			self.assertTrue(result['datetime']['$lte'] >= result['datetime']['$gte'])
			
		result = self._db_manager._setup_date_filter(valid_missing_first)
		self.assertIn('$lte', result['datetime'])
		self.assertNotIn('$gte', result['datetime'])
		
		result = self._db_manager._setup_date_filter(valid_missing_second)
		self.assertNotIn('$lte', result['datetime'])
		self.assertIn('$gte', result['datetime'])

		self.assertRaises(ValueError, self._db_manager._setup_date_filter, wrong_length)
	
		self.assertRaises(ValueError, self._db_manager._setup_date_filter, both_none)


if __name__ == '__main__':
	unittest.main()
