import unittest
from fogcoap import db_manager
from datetime import datetime
from pymongo.errors import DuplicateKeyError
from bson.objectid import ObjectId


class MyTestCase(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		cls._db_manager = db_manager.DatabaseManager('unit_tests')
		cls._db_manager.set_logging_level(999)  # Log nothing
		cls._db_manager._client_registry.delete_many({})
		cls._db_manager._type_metadata.delete_many({})
		for coll in cls._db_manager._database.list_collection_names(filter={'name': {'$regex': f'{cls._db_manager._Data}\.'}}):
			cls._db_manager[coll].drop()
	
	@classmethod
	def tearDownClass(cls):
		cls._db_manager._client_registry.delete_many({})
		cls._db_manager._type_metadata.delete_many({})
		for coll in cls._db_manager._database.list_collection_names(filter={'name': {'$regex': f'{cls._db_manager._Data}\.'}}):
			cls._db_manager[coll].drop()
		
		cls._db_manager.close()
	
	def test_1_parse_timestamp(self):
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
		
	def test_2_setup_date_filter(self):
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
		
	def test_3_verify_bounds(self):
		"""
		Tests for _verify_bounds, extensive as there are many things to test
		"""
		print('Testing _verify_bounds')
		
		# Valid ints
		valid_bounds = (100, 1000)
		valid_thresholds = (400, 600)
		valid_expected_int = db_manager.StorageType.NUMBER
		try:
			self._db_manager._verify_bounds(valid_bounds, valid_thresholds, valid_expected_int)
		except (ValueError, TypeError) as e:
			self.fail(f'_verify_bounds raised {type(e).__name__} on valid ints!')
		
		# Valid floats
		valid_floats = (-50.0, 50.0)
		valid_float_thresholds = (-10.0, 10.0)
		valid_expected_float = db_manager.StorageType.NUMBER
		try:
			self._db_manager._verify_bounds(valid_floats, valid_float_thresholds, valid_expected_float)
		except (ValueError, TypeError) as e:
			self.fail(f'_verify_bounds raised {type(e).__name__} on valid floats!')
			
		# Valid half None
		valid_bounds_missing_1st = (None, 1000)
		valid_thresholds_missing_1st = (None, 600)
		valid_bounds_missing_2nd = (100, None)
		valid_thresholds_missing_2nd = (400, None)
		try:
			for bounds in [valid_bounds_missing_1st, valid_bounds_missing_2nd, None]:
				for thresholds in [valid_thresholds_missing_1st, valid_thresholds_missing_2nd, None]:
					self._db_manager._verify_bounds(bounds, thresholds, db_manager.StorageType.NUMBER)
		except (ValueError, TypeError) as e:
			self.fail(f'_verify_bounds raised {type(e).__name__} on valid partials!')
			
		# Valid all None
		tuple_none = (None, None)
		try:
			self._db_manager._verify_bounds(tuple_none, tuple_none, db_manager.StorageType.NUMBER)
		except (ValueError, TypeError) as e:
			self.fail(f'_verify_bounds raised {type(e).__name__} on valid floats!')

		# Wrong length
		too_long = (400, 600, 800)
		self.assertRaises(ValueError, self._db_manager._verify_bounds, too_long, valid_thresholds, db_manager.StorageType.NUMBER)
		self.assertRaises(ValueError, self._db_manager._verify_bounds, valid_bounds, too_long, db_manager.StorageType.NUMBER)

		# Swapped values
		swapped = (600, 400)
		self.assertRaises(ValueError, self._db_manager._verify_bounds, swapped, valid_thresholds, db_manager.StorageType.NUMBER)
		self.assertRaises(ValueError, self._db_manager._verify_bounds, valid_bounds, swapped, db_manager.StorageType.NUMBER)
		
		# Thresholds outside bounds
		wrong_thresholds = (0, 2000)
		self.assertRaises(ValueError, self._db_manager._verify_bounds, valid_bounds, wrong_thresholds, db_manager.StorageType.NUMBER)
		
	def _register_client(self):
		"""
		Simple test when registering a client
		Can't easily test warnings yet
		"""
		print('Testing register_client')
		
		client = 'testerino'
		# Valid first insert
		self.assertIsInstance(self._db_manager.register_client(client), ObjectId)
		
		# Invalid duplicate
		self.assertRaises(DuplicateKeyError, self._db_manager.register_client, client)
	
	def _query_clients(self):
		"""
		Simple test for the query of clients
		Expects the client from the _register_client test, run after that test
		"""
		print('Testing query_clients')
		
		# Insert one more client, expect 2
		self._db_manager.register_client('testoncio')
		query_result = self._db_manager.query_clients()
		
		# Expect 2 clients
		self.assertEqual(len(query_result), 2)
		# Expect all clients to be dicts
		for client in query_result:
			self.assertIsInstance(client, dict)
			
	def test_4_clients(self):
		self._register_client()
		self._query_clients()
		
	def _register_data(self):
		print('Testing register_datatype')
		# Valid simple
		self.assertIsInstance(self._db_manager.register_datatype(
			name='temperature',
			storage_type=db_manager.StorageType.NUMBER,
			unit='°C',
			valid_bounds=(-273.15, None),
			alert_thresholds=(0, 50)
		), ObjectId)
		self.assertIsInstance(self._db_manager.register_datatype(
			name='othername',
			storage_type=db_manager.StorageType.NUMBER,
			unit='G',
			valid_bounds=None,
			alert_thresholds=(0, 50)
		), ObjectId)
		self.assertIsInstance(self._db_manager.register_datatype(
			name='cake',
			storage_type=db_manager.StorageType.NUMBER,
			unit=None,
			valid_bounds=(-273.15, 500),
			alert_thresholds=None
		), ObjectId)
		self.assertIsInstance(self._db_manager.register_datatype(
			name='nothing',
			storage_type=db_manager.StorageType.STR,
			unit=None,
			valid_bounds=None,
			alert_thresholds=None
		), ObjectId)
		self.assertIsInstance(self._db_manager.register_datatype(
			name='hasalerts',
			storage_type=db_manager.StorageType.NUMBER,
			unit=None,
			valid_bounds=None,
			alert_thresholds=(0.05, 500)
		), ObjectId)
		
		# Valid arrays
		self.assertIsInstance(self._db_manager.register_datatype(
			name='isarray',
			storage_type=db_manager.StorageType.ARRAY,
			array_type=db_manager.StorageType.NUMBER,
			unit='L',
			valid_bounds=(0.5, 900),
			alert_thresholds=(0.5, 500)
		), ObjectId)
		self.assertIsInstance(self._db_manager.register_datatype(
			name='isarraystr',
			storage_type=db_manager.StorageType.ARRAY,
			array_type=db_manager.StorageType.STR,
			unit='M',
			valid_bounds=None,
			alert_thresholds=None
		), ObjectId)
		
		# Invalid inserts
		# Will not test bounds and thresholds as those are tested in test_3_verify_bounds
		# Missing storage_type
		self.assertRaises(TypeError, self._db_manager.register_datatype,
		                  name='invalid',
		                  storage_type=None,
		                  array_type=None,
		                  unit='M',
		                  valid_bounds=None,
		                  alert_thresholds=None)
		# storage_type not in enum
		self.assertRaises(TypeError, self._db_manager.register_datatype,
		                  name='invalid',
		                  storage_type=50,
		                  array_type=None,
		                  unit='M',
		                  valid_bounds=None,
		                  alert_thresholds=None)
		
		# storage_type is array and no array_type
		self.assertRaises(TypeError, self._db_manager.register_datatype,
		                  name='invalid',
		                  storage_type=db_manager.StorageType.ARRAY,
		                  array_type=None,
		                  unit='M',
		                  valid_bounds=None,
		                  alert_thresholds=None)
		# storage_type is array and array_type is array
		self.assertRaises(TypeError, self._db_manager.register_datatype,
		                  name='invalid',
		                  storage_type=db_manager.StorageType.ARRAY,
		                  array_type=db_manager.StorageType.ARRAY,
		                  unit='M',
		                  valid_bounds=None,
		                  alert_thresholds=None)
		
		# duplicate
		self.assertRaises(DuplicateKeyError, self._db_manager.register_datatype,
		                  name='temperature',
		                  storage_type=db_manager.StorageType.NUMBER,
		                  array_type=None,
		                  unit='M',
		                  valid_bounds=None,
		                  alert_thresholds=None)
		
	def test_5_data(self):
		self._register_data()
		

if __name__ == '__main__':
	unittest.main()
