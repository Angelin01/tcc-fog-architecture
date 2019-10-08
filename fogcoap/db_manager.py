import pymongo
import re
import logging
import numpy as np
from fogcoap.alerts import AlertSpec, ArrayTreatment
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from enum import Enum
from bson.objectid import ObjectId
from datetime import datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from typing import Union, Tuple, List


database_logger = logging.Logger(__name__)


class StorageType(Enum):
	NUMBER = 0
	STR = 1
	ARRAY = 2
	
	_ignore_ = ['_type_storage_dict']
	_type_storage_dict = {}
	
	@staticmethod
	def type_enum(n):
		return StorageType._type_storage_dict[n]
	
	@staticmethod
	def is_instance(value, value_type):
		return StorageType._type_storage_dict.get(type(value)) == value_type
	
	
StorageType._type_storage_dict = {
	int: StorageType.NUMBER,
	float: StorageType.NUMBER,
	str: StorageType.STR,
	list: StorageType.ARRAY
}


class DatabaseManager:
	"""
	Abstraction class for interacting with the MongoDB.
	"""
	_ClientRegistry = 'client_registry'
	_ClientNameIndex = 'name_index'
	_TypeMetadata = 'type_metadata'
	_TypeMetadataNameIndex = 'type_index'
	_Data = 'data'

	def __init__(self, database: str, uri: str = 'mongodb://localhost', warnings: bool = True) -> None:
		"""
		Instances and connects a DatabaseManager to a MongoDB.
		:param database: The database name to use.
		:param uri: A connection uri used to connect to the database, same as would be used in connecting with Mongo normally.
		:param warnings: When set to true, the Manager will throw warnings when creating datatypes or registering clients with similar
		                 names to ones previously created, or when abnormalies happen, for example when data with a timestamp distant from
		                 the server's is received.
		"""
		# Setup logger #
		# ======================= #
		formatter = logging.Formatter(f'%(asctime)s - {database} - %(levelname)s - %(message)s')
		handler = logging.StreamHandler()
		handler.setFormatter(formatter)

		if warnings:
			handler.setLevel(logging.WARNING)
		else:
			handler.setLevel(logging.ERROR)

		database_logger.addHandler(handler)
		# ======================= #

		# Connect to database and setup data structure #
		# ======================= #
		
		self._client = pymongo.MongoClient(uri)
		try:
			# The ismaster command is cheap and does not require auth.
			self._client.admin.command('ismaster')
		except ConnectionFailure:
			database_logger.critical(f'Database connection to {uri} failed', flush=True)
			raise
		database_logger.info(f'Connected to database {database}')

		self._database = self._client[database]

		self._client_registry = self._database[self._ClientRegistry]
		self._client_registry.create_index('name', name=self._ClientNameIndex, unique=True)
		database_logger.debug('Created index on client names')

		self._type_metadata = self._database[self._TypeMetadata]
		self._type_metadata.create_index('name', name=self._TypeMetadataNameIndex, unique=True)
		self._data = self._database[self._Data]
		
		# ======================= #
		
		self.warnings = warnings
		
		self._cached_datatypes = {}
		self._cached_clients = {}
		self._cached_alert_specs = {}

	def register_client(self, client: str, ecc_public_key: bytes) -> ObjectId:
		"""
		Registers a client on the database.
		:param client: The client's name.
		:param ecc_public_key: A ECC PEM encoded public key.
		:return: The ObjectId for the client in the database.
		"""
		
		# Checks for errors on the public key, will be used for nothing else
		serialization.load_pem_public_key(ecc_public_key, default_backend())
		
		# ======================= #
		# Check for similarities if needed #
		# TODO Better similarity check
		similar_names = 0
		if self.warnings:
			similar_names = self._client_registry.count_documents({'name': re.compile(client, re.IGNORECASE)})
		# ======================= #

		# ======================= #
		# Actual insert
		try:
			obj_id = self._client_registry.insert_one({'name': client, 'ecc_public_key': ecc_public_key}).inserted_id
		except DuplicateKeyError:
			database_logger.error(f'Failed to add client {client} as it\'s a duplicate')
			raise
		database_logger.info(f'Registered new client {client} with id {obj_id}')
		# ======================= #

		# ======================= #
		# Warn for similarities if necessary #
		if similar_names > 0:
			database_logger.warning(f'There are {similar_names} clients with similar names to {client}')
		# ======================= #

		return obj_id

	def register_datatype(self, name: str, storage_type: StorageType, array_type: StorageType = None, unit: str = None,
	                      valid_bounds: tuple = None, alert_spec: AlertSpec = None) -> ObjectId:
		"""
		Register a type of data that will be inserted into the DB.
		Unregistered types or data that does not match the requirements will be rejected.
		
		:param name: The name of the data to be stored, for example "temperature".
		:param storage_type: What actual type the data is: `INT`, `FLOAT`, `STR` or an `ARRAY`, use the `StorageType` enum.
		:param array_type: If `storage_type` is ARRAY, a type of data must be set for said array. Cannot be `ARRAY`.
		:param unit: Optional unit like "seconds" or "s", as a string, to save with the rest of the data.
		:param valid_bounds: An optional tuple or list of 2 elements that dictates the minimum and maximum values, respectively,
		of the data to be stored. Both bounds are optional, being ignored if passed as `None`.
		:param alert_spec: An optional instance of `AlertSpec`, which specifies how (and if) alerts should be generated.
		will be generated. Both bounds are optional, being ignored if passed as `None`.
		:return: The ObjectId for the type in the database.
		"""
		# ======================= #
		# Validate received values #
		# Check if type is in enum, will not be needed in Python 3.8
		if storage_type not in StorageType:
			raise TypeError(f'Invalid storage_type when registering type {name}')
		
		if storage_type is not StorageType.ARRAY and array_type is not None:
			database_logger.warning('Parameter array_type will be ignored if storage_type is not ARRAY')
			array_type = None
		
		if storage_type is StorageType.ARRAY:
			if array_type is None:
				raise TypeError('If storage_type is ARRAY, array_type must be set')
			
			if array_type not in StorageType or array_type is StorageType.ARRAY:  # Same as above, check for enum won't be needed in 3.8
				raise TypeError('Invalid array storage type, must be NUMBER or STR')
			
			if alert_spec is not None and alert_spec.array_treatment is None:
				raise ValueError('Specified AlertSpec must have an array_treatment set for a datatype with type ARRAY')

		# Verify bounds and alerts, except for strs
		if not (storage_type is StorageType.STR or (storage_type is StorageType.ARRAY and array_type is StorageType.STR)):
			try:
				self._verify_bounds(valid_bounds, alert_spec, array_type if storage_type is StorageType.ARRAY else storage_type)
			except (ValueError, TypeError) as e:
				raise Exception(f'Invalid bounds or thresholds when registering type {name}') from e
		else:
			if valid_bounds is not None:
				valid_bounds = None
				database_logger.warning('Can\'t set bounds for STRs, they will be ignored')
			if alert_spec is not None:
				alert_spec = None
				database_logger.warning('Can\'t set alert thresholds for STRs, they will be ignored')
		# ======================= #
		
		# ======================= #
		# Check for similarities if needed #
		# TODO Better similarity check
		similar_names = 0
		if self.warnings:
			similar_names = self._type_metadata.count_documents({'name': re.compile(name, re.IGNORECASE)})
		# ======================= #

		# ======================= #
		# Actual insert
		try:
			obj_id = self._type_metadata.insert_one({
				'name': name,
				'storage_type': storage_type.value,
				'array_type': None if array_type is None else array_type.value,
				'unit': unit,
				'valid_bounds': valid_bounds,
				'alert_spec': alert_spec.to_dict()
			}).inserted_id
		except DuplicateKeyError:
			database_logger.error(f'Failed to add type {name} as it\'s a duplicate')
			raise
		database_logger.info(f'Registered new type {name} with id {obj_id}')
		
		# Warn for similarities if necessary #
		if similar_names > 0:
			database_logger.warning(f'There are {similar_names} types with similar names to {name}')
		# ======================= #
	
		return obj_id

	def insert_data(self, client: Union[str, ObjectId], data: dict) -> ObjectId:
		"""
		Method for inserting data into the database.
		:param client: Either the client name as a string or the client's `ObjectId` as returned by the `register_client` method.
		:param data: A dictionary containing the data. Should contain 3 keys:
		             "n" or "name": the name of the registered datatype.
		             "v" or "value": the actual value of the data. Must match the unit type specified when registering the data.
		             "t" or "time": A timestamp of when the data was collected. Can be an actual `int` timestamp, as the number of seconds since
		                            1970-01-01 UTC, an ISO date formatted string or a Python `datetime` object.
		:return: The ObjectID of the inserted data.
		
		"""
		# ======================= #
		# Check client #
		client_info = self._verify_client(client)
		# ======================= #
		
		# ======================= #
		# Get values from dict #
		data_name, data_value, data_datetime = self._verify_data(data)
		# ======================= #
		
		# ======================= #
		# Verify the received data #
		datatype_info = self._verify_datatype(data_name)
		
		try:
			value_type = StorageType.type_enum(type(data_value))
			if value_type.value != datatype_info['storage_type']:
				database_logger.info('Received data insert with incorrect value type')
				raise InvalidData('Value type is different from the registered data type')
			
			if value_type is not StorageType.ARRAY:
				if value_type is not StorageType.STR and datatype_info['valid_bounds'] is not None:
					if (datatype_info['valid_bounds'][0] is not None and data_value < datatype_info['valid_bounds'][0]) or \
					   (datatype_info['valid_bounds'][1] is not None and data_value > datatype_info['valid_bounds'][1]):
						database_logger.info('Received data insert with value outside the allowed bounds')
						raise InvalidData('Value is outside the valid bounds')
				
			else:
				if len(data_value) == 0:
					database_logger.info('Received array data insert with empty list')
					raise InvalidData('Array of values is empty')
				
				for v in data_value:
					v_type = StorageType.type_enum(type(v))
					if v_type.value != datatype_info['array_type']:
						database_logger.info('Received data insert with incorrect value type')
						raise InvalidData('Value type in list is different from the registered array type')
					if v_type is not StorageType.STR and datatype_info['valid_bounds'] is not None:
						if (datatype_info['valid_bounds'][0] is not None and data_value < datatype_info['valid_bounds'][0]) or \
						   (datatype_info['valid_bounds'][1] is not None and data_value > datatype_info['valid_bounds'][1]):
							database_logger.info('Received data insert with value outside the allowed bounds')
							raise InvalidData('Value in list is outside the valid bounds')
					
		except KeyError:
			database_logger.info('Received data insert with incorrect value type')
			raise InvalidData('Value type is not a valid type, expected number, str or list')
		
		if self.warnings:
			time_diff = data_datetime.timestamp() - datetime.now().timestamp()
			if time_diff >= 900:  # 15 minutes, make it configurable later?
				database_logger.warning(f'Data received from {client_info["name"]} has timestamp ahead of the server by {time_diff} seconds, '
				                        f'either server or client is desynced')
			elif time_diff <= -86400:  # 1 day, make it configurable later?
				database_logger.warning(f'Data received from {client_info["name"]} has timestamp behind of the server by {time_diff} seconds, '
				                        f'either client is desynced or it was disconnected for a long time')
		
		database_logger.info(f'Received successful data insert for client {client_info["name"]}')
		return self._data[str(client_info['name'])][str(datatype_info['name'])].insert_one({'value': data_value, 'datetime': data_datetime}).inserted_id

	def verify_alert(self, data: dict, client: Union[str, ObjectId]) -> Union[dict, None]:
		"""
		Verifies the client supplied data for alerts.
		:param data: A dictionary containing the data. Should contain 3 keys:
		             "n" or "name": the name of the registered datatype.
		             "v" or "value": the actual value of the data. Must match the unit type specified when registering the data.
		             "t" or "time": A timestamp of when the data was collected. Can be an actual `int` timestamp, as the number of seconds since
		                            1970-01-01 UTC, an ISO date formatted string or a Python `datetime` object.
		:return: Returns `None` if no alerts were found, otherwise returns a dict with three keys: "n" and "t", the same as supplied in the client
		         data, and the key "a" (for alert) which contains a *description string* of the alert similar to `value < min_threshold` or
		         `value > max_threshold`.
		"""
		data_name, data_value, data_datetime = self._verify_data(data)
		datatype_info = self._verify_datatype(data_name)
		if data_name in self._cached_alert_specs:
			alert_spec = self._cached_alert_specs[data_name]
		else:
			if datatype_info['alert_spec'] is not None:
				alert_spec = AlertSpec.from_dict(datatype_info['alert_spec'])
				self._cached_alert_specs[data_name] = alert_spec
			else:
				alert_spec = None
				self._cached_alert_specs[data_name] = None
		
		value_type = StorageType.type_enum(type(data_value))
		if value_type.value != datatype_info['storage_type']:
			database_logger.info('Received data insert with incorrect value type')
			raise InvalidData('Value type is different from the registered data type')
		
		if alert_spec is None or value_type is StorageType.STR:
			return None
		
		# If it's an array of values, convert the value to be whatever it needs to be according to the spec
		if value_type == StorageType.ARRAY and alert_spec.array_treatment is not ArrayTreatment.INDIVIDUALLY:
			if alert_spec.array_treatment is ArrayTreatment.MEAN:
				data_value = np.mean(data_value)
			elif alert_spec.array_treatment is ArrayTreatment.SUM:
				data_value = np.sum(data_value)
			elif alert_spec.array_treatment is ArrayTreatment.MAX:
				data_value = np.max(data_value)
			elif alert_spec.array_treatment is ArrayTreatment.MIN:
				data_value = np.min(data_value)
			elif alert_spec.array_treatment is ArrayTreatment.MEDIAN:
				data_value = np.median(data_value)
		
		if alert_spec.abs_alert_thresholds is not None:
			alert = self._verify_alert_abs_thresholds(data_value, alert_spec.abs_alert_thresholds)
			if alert is not None:
				# TODO: Actually do some stuff, don't just return it
				return alert
		
		if alert_spec.interval_groups is not None:
			for interval in alert_spec.interval_groups:
				alert = self._verify_alert_interval(data_value, interval)
				if alert is not None:
					# TODO: Actually do some stuff, don't just return it
					return alert
		
		# We can assume that past_avg_count is also not None since an AlertSpec checks for it
		if alert_spec.avg_deviation is not None:
			client_info = self._verify_client(client)
			# Only check avg if the number of documents stored is already higher than the past_avg_count necessary
			if self._database[self._Data][client_info['name']][datatype_info['name']].count_documents() >= alert_spec.past_avg_count:
				avg = np.mean([record['value'] for record in
				              self._database[self._Data][client_info['name']][datatype_info['name']]
				              .find(projection={'_id': 0, 'value': 1}).sort('_id', pymongo.DESCENDING).limit(alert_spec.past_avg_count)])
				
				alert = self._verify_alert_avg_deviation(data_value, alert_spec.avg_deviation, avg)
				if alert is not None:
					# TODO: Actually do some stuff, don't just return it
					return alert

	def query_data_client(self, client: Union[str, ObjectId], datatype: Union[str, ObjectId] = None,
	                      date_range: Tuple[Union[str, int, datetime, None], Union[str, int, datetime, None]] = None) -> dict:
		"""
		Queries the data for a specific client.
		:param client: Either the `ObjectID` or the name of the registered client.
		:param datatype: An optional `ObjectID` or the name of the registered datatype as a filter.
		:param date_range: An optional tuple that specifies the beginning and end dates for querying.
		:return: A dict with all the data.
		"""
		# ======================= #
		# Check client #
		client_info = self._verify_client(client)
		client_filter = client_info['name']
			
		# ======================= #
		# Check datatype #
		if datatype:
			datatype_info = self._verify_datatype(datatype)
			datatype_filter = str(datatype_info['name'])
		else:
			datatype_filter = '.*'
		# ======================= #
		date_filter = self._setup_date_filter(date_range)
		
		all_data = {}
		# Filter breaks down collections that start with the prefix for data
		# The name format is "data.[CLIENT_ID].[DATATYPE_ID]"
		for coll in self._database.list_collection_names(filter={'name': {'$regex': f'{self._Data}\.{client_filter}\.{datatype_filter}'}}):
			_, client, datatype = coll.split('.')
			
			# Convert the returns to a list and add it to the dict
			all_data[datatype] = list(self._database[coll].find(date_filter))
		
		database_logger.info(f'Received successful client data query for client {client}')
		return all_data

	def query_data_type(self, datatype: Union[str, ObjectId], date_range: Tuple[Union[str, int, datetime, None], Union[str, int, datetime, None]] = None) -> dict:
		"""
		Queries the data for a specific datatype.
		:param datatype: Either a `ObjectID` or the name of the registered datatype as a filter.
		:param date_range: An optional tuple that specifies the beginning and end dates for querying.
		:return: A dict with all the data.
		"""
		# ======================= #
		# Check datatype #
		datatype_info = self._verify_datatype(datatype)
		datatype_filter = str(datatype_info['name'])
		# ======================= #
		date_filter = self._setup_date_filter(date_range)
		
		all_data = {}
		# Filter breaks down collections that start with the prefix for data
		# The name format is "data.[CLIENT_ID].[DATATYPE_ID]"
		for coll in self._database.list_collection_names(filter={'name': {'$regex': f'{self._Data}\..*\.{datatype_filter}'}}):
			_, client, datatype = coll.split('.')
			
			# Convert the returns to a list and add it to the dict
			all_data[client] = list(self._database[coll].find(date_filter))
		
		database_logger.info(f'Received successful datatype data query for datatype {datatype}')
		return all_data
	
	def query_all(self, date_range: Tuple[Union[str, int, datetime, None], Union[str, int, datetime, None]] = None) -> dict:
		"""
		Returns all actual data in the database, not including the metadata for clients and datatypes.
		:param date_range: An optional tuple that specifies the beginning and end dates for querying.
		:return: A dict with all the data.
		"""
		date_filter = self._setup_date_filter(date_range)
		all_data = {}
		
		# Filter breaks down collections that start with the prefix for data
		# The name format is "data.[CLIENT_ID].[DATATYPE_ID]"
		for coll in self._database.list_collection_names(filter={'name': {'$regex': f'{self._Data}\.'}}):
			_, client, datatype = coll.split('.')
			
			# Create the dicts for the client if it doesn't exist on the return yet
			if not all_data.get(client):
				all_data[client] = {}
			
			# Create the dicts for the datatype on the client if it doesn't exist on the return yet
			if not all_data[client].get(datatype):
				all_data[client][datatype] = {}
			
			# Convert the returns to a list and add it to the dict
			all_data[client][datatype] = list(self._database[coll].find(date_filter))
		
		database_logger.info('Received successful generic data query')
		return all_data
	
	def query_datatypes(self) -> list:
		"""
		Queries all the registered datatypes in the database.
		:return: A list of datatypes on the database.
		"""
		database_logger.info('Received query for datatypes')
		# TODO: Add some filters?
		return list(self._type_metadata.find())
	
	def query_clients(self) -> list:
		"""
		Queries all the registered clients in the database.
		:return: A list of clients on the database.
		"""
		database_logger.info('Received query for clients')
		# TODO: Extra logging
		return list(self._client_registry.find())
	
	def close(self) -> None:
		"""
		Closes the connection to the database. If the database is used again, it will be automatically re-opened.
		"""
		self._client.close()
	
	def _verify_client(self, client: Union[str, ObjectId]) -> dict:
		client_info = None
		if client in self._cached_clients:
			return self._cached_datatypes[client]
		
		if isinstance(client, str):
			client_info = self._client_registry.find_one({'name': client})
		elif isinstance(client, ObjectId):
			client_info = self._client_registry.find_one({'_id': client})
		
		if client_info is None:
			database_logger.info(f'Received data insert request for non registered client {client}')
			raise InvalidClient('Specified client has not been registered')
		
		self._cached_clients[client_info['name']] = client_info
		self._cached_clients[client_info['_id']] = client_info
		
		return client_info
	
	def _verify_data(self, data: dict) -> Tuple[str, str, datetime]:
		data_name = data.get('n') or data.get('name')
		if not data_name:
			database_logger.info('Received data insert with missing data name')
			raise InvalidData('Data name "n" or "name" not specified')
		
		data_value = data.get('v') or data.get('value')
		if not data_value:
			database_logger.info('Received data insert with missing data value')
			raise InvalidData('Data value "v" or "value" not specified')
		
		data_timestamp = data.get('t') or data.get('time')
		if not data_timestamp:
			database_logger.info('Received data insert with missing data time')
			raise InvalidData('Data timestamp "t" or "time" not specified')
		
		try:
			data_datetime = self._parse_timestamp(data_timestamp)
		except InvalidData:
			database_logger.info('Received data insert with invalid time')
			raise
		
		return data_name, data_value, data_datetime
	
	def _verify_datatype(self, datatype: Union[str, ObjectId]) -> dict:
		datatype_info = None
		if datatype in self._cached_datatypes:
			return self._cached_datatypes[datatype]
		
		if isinstance(datatype, str):
			datatype_info = self._type_metadata.find_one({'name': datatype})
		elif isinstance(datatype, ObjectId):
			datatype_info = self._type_metadata.find_one({'_id': datatype})
		
		if datatype_info is None:
			database_logger.info(f'Received client data query request for non registered datatype {datatype}')
			raise InvalidData('Specified datatype has not been registered')
		
		self._cached_datatypes[datatype_info['name']] = datatype_info
		self._cached_datatypes[datatype_info['_id']] = datatype_info
	
		return datatype_info
	
	@staticmethod
	def _setup_date_filter(date_range: Tuple[Union[str, int, datetime, None], Union[str, int, datetime, None]]) -> dict:
		date_filter = None
		if date_range is not None:
			if not isinstance(date_range, list):
				raise TypeError('Date range is not a list')
			
			date_filter = {}
			if len(date_range) != 2:
				raise ValueError('Expected 2 values in date range')
			start_date = DatabaseManager._parse_timestamp(date_range[0]) if date_range[0] is not None else None
			end_date = DatabaseManager._parse_timestamp(date_range[1]) if date_range[1] is not None else None
			
			if start_date is None and end_date is None:
				raise ValueError('Either the start date or the end date must not be None')
			
			# Swap dates if they are inverted
			# Better to fix the mistake than to raise an exception when possible
			if (start_date is not None and end_date is not None) and start_date > end_date:
				start_date, end_date = end_date, start_date
			
			date_filter['datetime'] = {}
			if start_date:
				date_filter['datetime']['$gte'] = start_date
			if end_date:
				date_filter['datetime']['$lte'] = end_date
		return date_filter
	
	@staticmethod
	def _parse_timestamp(t):
		try:
			if isinstance(t, datetime):
				return t
			if isinstance(t, str):
				return datetime.fromisoformat(t)
			if isinstance(t, int):
				return datetime.fromtimestamp(t)
			
		except ValueError:
			raise InvalidData('Timestamp format is invalid. Expected datetime object or ISO str or int timestamp')
		raise InvalidData('Timestamp type is invalid, expected datetime object, str or int')
	
	@staticmethod
	def _verify_bounds(bounds: tuple, alert_spec: AlertSpec, expected_type: StorageType):
		# Check that type isn't strings
		if expected_type is not StorageType.NUMBER:
			raise TypeError('Bounds and Thresholds can only be numbers')
		
		# Check bounds object, types and values
		if bounds is not None:
			if len(bounds) != 2:
				raise ValueError('Expected 2 values in bounds')
			
			if not (StorageType.is_instance(bounds[0], expected_type) or bounds[0] is None) or \
			   not (StorageType.is_instance(bounds[1], expected_type) or bounds[1] is None):
				raise TypeError(f'Types for bounds don\'t match with expected type {expected_type.name}')
			
			# Swap bounds if they are inverted
			# Better to fix the mistake than to raise an exception
			if bounds[0] is not None and bounds[1] is not None and bounds[0] >= bounds[1]:
				raise ValueError(f'Low bound {bounds[0]} cannot be higher than high bound {bounds[1]}')
		
		# Check if thresholds are valid
		if bounds is not None and alert_spec is not None:
			if alert_spec.abs_alert_thresholds is not None:
				if bounds[0] is not None:
					if alert_spec.abs_alert_thresholds[0] is not None and alert_spec.abs_alert_thresholds[0] < bounds[0] or \
					   alert_spec.abs_alert_thresholds[1] is not None and alert_spec.abs_alert_thresholds[1] < bounds[0]:
						raise ValueError(f'Alert thresholds can\'t be lower than low valid bound {bounds[0]}')
				
				if bounds[1] is not None:
					if alert_spec.abs_alert_thresholds[0] is not None and alert_spec.abs_alert_thresholds[0] > bounds[1] or \
					   alert_spec.abs_alert_thresholds[1] is not None and alert_spec.abs_alert_thresholds[1] > bounds[1]:
						raise ValueError(f'Alert thresholds can\'t be higher than high valid bound {bounds[1]}')
			
			if alert_spec.interval_groups is not None:
				for group in alert_spec.interval_groups:
					if bounds[0] is not None:
						if group[0] is not None and group[0] < bounds[0] or \
						   group[1] is not None and group[1] < bounds[0]:
							raise ValueError(f'Alert intervals can\'t be lower than low valid bound {bounds[0]}')
					
					if bounds[1] is not None:
						if group[0] is not None and group[0] > bounds[1] or \
						   group[1] is not None and group[1] > bounds[1]:
							raise ValueError(f'Alert thresholds can\'t be higher than high valid bound {bounds[1]}')

	@staticmethod
	def _verify_alert_abs_thresholds(data_value, abs_thresholds):
		if hasattr(data_value, '__iter__'):
			return DatabaseManager._verify_loop_alert(DatabaseManager._verify_alert_abs_thresholds, data_value, abs_thresholds)
			
		else:
			if abs_thresholds[0] is not None and data_value < abs_thresholds[0]:
				return f'{data_value} < {abs_thresholds[0]}'
			elif abs_thresholds[1] is not None and data_value > abs_thresholds[1]:
				return f'{data_value} > {abs_thresholds[1]}'
		
		return None
	
	@staticmethod
	def _verify_alert_interval(data_value, interval):
		if hasattr(data_value, '__iter__'):
			return DatabaseManager._verify_loop_alert(DatabaseManager._verify_alert_interval, data_value, interval)
		
		else:
			if interval[0] < data_value < interval[1]:
				return f'{interval[0]} < {data_value} < {interval[1]}'
		
		return None
	
	@staticmethod
	def _verify_alert_avg_deviation(data_value, avg_limits, avg):
		if hasattr(data_value, '__iter__'):
			return DatabaseManager._verify_loop_alert(DatabaseManager._verify_alert_avg_deviation, data_value, avg_limits, avg)
		
		else:
			if avg_limits[0] is not None and data_value < (1 - avg_limits[0]) * avg:
				return f'{data_value} < {1 - avg_limits[0]}*{avg}'
			elif avg_limits[1] is not None and data_value > (1 + avg_limits[1]) * avg:
				return f'{data_value} > {1 + avg_limits[1]}*{avg}'
		
		return None

	@staticmethod
	def _verify_loop_alert(func, data_value, *args, **kwargs):
		if not hasattr(data_value, '__iter__'):
			raise TypeError('Loop alert method must receive an iterable data_value')
		
		has_alert = False
		alerts = []
		for value in data_value:
			alert = func(value, *args, **kwargs)
			alert.append(alerts)
			if alert is not None:
				has_alert = True
		return alerts if has_alert else None

	@staticmethod
	def set_logging_level(level: int) -> None:
		database_logger.setLevel(level)


class InvalidData(Exception):
	"""Raised when the received data's format is invalid"""
	pass


class InvalidClient(Exception):
	"""Raised when an unregisted client is specified when sending data"""
	pass


class InvalidECCKey(Exception):
	"""Raised when the provided ECC Key is invalid"""
	pass
