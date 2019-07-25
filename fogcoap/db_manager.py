import pymongo
import re
import logging
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from enum import Enum
from bson.objectid import ObjectId
from datetime import datetime
from typing import Union, Tuple, NewType


GDR = NewType('GenDatetimeRange', Union[Tuple[str, str], Tuple[int, int], Tuple[datetime, datetime]])
database_logger = logging.Logger(__name__)


class StorageType(Enum):
	INT = 0
	FLOAT = 1
	STR = 2
	ARRAY = 3
	
storage_type_dict = {
	StorageType.INT: int,
	StorageType.FLOAT: float,
	StorageType.STR: str,
	StorageType.ARRAY: list
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

	def __init__(self, database: str, host: str = 'localhost', port: int = 27017, warn_similarities: bool = True) -> None:
		"""
		Instances and connects a DatabaseManager to a MongoDB.
		:param database: The database name to use.
		:param host: The optional host to connect to, defaults to `localhost`.
		:param port: The optional port to connect to, defaults to `27017`.
		:param warn_similarities: When set to true, the Manager will throw warning when creating datatypes or registering clients with similar
		                          names to ones previously created.
		"""
		# Setup logger #
		# ======================= #
		formatter = logging.Formatter(f'%(asctime)s - {database}@{host}:{port} - %(levelname)s - %(message)s')
		handler = logging.StreamHandler()
		handler.setFormatter(formatter)

		if warn_similarities:
			handler.setLevel(logging.WARNING)
		else:
			handler.setLevel(logging.ERROR)

		database_logger.addHandler(handler)
		# ======================= #

		# Connect to database and setup data structure #
		# ======================= #
		
		self._client = pymongo.MongoClient(host, port)
		try:
			# The ismaster command is cheap and does not require auth.
			self._client.admin.command('ismaster')
		except ConnectionFailure:
			database_logger.critical(f'Database connection to {host}:{port} failed', flush=True)
			raise
		database_logger.info(f'Connected to database {database} on {host}:{port}')

		self._database = self._client[database]

		self._client_registry = self._database[self._ClientRegistry]
		self._client_registry.create_index('name', name=self._ClientNameIndex, unique=True)
		database_logger.debug('Created index on client names')

		self._type_metadata = self._database[self._TypeMetadata]
		self._type_metadata.create_index('name', name=self._TypeMetadataNameIndex, unique=True)
		self._data = self._database[self._Data]
		
		# ======================= #
		
		self.warn_similarities = warn_similarities

	def register_client(self, client: str) -> ObjectId:
		"""
		Registers a client on the database.
		:param client: The client's name.
		:return: The ObjectId for the client in the database.
		"""
		# ======================= #
		# Check for similarities if needed #
		# TODO Better similarity check
		similar_names = 0
		if self.warn_similarities:
			similar_names = self._client_registry.count_documents({'name': re.compile(client, re.IGNORECASE)})
		# ======================= #

		# ======================= #
		# Actual insert
		try:
			obj_id = self._client_registry.insert_one({'name': client}).inserted_id
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
	                      valid_bounds: tuple = None, alert_thresholds: tuple = None) -> ObjectId:
		"""
		Register a type of data that will be inserted into the DB.
		Unregistered types or data that does not match the requirements will be rejected.
		
		:param name: The name of the data to be stored, for example "temperature".
		:param storage_type: What actual type the data is: `INT`, `FLOAT`, `STR` or an `ARRAY`, use the `StorageType` enum.
		:param array_type: If `storage_type` is ARRAY, a type of data must be set for said array. Cannot be `ARRAY`.
		:param unit: Optional unit like "seconds" or "s", as a string, to save with the rest of the data.
		:param valid_bounds: An optional tuple or list of 2 elements that dictates the minimum and maximum values, respectively,
		of the data to be stored. Both bounds are optional, being ignored if passed as `None`.
		:param alert_thresholds: An optional tuple or list of 2 elements for alerts. If an inserted value is outside of this range, an alert
		will be generated. Both bounds are optional, being ignored if passed as `None`.
		:return: The ObjectId for the type in the database.
		"""
		# ======================= #
		# Validate received values #
		# Check if type is in enum, will not be needed in Python 3.8
		if storage_type not in StorageType:
			raise TypeError(f'Invalid storage_type when registering type {name}')
		
		if storage_type is StorageType.ARRAY:
			if array_type is None:
				raise TypeError('If storage_type is ARRAY, array_type must be set')
			
			if array_type not in StorageType or array_type is StorageType.ARRAY:  # Same as above, check for enum won't be needed in 3.8
				raise TypeError('Invalid array storage type, must be INT, FLOAT or STR')

		# Verify bounds and alerts, except for strs
		if storage_type is not StorageType.STR:
			try:
				self._verify_bounds(valid_bounds, alert_thresholds, storage_type_dict[array_type if storage_type is StorageType.ARRAY else storage_type])
			except (ValueError, TypeError) as e:
				raise Exception(f'Invalid bounds or thresholds when registering type {name}') from e
		else:
			if valid_bounds is not None:
				valid_bounds = None
				database_logger.warning('Can\'t set bounds for STRs, they will be ignored')
			if alert_thresholds is not None:
				alert_thresholds = None
				database_logger.warning('Can\'t set alert thresholds for STRs, they will be ignored')
		# ======================= #
		
		# ======================= #
		# Check for similarities if needed #
		# TODO Better similarity check
		similar_names = 0
		if self.warn_similarities:
			similar_names = self._type_metadata.count_documents({'name': re.compile(name, re.IGNORECASE)})
		# ======================= #

		# ======================= #
		# Actual insert
		try:
			obj_id = self._type_metadata.insert_one({
				'name': name,
				'storage_type': storage_type.value,
				'unit': unit,
				'valid_bounds': valid_bounds,
				'alert_thresholds': alert_thresholds
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
		# ======================= #
		# Check client #
		client_info = None
		if isinstance(client, str):
			client_info = self._client_registry.find_one({'name': client})
		elif isinstance(client, ObjectId):
			client_info = self._client_registry.find_one({'_id': client})
		
		if not client_info:
			raise InvalidClient('Specified client has not been registered')
		# ======================= #
		
		# ======================= #
		# Get values from data #
		data_name = data.get('n') or data.get('name')
		if not data_name:
			raise InvalidData('Invalid data format: data name "n" or "name" not specified')
		
		data_value = data.get('v') or data.get('value')
		if not data_value:
			raise InvalidData('Invalid data format: data value "v" or "value" not specified')
		
		data_timestamp = data.get('t') or data.get('time')
		if not data_timestamp:
			raise InvalidData('Invalid data format: data value "v" or "value" not specified')
		
		try:
			data_datetime = self._parse_timestamp(data_timestamp)
		except InvalidData:
			raise
		
		# try:
		# 	datatype_info = self._type_metadata.find_one({'name': data_name})
		# 	client_data = self._data[client_info['_id']]
		# except KeyError:
		# 	raise InvalidData('Invalid data format')

	def query_data_client(self, client: Union[str, ObjectId], datatype: Union[str, ObjectId] = None,
	                      date_range: GDR = None) -> dict:
		pass

	def query_data_type(self, datatype: Union[str, ObjectId] = None, date_range: GDR = None) -> dict:
		pass
	
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
	def _verify_bounds(bounds, thresholds, expected_type):
		# Check that type isn't strings
		if expected_type is not int or expected_type is not float:
			raise TypeError('Bounds and Thresholds can only be ints or floats')
		
		# Check bounds object, types and values
		if bounds is not None and len(bounds) != 2:
			raise ValueError('Expected 2 values in bounds')
		
		if not (isinstance(bounds[0], expected_type) or bounds[0] is None) or \
		   not (isinstance(bounds[1], expected_type) or bounds[1] is None):
			raise TypeError(f'Types for bounds don\'t match with expected type {expected_type.__name__}')
		
		if bounds[0] is not None and bounds[1] is not None and bounds[0] >= bounds[1]:
			raise ValueError(f'Low bound {bounds[0]} cannot be higher than high bound {bounds[1]}')
		
		# Check thresholds object, types and values
		if thresholds is not None and len(thresholds) != 2:
			raise ValueError('Expected 2 values in thresholds')
		
		if not (isinstance(thresholds[0], expected_type) or thresholds[0] is None) or \
		   not (isinstance(thresholds[1], expected_type) or thresholds[1] is None):
			raise TypeError(f'Types for thresholds don\'t match with expected type {expected_type.__name__}')
		
		if thresholds[0] is not None and thresholds[1] is not None and thresholds[0] >= thresholds[1]:
			raise ValueError(f'Low alert threshold {thresholds[0]} cannot be higher than high alert threshold {thresholds[1]}')
		
		# Check if thresholds are valid
		if bounds is not None:
			if bounds[0] is not None:
				if thresholds[0] is not None and thresholds[0] < bounds[0] or \
				   thresholds[1] is not None and thresholds[1] < bounds[0]:
					raise ValueError(f'Alert thresholds can\'t be lower than low valid bound {bounds[0]}')
			
			if bounds[1] is not None:
				if thresholds[0] is not None and thresholds[0] > bounds[1] or \
				   thresholds[1] is not None and thresholds[1] > bounds[1]:
					raise ValueError(f'Alert thresholds can\'t be higher than high valid bound {bounds[1]}')

	@staticmethod
	def set_logging_level(level: Union[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]) -> None:
		database_logger.setLevel(level)


class InvalidData(Exception):
	"""Raised when the received data's format is invalid"""
	pass


class InvalidClient(Exception):
	"""Raised when an unregisted client is specified when sending data"""
	pass
