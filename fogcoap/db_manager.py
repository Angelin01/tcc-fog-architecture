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
		# Check if type is in enum, will not be needed in Python 3.8
		if storage_type not in StorageType:
			raise TypeError(f'Invalid storage_type when registering type {name}')
		
		if storage_type is StorageType.ARRAY:
			if array_type is None:
				raise TypeError('If storage_type is ARRAY, array_type must be set')
			
			if array_type not in StorageType or array_type is StorageType.ARRAY:  # Same as above, check for enum won't be needed in 3.8
				raise TypeError('Invalid array storage type, must be INT, FLOAT or STR')

		# Verify bounds and alerts
		try:
			self._verify_bounds(valid_bounds, alert_thresholds, storage_type_dict[array_type if storage_type is StorageType.ARRAY else storage_type])
		except (ValueError, TypeError) as e:
			raise Exception(f'Invalid bounds when registering type {name}') from e
		
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
		
		# Verify if no bounds were set for strs
		if storage_type is StorageType.STR and \
		   (valid_bounds is not None or alert_thresholds is not None):
			database_logger.warning(f'Bounds were set for type {name} but type is STR, they will be ignored')
		
		# Warn for similarities if necessary #
		if similar_names > 0:
			database_logger.warning(f'There are {similar_names} types with similar names to {name}')
		# ======================= #
	
		return obj_id

	def insert_data(self, client: Union[str, ObjectId], data: dict) -> bool:
		pass

	def query_data_client(self, client: Union[str, ObjectId], datatype: Union[str, ObjectId] = None,
	                      date_range: GDR = None) -> dict:
		pass

	def query_data_type(self, datatype: Union[str, ObjectId] = None, date_range: GDR = None) -> dict:
		pass
	
	@staticmethod
	def _verify_bounds(bounds, thresholds, expected_type):
		# Check bounds first
		if bounds is not None and len(bounds) != 2:
			raise ValueError('Expected 2 values in bounds')
		
		if not (isinstance(bounds[0], expected_type) or bounds[0] is None) or \
		   not (isinstance(bounds[1], expected_type) or bounds[1] is None):
			raise TypeError(f'Types for bounds don\'t match with expected type {expected_type.__name__}')
		
		# Check thresholds
		if thresholds is not None and len(thresholds) != 2:
			raise ValueError('Expected 2 values in thresholds')
		
		if not (isinstance(thresholds[0], expected_type) or thresholds[0] is None) or \
		   not (isinstance(thresholds[1], expected_type) or thresholds[1] is None):
			raise TypeError(f'Types for thresholds don\'t match with expected type {expected_type.__name__}')
		
		
		
		

	@staticmethod
	def set_logging_level(level: Union[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]) -> None:
		database_logger.setLevel(level)
