import pymongo
import re
import logging
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from bson.objectid import ObjectId
from datetime import datetime
from typing import Union, Tuple, NewType


GDR = NewType('GenDatetimeRange', Union[Tuple[str, str], Tuple[int, int], Tuple[datetime, datetime]])
database_logger = logging.Logger(__name__)


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
			database_logger.warning(f'Warning: there are {similar_names} clients with similar names to {client}')
		# ======================= #

		return obj_id

	def register_datatype(self, name: str, storage_type: Union[int, float, complex, str],
	                      unit: str = None, valid_bounds: tuple = None, alert_thresholds: tuple = None) -> bool:
		pass

	def insert_data(self, client: Union[str, ObjectId], data: dict) -> bool:
		pass

	def query_data_client(self, client: Union[str, ObjectId], datatype: Union[str, ObjectId] = None,
	                      date_range: GDR = None) -> dict:
		pass

	def query_data_type(self, datatype: Union[str, ObjectId] = None, date_range: GDR = None) -> dict:
		pass

	@staticmethod
	def set_logging_level(level: Union[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]) -> None:
		database_logger.setLevel(level)
