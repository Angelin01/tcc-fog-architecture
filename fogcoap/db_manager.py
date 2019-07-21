import pymongo
import re
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from bson.objectid import ObjectId
from datetime import datetime
from typing import Union, Tuple, NewType


GDR = NewType('GenDatetimeRange', Union[Tuple[str, str], Tuple[int, int], Tuple[datetime, datetime]])


class DatabaseManager:
	_ClientRegistry = 'client_registry'
	_ClientNameIndex = 'name_index'
	_TypeMetadata = 'type_metadata'
	_Data = 'data'

	def __init__(self, database: str, host: str = 'localhost', port: int = 27017, warn_similarities: bool = True) -> None:
		self._client = pymongo.MongoClient(host, port)
		try:
			# The ismaster command is cheap and does not require auth.
			self._client.admin.command('ismaster')
		except ConnectionFailure:
			print(f'Database connection to {host}:{port} failed', flush=True)
			raise
		self._database = self._client[database]

		self._client_registry = self._database[self._ClientRegistry]
		self._client_registry.create_index('name', name=self._ClientNameIndex, unique=True)

		self._type_metadata = self._database[self._TypeMetadata]
		self._data = self._database[self._Data]
		self.warn_similarities = warn_similarities

	def register_client(self, client: str) -> ObjectId:
		similar_names = 0
		if self.warn_similarities:
			similar_names = self._client_registry.count_documents({'name': re.compile(client, re.IGNORECASE)})

		obj_id = self._client_registry.insert_one({'name': client}).inserted_id
		if similar_names > 0:
			print(f'Warning: there are {similar_names} clients with similar names to {client}')

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
