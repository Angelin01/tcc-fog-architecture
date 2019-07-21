import pymongo
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId
from datetime import datetime
from typing import Union, Tuple, NewType


GDR = NewType('GenDatetimeRange', Union[Tuple[str, str], Tuple[int, int], Tuple[datetime, datetime]])


class DatabaseManager:
	_ClientRegistry = 'client_registry'
	_TypeMetadata = 'type_metadata'
	_Data = 'data'

	def __init__(self, database: str, host: str = 'localhost', port: int = 27017) -> None:
		self._client = pymongo.MongoClient(host, port)
		try:
			# The ismaster command is cheap and does not require auth.
			self._client.admin.command('ismaster')
		except ConnectionFailure:
			print(f'Database connection to {host}:{port} failed', flush=True)
			raise

		self._database = self._client[database]
		self._client_registry = self._database[self._ClientRegistry]
		self._type_metadata = self._database[self._TypeMetadata]
		self._data = self._database[self._Data]

	def register_client(self, client: str) -> bool:
		if self._client_registry.count_documents({'name': client}, limit=1) != 0:
			self._client_registry.insert_one({'name': client})

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
