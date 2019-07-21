import pymongo
from bson.objectid import ObjectId
from datetime import datetime
from typing import Union, Tuple, NewType


GDR = NewType('GenDatetimeRange', Union[Tuple[str, str], Tuple[int, int], Tuple[datetime, datetime]])


class DatabaseManager:
	def __init__(self, database: str) -> None:
		pass

	def register_client(self, client: str) -> bool:
		pass

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
