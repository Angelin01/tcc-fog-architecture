import json
from enum import Enum
from typing import Union, Tuple, Optional, List
from aiocoap import Message
from aiocoap.resource import ObservableResource
from gzip import compress as gzcompress


class ArrayTreatment(Enum):
	INDIVIDUALLY = 0,
	AVG = 1,
	SUM = 2,
	MIN = 3,
	MAX = 4,
	MEDIAN = 5


class AlertSpec:
	def __init__(self):
		self.prohibit_insert = False
		self.abs_alert_thresholds: Optional[Tuple[Optional[int], Optional[int]]] = None
		self.interval_groups: Optional[List[Tuple[int, int]]] = None
		self.array_treatment: Optional[ArrayTreatment] = None
		self.avg_deviation: Optional[float] = None
		self.past_avg_count: Optional[int] = None
		
	@classmethod
	def from_json(cls, json_obj: Union[str, bytes]):
		pass
	
	@classmethod
	def from_dict(cls, dict_obj: dict):
		pass
	
	def to_json(self) -> str:
		pass
	
	def to_dict(self) -> dict:
		pass
	
	def set_abs_thresholds(self, min_value: int, max_values: int):
		pass
	
	def add_interval(self, min_value: int, max_values: int):
		pass
	
	def set_avg_deviation(self, deviation: float, count: int):
		pass


class ClientAlert(ObservableResource):
	def __init__(self):
		super().__init__()
		
		self._last_alert = b''
		
	def notify(self, alert):
		"""
		Called when you want to notify any subscribed clients of an alert.
		:param alert: The JSON dumpable alert, probably generated from the database manager.
		"""
		self._last_alert = gzcompress(json.dumps(alert, separators=(',', ':'), ensure_ascii=True).encode('ascii'))
		self.updated_state()
	
	async def render_get(self, request: Message):
		return Message(payload=self._last_alert)
