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
	def __init__(self, prohibit_insert: bool, abs_alert_thresholds: Optional[Tuple[Optional[float], Optional[float]]] = None,
	             interval_groups: Optional[List[Tuple[float, float]]] = None, array_treatment: Optional[ArrayTreatment] = None,
	             avg_deviation: Optional[float] = None, past_avg_count: Optional[int] = None):
		if abs_alert_thresholds is None and interval_groups is None and array_treatment is None and avg_deviation is None:
			raise ValueError('An AlertSpec instance must have at least one alert parameter set, simply use None in case you don\'t want alerts')
		
		if abs_alert_thresholds is not None:
			self.set_abs_thresholds(abs_alert_thresholds)
		else:
			self.abs_alert_thresholds = None
		
		self.interval_groups = None
		if interval_groups is not None:
			for group in interval_groups:
				self.add_interval(group)
		
		if avg_deviation is not None:
			self.set_avg_deviation(avg_deviation, past_avg_count)
		else:
			self.avg_deviation = None
			self.past_avg_count = None
		
		self.prohibit_insert = prohibit_insert
		if array_treatment not in ArrayTreatment:
			raise TypeError('array_treatment must be of type ArrayTreatment')
		
		self.array_treatment = array_treatment
	
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
	
	def set_abs_thresholds(self, abs_alert_thresholds: Tuple[Optional[float], Optional[float]]):
		if len(abs_alert_thresholds) != 2:
			raise ValueError('abs_alert_thresholds must have two values')
		elif abs_alert_thresholds[0] is None and abs_alert_thresholds[1] is None:
			raise ValueError('At least one of abs_alert_thresholds values must not be None')
		elif abs_alert_thresholds[0] is not None and abs_alert_thresholds[1] is not None and \
			abs_alert_thresholds[0] > abs_alert_thresholds[1]:
			raise ValueError(f'Low threshold value {abs_alert_thresholds[0]} cannot be higher than high value {abs_alert_thresholds[1]}')
		
		self.abs_alert_thresholds = abs_alert_thresholds
	
	def add_interval(self, interval_group: Tuple[float, float]):
		if len(interval_group) != 2:
			raise ValueError('An interval group must have two values')
		
		if interval_group[0] is None or interval_group[1] is None:
			raise ValueError('Both values for the interval group must be set')
		
		if self.interval_groups is None:
			self.interval_groups = [interval_group]
		else:
			self.interval_groups.append(interval_group)
	
	def set_avg_deviation(self, deviation: float, count: int):
		if deviation <= 0:
			raise ValueError('Deviation must be higher than 0')
		
		if count is None:
			raise ValueError('If avg_deviation is set, past_avg_count must also be set')
		elif count <= 0:
			raise ValueError('Past avg count must be higher than 0')
		
		self.avg_deviation = deviation
		self.past_avg_count = count


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
