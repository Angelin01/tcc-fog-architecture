import json
from enum import Enum
from typing import Union, Tuple, Optional, List
from aiocoap import Message
from aiocoap.resource import ObservableResource
from gzip import compress as gzcompress


class ArrayTreatment(Enum):
	"""
	Used to specify how an array of values should be treated for alerts
	INDIVIDUALLY means each value will be checked separately,
	MEAN checks the mean of all array values,
	SUM checks the sum all of all array values,
	MIN check the lowest value,
	MAX the highest value and
	MEDIAN takes the median value.
	"""
	INDIVIDUALLY = 0
	MEAN = 1
	SUM = 2
	MIN = 3
	MAX = 4
	MEDIAN = 5


class AlertSpec:
	def __init__(self, prohibit_insert: bool, abs_alert_thresholds: Optional[Tuple[Optional[float], Optional[float]]] = None,
	             interval_groups: Optional[List[Tuple[float, float]]] = None, array_treatment: Optional[ArrayTreatment] = None,
	             avg_deviation: Optional[Tuple[Optional[float], Optional[float]]] = None, past_avg_count: Optional[int] = None):
		"""
		An alert specification to be registered with a datatype.
		At least one alert specification must be supplied: either `abs_alert_thresholds`, `interval_groups` or `avg_deviation`.
		Additionally, if the `AlertSpec` is to be used with a datatype of the type ARRAY, `array_treatment` must be specified or the instance will not
		be suitable for use with said datatype.
		All alerts are optional, but at least one must be specified. Otherwise, simply use `None` when registering the datatype.
		:param prohibit_insert: Whether or not, when an alert is generated, an insert should be prohibited. This should be handled by the broker after
		                        receiving the database manager's response.
		:param abs_alert_thresholds: A tuple with two values. If any data received is OUTSIDE the range set by these two values, an alert with be
		                             generated. The first value is the lower bound and the second value the higher bound. If one of the values is None,
		                             only the other value will be considered as a lower bound.
		:param interval_groups: A list of tuples of two values which specify lower and higher bounds, similar to `abs_alert_thresholds`, except an
		                        alert will be generated if any data received is INSIDE the range specified by any group.
		:param array_treatment: How values of the type `ARRAY` must be treated. Must be an instance of `ArrayTreatment`.
		:param avg_deviation: A tuple of two float values, specifying how much the data can deviate from the average of the last `past_avg_count`
		                      values before generating an alert, as a percentage. Like other parameters, the first value specifies the lower bound and
		                      the second value the higher bound, both being optional. For example, if this is set to `(0.05, 0.1)` and
		                      `past_avg_count` is set to 15, an alert will be generated if the data is 5% below average or 10% above average of the
		                      last 15 received values.
		                      If this is set, `past_avg_count` must also be set.
		:param past_avg_count: How many of previous inserts should be used for the average. Note that until you have this many inserted values, the
		                       average deviation alert will not be generated.
		"""
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
	def from_dict(cls, dict_obj: dict):
		array_treatment = dict_obj.get('array_treatment')
		if array_treatment is not None:
			array_treatment = ArrayTreatment(array_treatment)
		
		return cls(
			prohibit_insert=dict_obj['prohibit_insert'],
			abs_alert_thresholds=dict_obj.get('abs_alert_thresholds'),
			interval_groups=dict_obj.get('interval_groups'),
			array_treatment=array_treatment,
			avg_deviation=dict_obj.get('avg_deviation'),
			past_avg_count=dict_obj.get('past_avg_count')
		)
	
	def to_dict(self) -> dict:
		obj = {'prohibit_insert': self.prohibit_insert}
		
		if self.abs_alert_thresholds is not None:
			obj['abs_alert_thresholds'] = self.abs_alert_thresholds
			
		if self.interval_groups is not None:
			obj['interval_groups'] = self.interval_groups
			
		if self.array_treatment is not None:
			obj['array_treatment'] = self.array_treatment.value
			
		if self.avg_deviation is not None and self.past_avg_count is not None:
			obj['avg_deviation'] = self.avg_deviation
			obj['past_avg_count'] = self.past_avg_count
		
		return obj
		
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
	
	def set_avg_deviation(self, deviation: Tuple[Optional[float], Optional[float]], count: int):
		if len(deviation) != 2:
			raise ValueError('Deviation must have two values')
		elif deviation[0] is None and deviation[1] is None:
			raise ValueError('At least one of the deviation values must not be None')
		
		if deviation[0] is not None and deviation[0] <= 0:
			raise ValueError('Deviation must be higher than 0')
		if deviation[1] is not None and deviation[1] <= 0:
			raise ValueError('Deviation must be higher than 0')
		
		if count is None:
			raise ValueError('If avg_deviation is set, past_avg_count must also be set')
		elif not isinstance(count, int):
			raise ValueError('Past avg count must be a positive int')
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
