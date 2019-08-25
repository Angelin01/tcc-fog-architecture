import json
from datetime import datetime
from gzip import compress as gzcompress, decompress as gzdecompress
from aiocoap import Code, Message
from aiocoap.resource import Resource
from fogcoap import DatabaseManager, InvalidData


def _gzip_payload(func):
	# Decorator for gzip compression
	# Takes the request payload, decompresses (and checks for errors), calls the method and finally recompresses the response
	def wraps(self: BaseResource, request: Message):
		try:
			request.payload = gzdecompress(request.payload)
		except OSError:
			# Since the received message was not properly compressed, return a non compressed response just in case
			return Message(code=Code.BAD_REQUEST, payload=b'{"error":"Bad GZIP compression"}')
		
		response = func(self, request)
		response.payload = gzcompress(response.payload)
		return response
	
	return wraps


class BaseResource(Resource):
	"""
	Base resource class for other resource classes.
	Simply contains the database manager in `self._db_manager` and provides de `_build_msg` method.
	"""
	def __init__(self, db_manager: DatabaseManager):
		self._db_manager = db_manager
		super().__init__()
	
	@staticmethod
	def _build_msg(code: Code = None, data=None) -> Message:
		"""
		Prepares a `Message` object, adding code and dumping the json data
		"""
		if data is not None:
			payload = json.dumps(data, separators=(',', ':'), ensure_ascii=True).encode('ascii')
		else:
			payload = b''
		return Message(code=code, payload=payload)
	

class AllClientsResource(BaseResource):
	"""
	Provides a GET method that returns all registered clients.
	"""
	def __init__(self, db_manager: DatabaseManager):
		self._get_response = self._build_msg(data={
			client['name']: {key: value for (key, value) in client if key != 'name'}
			for client in db_manager.query_clients()
		})
		super().__init__(db_manager)
		
	@_gzip_payload
	def render_get(self, request: Message):
		"""
		Returns a json object where each key is the name of the client and it's value is another object with it's remaining attributes.
		"""
		return self._get_response


class AllDatatypesResource(BaseResource):
	"""
	Provides a GET method that returns all registered datatypes.
	"""
	
	def __init__(self, db_manager: DatabaseManager):
		self._get_response = self._build_msg(data={
			datatype['name']: {key: value for (key, value) in datatype if key != 'name'}
			for datatype in db_manager.query_datatypes()
		})
		super().__init__(db_manager)
	
	@_gzip_payload
	def render_get(self, request: Message):
		"""
		Returns a json object where each key is the name of the datatype and it's value is another object with it's remaining attributes.
		See `StorageType` for the enum values of the `storage_type` attribute.
		"""
		return self._get_response


class ClientResource(BaseResource):
	def __init__(self, name: str, db_manager: DatabaseManager):
		"""
		Simple class for a client.
		:param name: The client's registered name.
		:param db_manager: An instance of the database manager.
		"""
		self._name = name
		self._last_rcv_timestamp = 0
		super().__init__(db_manager)

	@_gzip_payload
	def render_get(self, request: Message):
		"""
		Get method for the client, getting data the client has sent.
		If sent with an empty payload, will simply return all data.
		If a payload is present, expects a gzip compressed json payload (preferably minified) with 3 optional keys:
		`nd` or `nodata`: a true or false value. If true, the request will not return any of the clients data.
		`d` or `datatype: name of a specific registered datatype. If set, will only return data from that datatype.
		`t` or `time`: an array with two values for a range of values between dates. Additionally, if the first value is null,
		               all data since the beginning until the second value is returned. Similarly, if the second value is null,
		               all data since the first value until now will be returned.
		               
		The following is a valid payload, assuming the datatype "temp" exists:
		```
		{
			"d": "temp",
			"t": [null, 1566687475]
		}
		
		Returns a gzip compressed json object containing 3 keys:
		`c`: the clients name.
		`l`: the UTC timestamp when the broker last received a message from the client,
		     will be 0 if the broker hasn't received a message since startup.
		`d`: the clients data, filtered according to parameters. `null` if `nd` was received with anything not interpreted as false.
		"""
		if len(request.payload) > 0:
			# Load the json
			try:
				parameters = json.loads(request.payload)
				if not isinstance(parameters, dict):
					return self._build_msg(code=Code.BAD_REQUEST, data={'error': 'Bad JSON format'})
			except (json.JSONDecodeError, UnicodeDecodeError):
				return self._build_msg(code=Code.BAD_REQUEST, data={'error': 'Bad JSON format'})
			
			# Verify parameters
			no_data = parameters.get('nd') or parameters.get('nodata')
			
			if not no_data:
				datatype = parameters.get('d') or parameters.get('datatype')
				timerange = parameters.get('t') or parameters.get('time')
				try:
					clients_data = self._db_manager.query_data_client(self._name, datatype, timerange)
				except (InvalidData, ValueError, TypeError) as e:
					return self._build_msg(code=Code.BAD_REQUEST, data={'error': str(e)})
			else:
				clients_data = None
		
		else:
			clients_data = self._db_manager.query_data_client(self._name)
		
		# Reformat data for response
		if clients_data is not None:
			for datatype in clients_data.values():
				for item in datatype:
					item['_id'] = str(item['_id'])
					item['datetime'] = int(item['datetime'].timestamp())
		
		return self._build_msg(data={
			'c': self._name,
			'l': self._last_rcv_timestamp,
			'd': clients_data
		})
	
	@_gzip_payload
	def render_post(self, request: Message):
		"""
		Post method for the client, for inserting data values.
		Expects a gzip compressed payload with a json object list (preferably minified), with each object containing 3 values:
		`n` or `name`: the name specified when registering a `datatype`.
		`v` or `value`: the actual value for the data.
		`t` or `time`: timestamp for when the data was collected, as an int timestamp or str iso formatted string (preferred UTC time)
		
		The following is a valid payload example, considering a datatype named "temp" that expects a number and
		a datatype named "volts" that expects an array of numbers:
		```
		[
			{"n": "temp", "v": 21.5, "t": "2019-01-01 10:0:0"},
			{"n": "volts", "v": [3.27, 3.26, 3.3], "t": 1546336800}
		]
		```
		
		Will insert each object one at a time. To avoid retransmissions, will still continue to insert objects even if there is an error
		in one or more.
		On at least one successful insert, the return code will be CHANGED. If no inserts were successful, the return code will be BAD_REQUEST.
		The response payload will be a json object list where each object will either contain the key `id` with the string representation of the
		inserted `ObjectId` or the key `error` with a message explaining the error, ordered by the same order of the inserted data.
		Clients should only retransmit data which had errors. Ideally, an error would never happen and a check for errors would be unnecessary.
		
		If the entire json could not be loaded a BAD_REQUEST will be returned with a simple json object payload `{"error", "Bad JSON format"}`.
		Similarly, if the loaded object is not a list, the error shall be `"JSON top object not an array"`.
		The response payload will be gzip compressed.
		"""
		# Load the json data
		try:
			data_list = json.loads(request.payload)
		except (json.decoder.JSONDecodeError, UnicodeDecodeError):
			return self._build_msg(code=Code.BAD_REQUEST, data={'error': 'Bad JSON format'})
		
		if not isinstance(data_list, list) or len(data_list) == 0:
			return self._build_msg(code=Code.BAD_REQUEST, data={'error': 'JSON top object not an array'})
		
		one_successful = False
		insert_status = []
		
		# Insert values
		for data in data_list:
			if not isinstance(data, dict):
				insert_status.append({'error': 'Bad JSON format'})
				continue
				
			try:
				obj_id = self._db_manager.insert_data(self._name, data)
			except InvalidData as e:
				insert_status.append({'error': str(e)})
			
			except Exception:
				from traceback import print_exc
				print_exc()
				insert_status.append({'error': 'Internal Server Error'})
				
			else:
				insert_status.append({'id': str(obj_id)})
				one_successful = True
				
		if one_successful:
			self._last_rcv_timestamp = int(datetime.utcnow().timestamp())
		
		return self._build_msg(code=Code.CHANGED if one_successful else Code.BAD_REQUEST,
		                       data=insert_status)
	
