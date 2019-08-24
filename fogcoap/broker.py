import asyncio
import json
from datetime import datetime
from aiocoap import Code, Message, Context
from aiocoap.resource import Site, WKCResource, Resource
from fogcoap.db_manager import DatabaseManager, InvalidData


class ClientResource(Resource):
	def __init__(self, name: str, db_manager: DatabaseManager):
		self._name = name
		self._db_manager = db_manager
		self._last_rcv_timestamp = 0
		super().__init__()
		
	@staticmethod
	def _build_msg(code: Code = None, data=None) -> Message:
		return Message(code=code, payload=json.dumps(data, separators=(',', ':'), ensure_ascii=True).encode('ascii') if data is not None else b'')

	def render_get(self, request: Message):
		"""
		Get method for the client, getting data the client has sent.
		If sent with an empty payload, will simply return all data.
		If a payload is present, expects a json payload (preferably minified) with 3 optional keys:
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
		
		Returns an object containing 3 keys:
		`c`: the clients name.
		`l`: the UTC timestamp when the broker last received a message from the client,
		     will be 0 if the broker hasn't received a message since startup.
		`d`: the clients data, filtered according to parameters. `null` if `nd` was received with anything not interpreted as false.
		"""
		if len(request.payload) > 0:
			try:
				parameters = json.loads(request.payload)
			except (json.JSONDecodeError, UnicodeDecodeError):
				return self._build_msg(code=Code.BAD_REQUEST, data={'error': 'Bad JSON format'})
			
			no_data = parameters.get('nd') or parameters.get('nodata')
			
			if not no_data:
				datatype = parameters.get('d') or parameters.get('datatype')
				timerange = parameters.get('t') or parameters.get('time')
				try:
					clients_data = self._db_manager.query_data_client(self._name, datatype, timerange)
				except (InvalidData, ValueError) as e:
					return self._build_msg(code=Code.BAD_REQUEST, data={'error': str(e)})
			else:
				clients_data = None
		
		else:
			clients_data = self._db_manager.query_data_client(self._name)
		
		return self._build_msg(data={
			'c': self._name,
			'l': self._last_rcv_timestamp,
			'd': clients_data
		})
	
	def render_post(self, request: Message):
		"""
		Post method for the client, for inserting data values.
		Expects a payload with a json object list (preferably minified), with each object containing 3 values:
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
		"""
		try:
			data_list = json.loads(request.payload)
		except (json.decoder.JSONDecodeError, UnicodeDecodeError):
			return self._build_msg(code=Code.BAD_REQUEST, data={'error': 'Bad JSON format'})
		
		if not isinstance(data_list, list) or len(data_list) == 0:
			return self._build_msg(code=Code.BAD_REQUEST, data={'error': 'JSON top object not an array'})
		
		one_successful = False
		insert_status = []
		
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


class Broker:
	def __init__(self, db_manager: DatabaseManager, port: int = 5683):
		self._db_manager = db_manager
		self._port = port
		self._root = Site()
		self._root.add_resource(('.well-known', 'core'),
		                        WKCResource(self._root.get_resources_as_linkheader))
		self._setup_resources()
	
	def _setup_resources(self):
		self._setup_clients()
		self._setup_datatypes()
		
	def _setup_clients(self):
		for client in self._db_manager.query_clients():
			self._root.add_resource(('client', client['name']),
			                        ClientResource(client['name'], self._db_manager))
		
	def _setup_datatypes(self):
		pass
	
	def run(self):
		self._setup_resources()
		asyncio.Task(Context.create_server_context(self._root, bind=('::', self._port)))
		asyncio.get_event_loop().run_forever()
	
	def stop(self):
		pass
