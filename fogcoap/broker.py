import json
import aiocoap.resource as resource
from aiocoap.numbers.codes import Code
from aiocoap.message import Message
from .db_manager import DatabaseManager, InvalidData


class ClientResource(resource.Resource):
	def __init__(self, name: str, db_manager: DatabaseManager):
		self._name = name
		self._db_manager = db_manager
		super().__init__()
		
	@staticmethod
	def _build_msg(code: Code = None, data: dict = None) -> Message:
		return Message(code=code, payload=json.dumps(data, separators=(',', ':'), ensure_ascii=True).encode('ascii') if data is not None else b'')

	def render_get(self, request: Message):
		pass
	
	def render_post(self, request: Message):
		try:
			obj_id = self._db_manager.insert_data(self._name, json.loads(request.payload))
		except json.decoder.JSONDecodeError:
			return self._build_msg(code=Code.BAD_REQUEST, data={'error': 'Bad JSON format'})
		except InvalidData as e:
			return self._build_msg(code=Code.BAD_REQUEST, data={'error': str(e)})
		except Exception:
			from traceback import print_exc
			print_exc()
			return self._build_msg(code=Code.INTERNAL_SERVER_ERROR)
		
		return self._build_msg(data={'id': str(obj_id)})


class Broker:
	def __init__(self, db_manager: DatabaseManager, port: int = 5683):
		self._db_manager = db_manager
		self._port = port
		self._root = resource.Site()
		self._root.add_resource(('.well-known', 'core'),
		                        resource.WKCResource(self._root.get_resources_as_linkheader))
		self._setup_resources()
		
	def _setup_resources(self):
		clients = self._db_manager.query_clients()
		pass
	
	def run(self):
		pass
	
	def stop(self):
		pass
