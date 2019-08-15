import aiocoap.resource as resource
from .db_manager import DatabaseManager


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
