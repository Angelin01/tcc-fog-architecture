import asyncio
from signal import SIGINT, SIGTERM
from aiocoap import Context
from aiocoap.resource import Site, WKCResource
from fogcoap.db_manager import DatabaseManager
from fogcoap.resources import ClientResource, DatatypeResource, AllClientsResource, AllDatatypesResource


class Broker:
	def __init__(self, db_manager: DatabaseManager, port: int = 5683):
		self._db_manager = db_manager
		self._port = port
		
		self._loop = None
		
		self._root = Site()
		self._setup_resources()
	
	def _setup_resources(self):
		self._root.add_resource(('.well-known', 'core'),
		                        WKCResource(self._root.get_resources_as_linkheader))
		self._setup_clients()
		self._setup_datatypes()
		
	def _setup_clients(self):
		self._root.add_resource(('clients',),
		                        AllClientsResource(self._db_manager))
		for client in self._db_manager.query_clients():
			self._root.add_resource(('client', client['name']),
			                        ClientResource(client['name'], self._db_manager))
		
	def _setup_datatypes(self):
		self._root.add_resource(('datatypes',),
		                        AllDatatypesResource(self._db_manager))
		for datatype in self._db_manager.query_datatypes():
			self._root.add_resource(('datatype', datatype['name']),
			                        DatatypeResource(datatype['name'], self._db_manager))
	
	def run(self):
		self._setup_resources()
		
		self._loop = asyncio.get_event_loop()
		self._loop.add_signal_handler(SIGTERM, self.stop)
		self._loop.add_signal_handler(SIGINT, self.stop)
		
		asyncio.Task(Context.create_server_context(self._root, bind=('::', self._port)))
		self._loop.run_forever()
	
	def stop(self, s=None, f=None):
		self._loop.stop()
		self._loop = None
