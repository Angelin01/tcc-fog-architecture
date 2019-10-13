import asyncio
from signal import SIGINT, SIGTERM
from aiocoap import Context
from typing import Union
from aiocoap.resource import Site, WKCResource, Resource, ObservableResource
from fogcoap.data_manager import DataManager
from fogcoap.resources import ClientResource, DatatypeResource, ListClientsResource, ListDatatypesResource, AllData
from fogcoap.alerts import ClientAlert


class Broker:
	def __init__(self, db_manager: DataManager, port: int = 5683):
		self._db_manager = db_manager
		self._port = port
		
		self._loop = None
		self._root = Site()
	
	def add_topic(self, path: tuple, instance: Union[Resource, ObservableResource]):
		"""
		Adds a topic to the broker.
		Should be called before starting the broker.
		:param path: A tuple separated path to the topic. For example, `('some', 'path')` will turn into `coap://yourdomain/some/path`.
		:param instance: An instance of aiocoap's resources or something that inherits from it.
		"""
		self._root.add_resource(path, instance)
	
	def _setup_resources(self):
		self.add_topic(('.well-known', 'core'), WKCResource(self._root.get_resources_as_linkheader))
		self.add_topic(('alldata',), AllData(self._db_manager))
		
		self._setup_clients()
		self._setup_datatypes()
		
	def _setup_clients(self):
		self.add_topic(('list', 'clients'), ListClientsResource(self._db_manager))
		
		for client in self._db_manager.query_clients():
			alert_resource = ClientAlert()
			self.add_topic(('alert', client['name']), alert_resource)
			self.add_topic(('client', client['name']),  ClientResource(client['name'], client['ecc_public_key'], self._db_manager, alert_resource))
		
	def _setup_datatypes(self):
		self.add_topic(('list', 'datatypes'),  ListDatatypesResource(self._db_manager))
		
		for datatype in self._db_manager.query_datatypes():
			self.add_topic(('datatype', datatype['name']), DatatypeResource(datatype['name'], self._db_manager))
	
	def run(self):
		self._setup_resources()
		
		self._loop = asyncio.get_event_loop()
		self._loop.add_signal_handler(SIGTERM, self.stop)
		self._loop.add_signal_handler(SIGINT, self.stop)
		
		asyncio.Task(Context.create_server_context(self._root, bind=('::', self._port)))
		self._loop.run_forever()
	
	def stop(self, s=None, f=None):
		self._loop.stop()
		self._db_manager.close()
		self._loop = None
