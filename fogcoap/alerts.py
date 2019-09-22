import asyncio
from datetime import datetime
from aiocoap import Code, Message
from aiocoap.resource import ObservableResource


class ClientAlert(ObservableResource):
	def __init__(self):
		super().__init__()
		
		self._should_alert = False
		self._loop = asyncio.get_event_loop()
		self._handle = None
		
	def notify(self):
		self.updated_state()
		self._handle = self._loop.call_later(5, self.notify)
	
	def update_observation_count(self, new_count: int):
		if new_count > 0 and self._handle is None:
			print('Restarting clock')
			self._handle = self._loop.call_later(5, self.notify)
			
		elif new_count == 0 and self._handle is not None:
			print('Stopping clock')
			self._handle.cancel()
			self._handle = None
	
	async def render_get(self, request: Message):
		payload = str(int(datetime.utcnow().timestamp())).encode('ascii')
		return Message(payload=payload)
