import json
from aiocoap import Message
from aiocoap.resource import ObservableResource
from gzip import compress as gzcompress


class ClientAlert(ObservableResource):
	def __init__(self):
		super().__init__()
		
		self._last_alert = b''
		
	def notify(self, alert: dict):
		self._last_alert = gzcompress(json.dumps(alert, separators=(',', ':'), ensure_ascii=True).encode('ascii'))
		self.updated_state()
	
	async def render_get(self, request: Message):
		return Message(payload=self._last_alert)
