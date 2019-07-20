import json
from gzip import compress, decompress
import aiocoap as coap
import aiocoap.resource as resource


class GenericResource(resource.Resource):
	"""
	Generic Resource for sending and getting data
	For now, should just print things as a test
	"""
	def __init__(self, structure: tuple):
		super().__init__()
		self._content = b''

	async def render_get(self, request):
		print(f'Received request of type {type(request)}')
		print(request)
		print(f'Returning current content: {self._content}')
		return coap.Message(payload=self._content)

	async def render_post(self, request):
		print(f'Received request of type {type(request)}')
		print(request)
		self._content = request.payload
		return coap.Message(code=coap.CHANGED)
