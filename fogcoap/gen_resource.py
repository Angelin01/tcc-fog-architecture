from aiocoap import resource, Message


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
		print(f'Returning current content: {self.content}')
		return Message(payload=self.content)

	async def render_post(self, request):
		pass
