from aiocoap import resource


class GenericResource(resource.Resource):
	"""
	Generic Resource for sending and getting data
	For now, should just print things as a test
	"""
	def __init__(self, structure: tuple):
		super().__init__()

	async def render_get(self, request):
		pass

	async def render_post(self, request):
		pass
