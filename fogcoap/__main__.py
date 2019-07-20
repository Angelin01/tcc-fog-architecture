import asyncio
from aiocoap import resource, Context


def main():
	root = resource.Site()

	root.add_resource(('.well-known', 'core'),
	                  resource.WKCResource(root.get_resources_as_linkheader))

	asyncio.Task(Context.create_server_context(root))
	asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
	main()
