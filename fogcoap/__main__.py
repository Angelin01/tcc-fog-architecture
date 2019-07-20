import asyncio
from aiocoap import resource, Context
from fogcoap.gen_resource import GenericResource


def main():
	root = resource.Site()

	root.add_resource(('.well-known', 'core'),
	                  resource.WKCResource(root.get_resources_as_linkheader))
	root.add_resource(('generic', 'resource'),
	                  GenericResource(('something',)))

	asyncio.Task(Context.create_server_context(root))
	asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
	main()
