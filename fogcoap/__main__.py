import asyncio
import aiocoap as coap
import aiocoap.resource as resource
from fogcoap.gen_resource import GenericResource


def main():
	# Resource tree creation
	root = resource.Site()

	root.add_resource(('.well-known', 'core'),
                       resource.WKCResource(root.get_resources_as_linkheader))
	root.add_resource(('test',), GenericResource())

	asyncio.Task(coap.Context.create_server_context(root))

	asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
	main()