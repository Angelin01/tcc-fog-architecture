import aiocoap as coap
import asyncio
import json
from gzip import compress
from sys import argv, exit


async def main(host: str, uri: str, method: coap.codes = coap.GET, payload: dict = None):
	compressed_payload = compress(bytes(json.dumps(payload, separators=(',', ':'), force_ascii=True), 'ascii'), 9)

	protocol = await coap.Context.create_client_context()

	request = coap.Message(code=method, uri=f'coap://{host}/{uri}', payload=compressed_payload)

	try:
		response = await protocol.request(request).response
	except Exception as e:
		print('Failed to fetch resource:')
		print(e)
		exit(1)

	print(f'Response code: {response.code}'
	      f'Payload: {response.payload}')


if __name__ == '__main__':

	asyncio.get_event_loop().run_until_complete(main())