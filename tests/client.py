import aiocoap as coap
import asyncio
import json
from gzip import compress
from sys import argv, exit


async def main(host: str, uri: str, method: coap.Code = coap.GET, payload: dict = None):
	compressed_payload = compress(bytes(json.dumps(payload, separators=(',', ':'), force_ascii=True), 'ascii'), 9) if payload else ''

	protocol = await coap.Context.create_client_context()

	uri = f'coap://{host}/{uri}'

	request = coap.Message(code=method, uri=f'coap://{host}/{uri}', payload=compressed_payload)

	try:
		print(f'Sending request to {uri} with payload {compressed_payload}')
		response = await protocol.request(request).response
	except Exception as e:
		print('Failed to fetch resource:')
		print(e)
		exit(1)

	print(f'Response code: {response.code}'
	      f'Payload: {response.payload}')


if __name__ == '__main__':
	if not 2 < len(argv) < 6:
		print(f'Usage: {argv[0]} host uri [method] [payload json str|file]')
		exit(1)

	HOST = argv[1]
	if not isinstance(HOST, str):
		print('Invalid host, must be a str')
		exit(1)
	HOST = HOST.rstrip('/')

	URI = argv[2]
	if not isinstance(URI, str):
		print('Invalid uri, must be a str')
		exit(1)
	URI = URI.lstrip('/')

	METHOD = None
	if len(argv) >= 4:
		if argv[3] == 'get' or argv[3] == 'GET':
			METHOD = coap.GET
		elif argv[3] == 'post' or argv[3] == 'POST':
			METHOD = coap.POST
		elif argv[3] == 'put' or argv[3] == 'PUT':
			METHOD = coap.PUT
		elif argv[3] == 'delete' or argv[3] == 'DELETE':
			METHOD = coap.DELETE
		else:
			print(f'Invalid method {argv[3]}, use GET POST PUT or DELETE')
			exit(1)

	PAYLOAD = None
	if len(argv) >= 5:
		try:
			PAYLOAD = json.loads(argv[4])
		except ValueError:
			try:
				with open(argv[4]) as json_file:
					PAYLOAD = json_file.read()
			except FileNotFoundError:
				print(f'{argv[4]} is not a valid JSON string or JSON file')
				exit(1)

	asyncio.run(main(HOST, URI, METHOD, PAYLOAD))
