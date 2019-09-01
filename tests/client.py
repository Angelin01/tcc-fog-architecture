import aiocoap as coap
import asyncio
import json
from gzip import compress, decompress
from sys import argv, exit
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.hashes import SHA256


async def main(host: str, resource: str, method: coap.Code = coap.GET, payload: dict = None, priv_key: ec.EllipticCurvePrivateKey = None):
	compressed_payload = compress(json.dumps(payload, separators=(',', ':'), ensure_ascii=True).encode('ascii'), 9) if payload else ''
	
	# Sign message if a key was passed
	if priv_key is not None:
		signature = priv_key.sign(compressed_payload, SHA256())
		compressed_payload = len(signature).to_bytes(2, 'big') + signature + compressed_payload
	
	protocol = await coap.Context.create_client_context()

	uri = f'coap://{host}/{resource}'

	request = coap.Message(code=method, uri=uri, payload=compressed_payload)

	print(f'Sending request to {uri} with method {method} and payload "{compressed_payload}"')

	try:
		response = await protocol.request(request).response
	except Exception as e:
		print('Failed to fetch resource:')
		print(e)
		exit(1)

	print(f'Response code: {response.code}\n'
	      f'Payload: {decompress(response.payload)}')


if __name__ == '__main__':
	if not 3 <= len(argv) <= 6:
		print(f'Usage: {argv[0]} host uri [method] [payload json str|file] [keyfile]')
		exit(1)

	HOST = argv[1]
	if not isinstance(HOST, str):
		print('Invalid host, must be a str')
		exit(1)
	HOST = HOST.rstrip('/')

	RESOURCE = argv[2]
	if not isinstance(RESOURCE, str):
		print('Invalid resource, must be a str')
		exit(1)
	RESOURCE = RESOURCE.lstrip('/')

	METHOD = coap.GET
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
				
	PRIV_KEY = None
	if len(argv) >= 6:
		try:
			with open(argv[5]) as key_file:
				PRIV_KEY = serialization.load_pem_private_key(key_file.read(), None, default_backend())
		except ValueError:
			print(f'{argv[5]} does not contain a valid PEM encoded ECC private key')
		except FileNotFoundError:
			print(f'Could not find or read file {argv[5]}')

	asyncio.run(main(HOST, RESOURCE, METHOD, PAYLOAD, PRIV_KEY))
