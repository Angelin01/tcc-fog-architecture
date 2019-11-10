import aiocoap as coap
import asyncio
import json
from datetime import datetime
from csv import reader
from os.path import isfile
from gzip import compress as gzcompress, decompress as gzdecompress
from sys import argv

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.hashes import SHA256


def get_payload(msg: str, priv_key: ec.EllipticCurvePrivateKey) -> bytes:
	compressed_msg = gzcompress(json.dumps(msg, separators=(',', ':'), ensure_ascii=True).encode('ascii'), 9)
	signature = priv_key.sign(compressed_msg, ec.ECDSA(SHA256()))
	return len(signature).to_bytes(2, 'big') + signature + compressed_msg
	
	
def get_next_data(csvreader: reader):
	line = next(csvreader)
	time = int(datetime.fromisoformat(line[1]).timestamp())
	line = line[2:]
	return [
		{
			't': time,
			'n': datatype,
			'v': int(value)
		} for datatype, value in zip(header, line)
	]


async def main(uri: str, csvreader: reader, priv_key: ec.EllipticCurvePrivateKey):
	protocol = await coap.Context.create_client_context()
	
	try:
		while True:
			data = get_next_data(csvreader)
			request = coap.Message(code=coap.POST, uri=uri, payload=get_payload(data, priv_key))
			print(f'Sending post to {uri} with data {data}')
			
			try:
				response = await protocol.request(request).response
			except Exception as e:
				print('Failed to fetch resource:')
				print(e)
				exit(1)
			
			print(f'Response code: {response.code}')
			print('Answers are:')
			answers = json.loads(gzdecompress(response.payload))
			for a in answers:
				print(a)
			
			await asyncio.sleep(15)

	except KeyboardInterrupt:
		exit(0)


if __name__ == '__main__':
	if len(argv) != 4:
		print(f'Usage: {argv[0]} uri csvfile keyfile')
		exit(1)
	
	URI = argv[1].rstrip('/')
	
	try:
		csvfile = open(argv[2])
		CSVREADER = reader(csvfile)
	except FileNotFoundError:
		print(f'Could not find or read file {argv[2]}')
		exit(1)
	
	header = next(CSVREADER)
	header = header[2:]
	
	try:
		with open(argv[3], 'rb') as key_file:
			PRIV_KEY = serialization.load_pem_private_key(key_file.read(), None, default_backend())
	except ValueError:
		print(f'{argv[3]} does not contain a valid PEM encoded ECC private key')
		exit(1)
	except FileNotFoundError:
		print(f'Could not find or read file {argv[3]}')
		exit(1)
	
	asyncio.run(main(URI, CSVREADER, PRIV_KEY))
