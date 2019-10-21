import aiocoap as coap
import asyncio
import json
from random import randint
from datetime import datetime
from typing import List
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
	

def update_data(data_list: List[dict]):
	timestamp = datetime.now().timestamp()
	for data in data_list:
		data['t'] = timestamp + randint(-2, 2)
		
		if data['n'] == 'pressure_1' or data['n'] == 'pressure_2':
			for i in range(len(data['v'])):
				data['v'][i] += randint(-250, 250) / 10
				
		elif data['n'] == 'water_level_1' or data['n'] == 'water_level_2':
			data['v'] += randint(-20, 20) / 10
		
		elif data['n'] == 'volts':
			for i in range(len(data['v'])):
				change = randint(0, 20) / 1000
				data['v'][i] += -change if data['v'][i] > 0 else change

		elif data['n'] == 'temp':
			data['v'] += randint(-20, 20) / 10
		
		elif data['n'] == 'location':
			continue
		
		else:
			raise ValueError('Unexpected datatype')
			

async def main(uri: str, priv_key: ec.EllipticCurvePrivateKey):
	protocol = await coap.Context.create_client_context()
	
	data = [
		{
			't': None,
			'n': 'pressure_1',
			'v': [500, 557, 601]
		},
		{
			't': None,
			'n': 'pressure_2',
			'v': [720, 557, 670]
		},
		{
			't': None,
			'n': 'water_level_1',
			'v': 12.5
		},
		{
			't': None,
			'n': 'water_level_2',
			'v': 32
		},
		{
			't': None,
			'n': 'volts',
			'v': [5, 5, -5]
		},
		{
			't': None,
			'n': 'temp',
			'v': 28
		},
		{
			't': None,
			'n': 'location',
			'v': 'Lat=-25.439058, Long=-49.268344'
		}
	]
	
	try:
		while True:
			update_data(data)
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
	if len(argv) != 3:
		print(f'Usage: {argv[0]} uri keyfile')
		exit(1)
	
	URI = argv[1].rstrip('/')
	
	try:
		with open(argv[5], 'rb') as key_file:
			PRIV_KEY = serialization.load_pem_private_key(key_file.read(), None, default_backend())
	except ValueError:
		print(f'{argv[5]} does not contain a valid PEM encoded ECC private key')
		exit(1)
	except FileNotFoundError:
		print(f'Could not find or read file {argv[2]}')
		exit(1)
	
	asyncio.run(main(URI, PRIV_KEY))
