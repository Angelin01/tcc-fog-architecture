import fogcoap
from sys import argv


def main():
	database = 'fogcoap'
	uri = 'mongodb://localhost'
	if len(argv) >= 2:
		database = argv[1]
	
	if len(argv) >= 3:
		uri = argv[2]
	
	print(f'Connecting to database {database} on uri {uri}')
	dm = fogcoap.DataManager(database, uri)
	print('Starting broker')
	broker = fogcoap.Broker(dm)
	broker.run()
	print('Stopped')


if __name__ == '__main__':
	main()
