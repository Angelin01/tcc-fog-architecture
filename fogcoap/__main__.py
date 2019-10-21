import fogcoap
from sys import argv


def main():
	database = 'fogcoap'
	uri = 'mongodb://localhost'
	if len(argv) >= 3:
		database = argv[2]
	
	if len(argv) >= 4:
		uri = argv[3]
	
	print('Connecting to database')
	dm = fogcoap.DataManager(database, uri)
	print('Starting broker')
	broker = fogcoap.Broker(dm)
	broker.run()
	print('Stopped')


if __name__ == '__main__':
	main()
