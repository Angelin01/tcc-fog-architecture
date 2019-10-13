import fogcoap


def main():
	print('Connecting to database')
	dm = fogcoap.DataManager('fogcoap')
	print('Starting broker')
	broker = fogcoap.Broker(dm)
	broker.run()
	print('Stopped')


if __name__ == '__main__':
	main()
