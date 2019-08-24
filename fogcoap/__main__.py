import fogcoap


def main():
	db = fogcoap.DatabaseManager('fogcoap')
	broker = fogcoap.Broker(db)
	broker.run()
	print('After broker run, shouldn\'t happen')


if __name__ == '__main__':
	main()
