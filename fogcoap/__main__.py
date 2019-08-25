import fogcoap


def main():
	db = fogcoap.DatabaseManager('fogcoap')
	broker = fogcoap.Broker(db)
	broker.run()


if __name__ == '__main__':
	main()
