import fogcoap


def main():
	db = fogcoap.DatabaseManager('fogcoap')
	broker = fogcoap.Broker(db)


if __name__ == '__main__':
	main()
