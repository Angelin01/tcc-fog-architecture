# tcc-fog-architecture
Course completion project for UTFPR's computer engineering bachelor's degree


# Usage

Install the requirements using `pip3 install -r requirements.txt`, preferably in a virtual environment.  
The module itself can be called using `python3 -m fogcoap [database] [mongo_uri]`, where the two parameters default to `'fogcoap'`and `'mongodb://localhost'` if not supplied.

Alternatively, following the example of the main code, you can create your own aplication:

```py
import fogcoap
from sys import argv


def main():
	database = 'fogcoap'
	uri = 'mongodb://localhost'
	if len(argv) >= 2:
		database = argv[1]
	
	if len(argv) >= 3:
		uri = argv[2]
	
	dm = fogcoap.DataManager(database, uri)
	broker = fogcoap.Broker(dm)
	broker.run()


if __name__ == '__main__':
	main()
```

You can stop the broker by calling it's stop function. Beware, it's run function is blocking, therefore you will need to thread it if you intend to stop it this way. The broker will also stop running when receiving a SIGINT (usually supplied by CTRL + C) or a SIGTERM, which is the preferred mode of operation.
