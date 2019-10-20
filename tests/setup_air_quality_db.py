from os import makedirs, path
from fogcoap import DataManager, AlertSpec, StorageType
from fogcoap.alerts import ArrayTreatment
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from pymongo import MongoClient
from pymongo.database import Database, Collection


class Register:
	def __init__(self, data_manager: DataManager):
		self._data_manager = data_manager
	
	def register_pressure_1(self):
		print('Registering pressure')
		alert_spec = None
		
		self._data_manager.register_datatype(
			name='pressure',
			storage_type=StorageType.NUMBER,
			valid_bounds=(0, None),
			alert_spec=alert_spec
		)
	
	def register_humidity(self):
		print('Registering humidity')
		alert_spec = AlertSpec(
			False,
			abs_alert_thresholds=(20, 95),
			array_treatment=ArrayTreatment.MIN
		)
		
		self._data_manager.register_datatype(
			name='humidity',
			storage_type=StorageType.ARRAY,
			array_type=StorageType.NUMBER,
			valid_bounds=(0, 100),
			alert_spec=alert_spec
		)
	
	def register_temp(self):
		print('Registering temp')
		alert_spec = AlertSpec(
			False,
			abs_alert_thresholds=(-5, 25),
			avg_deviation=(0.25, 0.25),
			past_avg_count=5
		)
		
		self._data_manager.register_datatype(
			name='temp',
			storage_type=StorageType.NUMBER,
			valid_bounds=(-273.15, None),
			alert_spec=alert_spec
		)
	
	def register_pm1(self):
		print('Registering pm1')
		alert_spec = AlertSpec(
			False,
			abs_alert_thresholds=(None, 60),
			array_treatment=ArrayTreatment.MEAN
		)
		
		self._data_manager.register_datatype(
			name='pm1',
			storage_type=StorageType.ARRAY,
			array_type=StorageType.NUMBER,
			valid_bounds=(0, None),
			alert_spec=alert_spec
		)
	
	def register_pm25(self):
		print('Registering pm2.5')
		alert_spec = AlertSpec(
			False,
			abs_alert_thresholds=(None, 100),
			array_treatment=ArrayTreatment.MEAN
		)
		
		self._data_manager.register_datatype(
			name='pm25',
			storage_type=StorageType.ARRAY,
			array_type=StorageType.NUMBER,
			valid_bounds=(0, None),
			alert_spec=alert_spec
		)
	
	def register_pm10(self):
		print('Registering pm10')
		alert_spec = AlertSpec(
			False,
			abs_alert_thresholds=(None, 140),
			array_treatment=ArrayTreatment.MEAN
		)
		
		self._data_manager.register_datatype(
			name='pm10',
			storage_type=StorageType.ARRAY,
			array_type=StorageType.NUMBER,
			valid_bounds=(0, None),
			alert_spec=alert_spec
		)


def input_yn(msg):
	global all_yes
	if all_yes:
		return True
	
	answer = input(f'{msg} (y/[N]) ')
	return answer.lower() == 'y'


def main():
	uri = input('MongoDB uri to connect to: ') or 'mongodb://localhost'
	database = input('Database to connect to: ')
	
	data_manager = DataManager(database, uri)
	client = MongoClient(uri)
	db: Database = client[database]
	type_metadata: Collection = db['type_metadata']
	client_registry: Collection = db['client_registry']
	
	# ============================= #
	# Clean existing data
	# ============================= #
	
	if input_yn('Clean all data?'):
		print('Cleaning data')
		for coll in db.list_collection_names(filter={'name': {'$regex': 'data\.'}}):
			db.drop_collection(coll)
	
	# ============================= #
	# Register datatypes
	# ============================= #
	
	registerer = Register(data_manager)
	datatypes = ['pressure', 'humidity', 'temp', 'pm1', 'pm25', 'pm10']
	for datatype in datatypes:
		has_datatype = type_metadata.find_one({'name': datatype}) is not None
		if has_datatype and input_yn(f'Type "{datatype}" already registered, delete? '):
			print(f'Deleting type "{datatype}"')
			type_metadata.delete_one({'name': datatype})
			has_datatype = False
		
		if not has_datatype:
			getattr(registerer, f'register_{datatype}')()
	
	# ============================ #
	# Register clients
	# ============================ #
	
	num_clients = int(input('How many clients do you want to register? '))
	if num_clients > 0:
		prefix = input('Choose a client name prefix: ') or 'client'
		output_dir = path.abspath(path.join(path.abspath(__file__), path.pardir, 'keys'))
		makedirs(output_dir, exist_ok=True)
		
		for i in range(num_clients):
			client_name = f'{prefix}{i}'
			has_client = client_registry.find_one({'name': client_name})
			if has_client and input_yn(f'Client {client_name} already registered, delete? '):
				print(f'Deleting client {client_name}')
				client_registry.delete_one({'name': client_name})
				has_client = False
			
			if not has_client:
				private_key = ec.generate_private_key(ec.SECP384R1, default_backend())
				public_key_serialized = private_key.public_key().public_bytes(encoding=serialization.Encoding.PEM,
				                                                              format=serialization.PublicFormat.SubjectPublicKeyInfo)
				private_key_serialized = private_key.private_bytes(encoding=serialization.Encoding.PEM,
				                                                   format=serialization.PrivateFormat.PKCS8,
				                                                   encryption_algorithm=serialization.NoEncryption())
				
				data_manager.register_client(client_name, public_key_serialized)
				with open(path.join(output_dir, f'{client_name}_priv.pem'), 'wb') as key:
					key.write(private_key_serialized)
				
				print(f'Registered client {client_name}')
		
		print(f'Finished registering clients, keys have been placed in {output_dir}{path.sep}[CLIENT_NAME]_priv.pem')
	
	data_manager.close()
	client.close()


if __name__ == '__main__':
	all_yes = False
	main()
