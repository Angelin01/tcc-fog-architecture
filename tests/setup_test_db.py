from fogcoap import DatabaseManager, AlertSpec, StorageType
from fogcoap.alerts import ArrayTreatment
from pymongo import MongoClient
from pymongo.database import Database, Collection


class Register:
	def __init__(self, db_manager: DatabaseManager):
		self._db_manager = db_manager
	
	def register_pressure(self):
		print('Registering pressure')
		alert_spec = AlertSpec(
			False,
			abs_alert_thresholds=(None, 1000),
			array_treatment=ArrayTreatment.MAX
		)
		
		self._db_manager.register_datatype(
			name='pressure',
			storage_type=StorageType.ARRAY,
			array_type=StorageType.NUMBER,
			valid_bounds=(0, None),
			alert_spec=alert_spec
		)
		
	
	def register_water_level(self):
		print('Registering water_level')
	
	def register_volts(self):
		print('Registering volts')
	
	def register_temp(self):
		print('Registering temp')


def input_yn(msg):
	global all_yes
	if all_yes:
		return True
	
	answer = input(f'{msg} (y/[N]) ')
	return answer.lower() == 'y'


def main():
	uri = input('MongoDB uri to connect to: ') or 'mongodb://localhost'
	database = input('Database to connect to: ')

	db_manager = DatabaseManager(database, uri)
	client = MongoClient(uri)
	db: Database = client[database]
	type_metadata: Collection = db['type_metadata']
	
	if input_yn('Clean all data?'):
		print('Cleaning data')
		for coll in db.list_collection_names(filter={'name': {'$regex': 'data\.'}}):
			db.drop_collection(coll)
	
	registerer = Register(db_manager)
	datatypes = ['pressure', 'water_level', 'volts', 'temp']
	for datatype in datatypes:
		has_datatype = type_metadata.find_one({'name': datatype}) is not None
		if has_datatype and input_yn(f'Type "{datatype}" already registered, delete?'):
			print(f'Deleting type "{datatype}"')
			type_metadata.delete_one({'name': datatype})
		
		if not has_datatype:
			getattr(registerer, f'register_{datatype}')()

	db_manager.close()
	client.close()


if __name__ == '__main__':
	all_yes = False
	main()
