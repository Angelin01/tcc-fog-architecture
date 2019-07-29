import unittest
from fogcoap import db_manager


class MyTestCase(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		cls._db_manager = db_manager.DatabaseManager('unit_tests')
	
	@classmethod
	def tearDownClass(cls):
		cls._db_manager.destroy()


if __name__ == '__main__':
	unittest.main()
