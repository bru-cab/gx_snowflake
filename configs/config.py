import os
from dotenv import load_dotenv
load_dotenv()

snowflake_conn_prop_local = {
   "account": os.environ.get('account'),
   "user": os.environ.get('user'),
   "password": os.environ.get('password'),
   "database": os.environ.get('database'),
   "schema": os.environ.get('schema'),
   "warehouse": os.environ.get('warehouse'),
   "role": os.environ.get('role'),
}