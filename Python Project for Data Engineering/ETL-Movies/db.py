import pandas as pd
import sqlite3

table_name = 'INSTRUCTOR'
table_name1 = 'Departments'

df = pd.read_csv('INSTRUCTOR.csv', names=['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE'])
df1 = pd.read_csv('Departments.csv', names=['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID'])

connection = sqlite3.connect('STAFF.db')

df.to_sql(table_name, connection, if_exists='replace', index=False)
df1.to_sql(table_name1, connection, if_exists='replace', index=False)
print('Table is Ready')

print('\n\n')

print('INSTRUCTOR Queries')
query = f'select * from {table_name}'
output = pd.read_sql(query, connection)
print(query)
print(output)

query = f'select FNAME from {table_name}'
output = pd.read_sql(query, connection)
print(query)
print(output)

query = f'select count(*) from {table_name}'
output = pd.read_sql(query, connection)
print(query)
print(output)

data_dict = {
    'ID': [100],
    'FNAME': ['John'],
    'LNAME': ['Doe'],
    'CITY': ['Paris'],
    'CCODE': ['FR']
}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, connection, if_exists='append', index=False)
print('Data appended successfully for INSTRUCTOR table')

query = f'select count(*) from {table_name}'
output = pd.read_sql(query, connection)
print(query)
print(output)

print('\n\n')

print('Departments Queries')

data_dict1 = {
    'DEPT_ID': [9],
    'DEP_NAME': ['Quality Assurance'],
    'MANAGER_ID': [30010],
    'LOC_ID': ['L0010']
}
data_append1 = pd.DataFrame(data_dict1)
data_append1.to_sql(table_name1, connection, if_exists='append', index=False)
print('Data appended successfully for Departments table')

query = f'select * from {table_name1}'
output = pd.read_sql(query, connection)
print(query)
print(output)

query = f'select DEP_NAME from {table_name1}'
output = pd.read_sql(query, connection)
print(query)
print(output)

query = f'select count(*) from {table_name1}'
output = pd.read_sql(query, connection)
print(query)
print(output)

connection.close()

