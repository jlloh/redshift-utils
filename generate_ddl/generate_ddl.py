import os
import psycopg2
import ConfigParser

#---------------------------------------------------
#Inputs here:
schema = 'cia'
tablename = 'fraud_booking_v2_flags'
distkeyname = 'driver_id'

#---------------------------------------------------

config=ConfigParser.RawConfigParser()
config.read(os.path.expanduser('~/.config.cfg'))
  
username=config.get('main_cluster','user')
password=config.get('main_cluster','password')
dbname=config.get('main_cluster','database')
host=config.get('main_cluster','host')
port=config.get('main_cluster','port')

con=psycopg2.connect(dbname=dbname,user=username,host=host
                    ,password=password,port=port)
                    
cur=con.cursor()
                    
query = '''
SET SEARCH_PATH TO %(schema)s;
SELECT "column", "type", encoding, distkey, sortkey, "notnull"
  FROM pg_table_def
 WHERE tablename = %(tablename)s;
'''

parameters = {'schema': schema,'tablename': tablename}

cur.execute(query,parameters)

colnames = []
distkeys = []
sortkeys = {}

for row in cur.fetchall():
	column, type, encoding, distkey, sortkey, not_null = row
	colnames.append((column,type,encoding,not_null))
	distkeys.append(distkey)
	if sortkey != None:
		sortkeys[sortkey]=column

ddl = '''
CREATE TABLE %(schema)s.%(tablename)s_temp (
'''%parameters
for index,column in enumerate(colnames):
	colname, type, encoding, not_null = column
	if not_null:
		not_null='not null'
	else:
		not_null=''

	if index > 0:
		ddl += ', %(colname)s %(type)s %(notnull)s\n'%{'colname':colname,'type':type
		                                              ,'notnull':not_null}
	else:
		ddl += '  %(colname)s %(type)s %(notnull)s\n'%{'colname':colname,'type':type
		                                            ,'notnull':not_null}
ddl += ')\n'

#To add in new distkey
ddl += 'distkey(%(distkey)s) '%{'distkey':distkeyname} 


if sortkeys:
	ddl+= 'sortkey ('
	for index,column in enumerate([sortkeys[index] for index in sorted(sortkeys.keys())]):
		if index > 0:
			ddl += ', '
		ddl += column
	ddl += ');'		


#Grant step to check select and insert permissions and put them into ddl
grantquery = '''
SELECT usename
      ,has_table_privilege (usename,%(tablename)s,'select') as select_access
      ,has_table_privilege (usename,%(tablename)s,'insert') as insert_access
 FROM pg_user
WHERE has_table_privilege (usename,%(tablename)s,'select') = True
  AND usesuper = False
'''
selectusers=[]
insertusers=[]
cur.execute(grantquery,parameters)
for row in cur.fetchall():
	username, select, insert = row
	if select == True:
		selectusers.append(username)
	if insert == True:
		insertusers.append(username)

parameters['selectusers'] = ','.join(selectusers)
parameters['insertusers'] = ','.join(insertusers)
grantselect = '\nGRANT SELECT ON %(tablename)s to %(selectusers)s;'%parameters
grantinsert = '\nGRANT INSERT ON %(tablename)s to %(insertusers)s;'%parameters
ddl+=grantselect+grantinsert

print ddl

#New ddl generated
#Next steps: 
#1. Create new temp table with dist key
#2. Copy data over from original table. Proper way to re-encode this would be to unload
#   to s3 and then reload back to the new temp table. Most of my tables are uncompressed.
#3. Drop old table
#4. Rename temp table

print
print 'DDL Generated'
print

print 'Generating temp table with new dist key....'
cur.execute(ddl)

print 'Copying data over from original table'
copystatement = '''
INSERT INTO %(schema)s.%(tablename)s_temp
SELECT * FROM %(schema)s.%(tablename)s;
'''%parameters

print 'Drop old table step'

print 'Altering table name'
renamequery = '''
ALTER TABLE %(schema)s.%(tablename)s_temp RENAME TO %(tablename)s_2;
'''%parameters
cur.execute(renamequery)


#con.commit()



