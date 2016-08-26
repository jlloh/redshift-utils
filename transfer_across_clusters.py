import ConfigParser
import os
import psycopg2
from psycopg2.extensions import AsIs
import subprocess


def get_config(location = None):
    """
    Input Arguments: Defaults to None to read file in home directory
    Returns: config object. 
    """
    config = ConfigParser.RawConfigParser()
    config.read(os.path.expanduser('~/.config.cfg'))
  
    return config

def get_redshift_connection(config, cluster_name):
    """
    Input Arguments: ConfigParser Object, Section Name (i.e. cluster name)
    Returns: psycopg connection object
    """
    username = config.get(cluster_name,'user')
    password = config.get(cluster_name,'password')
    dbname = config.get(cluster_name,'database')
    host = config.get(cluster_name,'host')
    port = config.get(cluster_name,'port')

    con = psycopg2.connect(dbname = dbname, user = username, host = host
                          ,password = password,port = port)

    return con

def get_s3_credentials(config, s3_section_name):
    """
    Input Arguments: ConfigParser Object, s3_section_name (which might be bucket name)
    Returns: tuple with bucket name, access key id and secret access key
    """
    bucket_name = config.get('s3','bucket')
    access_key_id = config.get('s3','aws_access_key_id')
    secret_access_key = config.get('s3','aws_secret_access_key')

    return bucket_name, access_key_id, secret_access_key

def purge_s3(bucket_name):
    """
    Input Arguments: 
    """
    subprocess.check_output(['aws', 's3', 'rm', '--recursive'
                           , 's3://%(bucket_name)s/jl/unload/'%\
                            {'bucket_name': bucket_name}])

def generate_ddl(cur = None, schema = None, table_name = None, new_dist_key = None):
    """
    Input Arguments: cur (psycopg cursor), schema, table_name
    Returns: Text string of ddl
    """
    query = '''
    SET SEARCH_PATH TO %(schema)s;
 SELECT "column", "type", encoding, distkey, sortkey, "notnull"
   FROM pg_table_def
  WHERE tablename = %(table_name)s;
    '''
    colnames = []
    distkeys = []
    sortkeys = {}
    not_null_dict = {True: 'not null', False: ''}
    cur.execute(query, {'schema': schema, 'table_name': table_name})
    for row in cur.fetchall():
        column, type, encoding, distkey, sortkey, not_null = row
        colnames.append((column,type,encoding,not_null))
        distkeys.append(distkey)
        if sortkey != None:
            sortkeys[sortkey]=column
    keys = ['%(col_name)s %(type)s %(not_null)s'%{'col_name': col_name[0]
                                                , 'type': col_name[1]
                                                , 'not_null': not_null_dict[col_name[3]]} 
            for index, col_name in enumerate(colnames)]
    print 'table_name:', table_name
    print 'Columns:'
    for column in colnames:
        print column
    dist_key_option = raw_input('New distkey? [y/n]')
    if dist_key_option == 'y':
        new_dist_key = raw_input('Input column name:')
        new_dist_key = 'distkey( %(new_dist_key)s )'%{'new_dist_key': new_dist_key}
    else:
        new_dist_key = ''
    ddl = '''
DROP TABLE IF EXISTS cia.%(table_name)s;
CREATE TABLE cia.%(table_name)s (
%(keys)s
)
%(distkey)s
sortkey (%(sortkeys)s);
    '''%{'schema': schema, 'table_name': table_name, 'keys': '\n, '.join(keys)
       , 'sortkeys':', '.join([sortkeys[key] for key in sorted(sortkeys)])
       , 'distkey': new_dist_key}

    return ddl 

def unload_to_s3(cur = None, schema = None, table_name = None , bucket_name = None
               , access_key_id = None, secret_access_key = None):
    """
    Input Arguments: cur (psycopg cursor), schema, table_name, bucket_name, access_key_id
                   , secret_access_key (all strings)
    Returns: None
    Unloads table to s3 bucket. For not assumes path bucket/jl/unload/table_name_
    Generates manifest file in that location
    """
    unload_query = '''
    UNLOAD (
   'SELECT * FROM %(schema)s.%(table_name)s'
    )
    TO 's3://%(bucket_name)s/jl/unload/%(table_name)s/%(table_name)s_' 
    CREDENTIALS
    'aws_access_key_id=%(access_key_id)s;aws_secret_access_key=%(secret_access_key)s'
    manifest 
    delimiter '|'
    escape
    gzip;
    '''

    cur.execute(unload_query, {'table_name': table_name, 'bucket_name': bucket_name
                             , 'schema': schema, 'access_key_id': access_key_id
                             , 'secret_access_key': secret_access_key})

def load_from_s3(cur = None, schema = None, table_name = None , bucket_name = None
               , access_key_id = None, secret_access_key = None):
    """
    Input Arguments: cur (psycopg cursor), schema, table_name, bucket_name, access_key_id
                   , secret_access_key (all strings)
    Returns: None
    Loads back gziped data from s3 bucket into table. 
    """
    load_query = '''
    COPY cia.%(table_name)s 
    FROM 's3://%(bucket_name)s/jl/unload/%(table_name)s/%(table_name)s_manifest'
    CREDENTIALS
    'aws_access_key_id=%(access_key_id)s;aws_secret_access_key=%(secret_access_key)s'
    manifest 
    delimiter '|'
    escape
    gzip;
    '''
    cur.execute(load_query, {'table_name': table_name, 'bucket_name': bucket_name
                           , 'schema': schema, 'access_key_id': access_key_id
                           , 'secret_access_key': secret_access_key})

if __name__ == '__main__':
    config = get_config()
    main_connection = get_redshift_connection(config, 'main_cluster')
    other_connection = get_redshift_connection(config, 'other_cluster')
    main_cursor = main_connection.cursor()
    other_cursor = other_connection.cursor()
    bucket_name, access_key_id, secret_access_key = [AsIs(i) 
                                                     for i 
                                                     in get_s3_credentials(config, 's3')]
    purge_s3(bucket_name)
    tables = ['bla.blabla']
    for table in tables:
        schema, table_name = [AsIs(i) for i in table.split('.')]
        unload_to_s3(cur = main_cursor, schema = schema, table_name = table_name
                   , bucket_name = bucket_name, access_key_id = access_key_id
                   , secret_access_key = secret_access_key)
        ddl = generate_ddl(cur = main_cursor, schema = schema
                         , table_name = str(table_name))
        other_cursor.execute(ddl)
        other_connection.commit()
        load_from_s3(cur = other_cursor, schema = schema, table_name = table_name
                   , bucket_name = bucket_name, access_key_id = access_key_id
                   , secret_access_key = secret_access_key)
        other_connection.commit()

    main_connection.close()
    other_connection.close()