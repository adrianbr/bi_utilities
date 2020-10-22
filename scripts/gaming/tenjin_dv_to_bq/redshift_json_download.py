import psycopg2
from psycopg2.extras import RealDictCursor
import json


default_redshift_conn = {'host':'',
                 'port':'',
                 'dbname':'',
                 'user':'',
                 'password':'',
                 }

#comment out of not using the credential file
import credential
default_redshift_conn=credential.default_redshift_conn

def _redshift_select_to_dict(query, redshift_conn=default_redshift_conn):
    ''' query redshift and return json '''
    #print(redshift_conn)

    conn = psycopg2.connect(**redshift_conn)

    cur = conn.cursor(cursor_factory=RealDictCursor)
    print('Executing: \n \n' + query)
    cur.execute(query)
    conn.commit()
    print ('rows affected:' + str(cur.rowcount))
    if cur.description:
        rows = cur.fetchall()
    else:
        rows = None
    cur.close()
    conn.close()
    return rows


def _rows_to_json_file(rows, filename):
    with open(filename, 'w') as f:
        for row in rows:
            json.dump(row, f, default=str)
            f.write('\n')
    return filename


def redshift_query_to_file(query, filename, redshift_conn=default_redshift_conn):
    json_data = _redshift_select_to_dict(query, redshift_conn=redshift_conn)
    filename = _rows_to_json_file(json_data, filename)
    return filename
