import sqlalchemy as s
import pandas as pd


def create_connection():
    host = 'tai.db.elephantsql.com'
    user = 'xbsstbyy'
    db = 'xbsstbyy'
    pwd = 'mpPWwkb3YjnyKUjYGgunh5C_3PZyjuls'
    engine = s.create_engine(f'postgresql://{user}:{pwd}@{host}/{db}')
    return engine


def query(q):
    conn = create_connection()
    if 'update' not in q and 'create' not in q and 'insert' not in q and 'select' in q:
        return pd.read_sql_query(q, con=conn)
    else:
        conn.execute(q)
        return True


def db_insert(data, target):
    conn = create_connection()
    insert_statement = ''
    cols = ','.join(data.columns)
    for i in range(0, data.shape[0]):
        values = tuple(list(data.iloc[i, :]))
        values = str(values) if len(values) > 1 else str(values).replace(',', '')
        values = values.replace("'null'", "null").replace('"', "'")
        insert_statement += f'insert into public.{target} ({cols}) values {values};'
    conn.execute(insert_statement)
    conn.dispose()
    return True