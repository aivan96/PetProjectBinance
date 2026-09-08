import clickhouse_connect
import psycopg2
from config.config import ch, pg_prod_fbe

def get_ch_connection():
    return clickhouse_connect.get_client(**ch)

def table_exists(client, db, table):
    query = f"""
        SELECT count()
        FROM system.tables
        WHERE database = '{db}'
        AND name = '{table}'
    """
    result = client.query(query)
    return result.result_rows[0][0] == 1

def get_pg_connection():
    return psycopg2.connect(**pg_prod_fbe)

def ch_struct_to_columns_expr(structure):
    columns = []
    for c in structure:
        columns.append(f'`{c[0]}` {c[1]}')
    return ",\n\t".join(columns)

def ch_create_local(schema, table, structure, connection, partition_by='tuple()', order_by='tuple()'):
    columns_expr = ch_struct_to_columns_expr(structure)
    connection.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table}_local
    ( 
        {columns_expr}
    ) 
    engine = MergeTree() 
    PARTITION BY {partition_by}
    ORDER BY {order_by}
    SETTINGS storage_policy = 'default', index_granularity = 8192
    """)

def ch_create_dist(schema, table, structure, connection, sharding_key='rand()'):
    columns_expr = ch_struct_to_columns_expr(structure)
    connection.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table}
    ( 
        {columns_expr}
    ) 
    engine = Distributed('default', '{schema}', '{table}_local', {sharding_key});
    """)

def ch_truncate(schema, table, connection):
    connection.execute(f"""
    TRUNCATE TABLE IF EXISTS {schema}.{table}
    """)

def ch_fill_temp(schema, table, schema_temp, table_temp, primary, connection):
    connection.execute(f"""
    INSERT INTO {schema_temp}.{table_temp}
    SELECT * FROM {schema}.{table}
    WHERE {primary} GLOBAL NOT IN (
        SELECT {primary} FROM {schema_temp}.{table_temp}
    )
    """)

def ch_drop(schema, table, connection):
    return connection.execute(f"""
    DROP TABLE IF EXISTS {schema}.{table}
    """)

def ch_rename(schema_temp, table_temp, schema, table, ch_connection):
    return ch_connection.execute(f"""
    RENAME TABLE IF EXISTS {schema_temp}.{table_temp} TO {schema}.{table}
    """)

def get_schema_from_ch_table(database, table, ch_connection):
    r = ch_connection.execute(f'''
        select name, type 
        from system.columns 
        where database = '{database}' and table = '{table}' 
        order by position
    ''')
    r = list(r)
    schema = []
    for col in r:
        _name = col[0].split('.')[-1]
        _type = col[1]
        schema.append((_name, _type))
    if len(schema) == 0:
        return None
    return schema

def get_schema_from_ch(query, ch_connection):
    r = ch_connection.execute(f'describe table ({query})')
    r = list(r)
    schema = []
    for col in r:
        _name = col[0].split('.')[-1]
        _type = col[1]
        schema.append((_name, _type))
    if len(schema) == 0:
        return None
    return schema

def schema_changed(old_schema, new_schema):
    if not old_schema or not new_schema:
        return True
    os_len = len(old_schema)
    if not os_len == len(new_schema):
        return True
    for i in range(os_len):
        old_column = old_schema[i]
        new_column = new_schema[i]
        for j in range(2):
            if not old_column[j] == new_column[j]:
                return True
    return False

def get_params_list(sql, ch_connection, **kwargs):
    if kwargs:
        sql = sql.format(**kwargs)
    r = ch_connection.execute(sql)
    r = list(r)
    r = [c[0] for c in r]
    return r

def get_params(sql, ch_connection, **kwargs):
    if kwargs:
        sql = sql.format(**kwargs)
    r = ch_connection.execute(sql)
    r = list(list(r)[0])
    if len(r) == 1:
        r = r[0]
    return r

def ch_exec_insert_query(target, sql, ch_connection, *args):
    if args:
        sql = sql.format(*args)
    sql = f'INSERT INTO {target}\n' + sql
    ch_connection.execute(sql)

def ch_exec_insert_query_kwargs(target, sql, ch_connection, **kwargs):
    if kwargs:
        sql = sql.format(**kwargs)
    sql = f'INSERT INTO {target}\n' + sql
    ch_connection.execute(sql)