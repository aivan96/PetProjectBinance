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
    connection.command(f"""
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
    connection.command(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table}
    ( 
        {columns_expr}
    ) 
    engine = Distributed('default', '{schema}', '{table}_local', {sharding_key});
    """)

def ch_insert(database, table, data, ch_connection):
    sql = f'INSERT INTO {database}.{table} VALUES {data}'
    ch_connection.command(sql)