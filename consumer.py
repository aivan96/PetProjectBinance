from confluent_kafka import Consumer
import json
from datetime import datetime
from config.config import conf_consumer, database
from lib.lib import get_ch_connection, table_exists, ch_create_local, ch_create_dist, ch_insert

consumer = Consumer(**conf_consumer)
producers = ['binance_btcusdt_producer',
             'binance_ethusdt_producer',
             'binance_ethbtc_producer',
             ]
consumer.subscribe(producers)

tables = []
structure = [
    ('operation_id', 'UInt32'),
    ('price', 'Float64'),
    ('timestamp', 'DateTime'),
]
for p in producers:
    print('Проверка таблиц:')
    table = f'{p.split('_')[0]}_{p.split('_')[1]}'
    table_local = f'{table}_local'
    tables.append(table)
    if table_exists(get_ch_connection(), database, table_local):
        print(f'{table}_local есть в Clickhouse.')
    else:
        print(f'{table} нет в Clickhouse. Создание локальной таблицы...')
        ch_create_local(database, table, structure, get_ch_connection())
        print(f'{table} создана.')
    if table_exists(get_ch_connection(), database, table):
        print(f'{table} есть в Clickhouse.')
    else:
        print(f'{table} нет в Clickhouse. Создание локальной таблицы...')
        ch_create_dist(database, table, structure, get_ch_connection())
        print(f'{table} создана.')

print(f'Consumer подключен к {conf_consumer['bootstrap.servers']}')

try:
    while True:
        target = ''
        price = ''
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(f'Ошибка: {msg.error()}')
            continue

        value = json.loads(msg.value().decode('utf-8'))
        table = f"{value["service"].lower()}_{value["crypto"]}"

        if value['data'] and table in tables:
            target = f'{database}.{table}'
            value['data']['w'] = value['data']['w'].replace(',', '.')
            price = float(value['data']['w'])

        if target != '' and price != '':
            data = (value['id'], price, datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
            ch_insert(database, table, data, get_ch_connection())
            print(f'Данные вставлены в {target}. Данные: {data}')

except KeyboardInterrupt:
    print('\n Остановка...')
finally:
    consumer.close()