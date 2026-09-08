from confluent_kafka import Consumer
import json
from datetime import datetime
from config.config import conf_consumer, database
from lib.lib import get_ch_connection

consumer = Consumer(**conf_consumer)
producers = ['binance-btcusdt-producer', 'binance-ethusdt-producer', 'binance-ethbtc-producer']
consumer.subscribe(producers)

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

        if value['service'] == 'Binance' and value['data']:
            target = 'default.btcusdt_binance'
            value['data']['w'] = value['data']['w'].replace(',','.')
            price = value['data']['w']

        if target != '' and price != '':
            sql = f"""INSERT INTO {target} ("operation_id", "price", "timestamp") 
                                            VALUES ({value['id']}, {price}, '{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}')"""
            client = get_ch_connection()
            client.query(sql)
            print(f'Данные вставлены в {target}. SQL: {sql}')

except KeyboardInterrupt:
    print('\n Остановка...')
finally:
    consumer.close()