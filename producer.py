import asyncio
import json
import websockets
from confluent_kafka import Producer
from datetime import datetime
from config.config import conf_producer

print(f'Producer подключен к {conf_producer['bootstrap.servers']}')

producer = Producer(**conf_producer)

async def binance_websocket_btcusdt():
    symbol = 'btcusdt'
    url = f'wss://stream.binance.com:9443/ws/{symbol}@ticker'

    print(f'Подключение к WebSocket Binance {symbol} ...')

    async with websockets.connect(url) as websocket:
        print('Соединение установлено!')
        while True:
            i = 1
            async for message in websocket:
                msg = {
                    'id': i,
                    'service': 'Binance',
                    'crypto': 'btcusdt',
                    'text': f'Сообщение #{i}',
                    'timestamp': datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S"),
                    'data': '',
                }

                try:
                    data = json.loads(message)
                    msg['data'] = data
                    c_date = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
                    print(f'Время получения: {c_date} Данные BTC/USDT получены: {msg['data']}')

                    if 'ping' in message:
                        await websocket.send(json.dumps({"pong": data['ping']}))

                    producer.produce(
                        'binance_btcusdt_producer',
                        value=json.dumps(msg).encode('utf-8'),
                        key=str(i).encode('utf-8'),
                    )
                    producer.poll(0)
                    await asyncio.sleep(5)

                except Exception as e:
                    print(f'Ошибка: {e} - сервис Binance {symbol}')

                finally:
                    i += 1
                    producer.flush(timeout=10)

async def binance_websocket_ethusdt():
    symbol = 'ethusdt'
    url = f'wss://stream.binance.com:9443/ws/{symbol}@ticker'

    print(f'Подключение к WebSocket Binance {symbol} ...')

    async with websockets.connect(url) as websocket:
        print('Соединение установлено!')
        while True:
            i = 1
            async for message in websocket:
                msg = {
                    'id': i,
                    'service': 'Binance',
                    'crypto': 'ethusdt',
                    'text': f'Сообщение #{i}',
                    'timestamp': datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S"),
                    'data': '',
                }

                try:
                    data = json.loads(message)
                    msg['data'] = data
                    c_date = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
                    print(f'Время получения: {c_date} Данные ETH/USDT получены: {msg['data']}')

                    if 'ping' in message:
                        await websocket.send(json.dumps({"pong": data['ping']}))

                    producer.produce(
                        'binance_ethusdt_producer',
                        value=json.dumps(msg).encode('utf-8'),
                        key=str(i).encode('utf-8'),
                    )
                    producer.poll(0)
                    await asyncio.sleep(5)

                except Exception as e:
                    print(f'Ошибка: {e} - сервис Binance {symbol}')

                finally:
                    i += 1
                    producer.flush(timeout=10)

async def binance_websocket_ethbtc():
    symbol = 'ethbtc'
    url = f'wss://stream.binance.com:9443/ws/{symbol}@ticker'

    print(f'Подключение к WebSocket Binance {symbol} ...')

    async with websockets.connect(url) as websocket:
        print('Соединение установлено!')
        while True:
            i = 1
            async for message in websocket:
                msg = {
                    'id': i,
                    'service': 'Binance',
                    'crypto': 'ethbtc',
                    'text': f'Сообщение #{i}',
                    'timestamp': datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S"),
                    'data': '',
                }

                try:
                    data = json.loads(message)
                    msg['data'] = data
                    c_date = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
                    print(f'Время получения: {c_date} Данные ETH/BTC получены: {msg['data']}')

                    if 'ping' in message:
                        await websocket.send(json.dumps({"pong": data['ping']}))

                    producer.produce(
                        'binance_ethbtc_producer',
                        value=json.dumps(msg).encode('utf-8'),
                        key=str(i).encode('utf-8'),
                    )
                    producer.poll(0)
                    await asyncio.sleep(5)

                except Exception as e:
                    print(f'Ошибка: {e} - сервис Binance {symbol}')

                finally:
                    i += 1
                    producer.flush(timeout=10)

async def main():
    result = await asyncio.gather(binance_websocket_btcusdt(), binance_websocket_ethusdt(), binance_websocket_ethbtc())
    print(result)

asyncio.run(main())