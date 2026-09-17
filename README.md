# 📈 PetProjectBinance

> **Real-time криптовалютный пайплайн: Binance WebSocket → Kafka → ClickHouse**

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=flat&logo=clickhouse&logoColor=black)
![WebSocket](https://img.shields.io/badge/WebSocket-010101?style=flat&logo=socketdotio&logoColor=white)

---

## 🎯 О проекте

Реалтайм-пайплайн для сбора и обработки криптовалютных котировок с биржи **Binance**.

Проект демонстрирует навыки работы с **потоковыми данными**, **брокерами сообщений** и **аналитическими базами данных** — от подключения к WebSocket API до записи в колоночное хранилище.

**В планах** — расчёт арбитражного треугольника по трём валютным парам.

---

## 🛠 Стек технологий

| Компонент | Технология | Назначение |
|---|---|---|
| **Язык** | Python 3.10+ | Основной язык разработки |
| **Поток данных** | WebSocket | Получение котировок с Binance в реальном времени |
| **Брокер** | Apache Kafka | Буферизация и передача сообщений |
| **Хранилище** | ClickHouse | Колоночная БД для аналитики |
| **Библиотеки** | `websockets`, `confluent-kafka`, `clickhouse-connect`, `psycopg2-binary` | — |

---

## ✅ Что реализовано
☑ Подключение к Binance WebSocket API

☑ Подписка на пары BTCUSDT, ETHUSDT, ETHBTC

☑ Продюсер Kafka — приём и публикация потока

☑ Консьюмер Kafka — чтение и запись в ClickHouse

☑ Модуль lib/lib.py для работы с ClickHouse

☑ Заготовка triangular_arbitrage_consumer.py

---

🚧 В планах

□ Реализовать расчёт арбитражного треугольника по трём парам

□ Построить дашборд

□ Добавить мониторинг и логирование

