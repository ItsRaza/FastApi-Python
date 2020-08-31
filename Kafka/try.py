import confluent_kafka
import asyncio
from threading import Thread


class AIOProducer:
    def __init__(self, configs, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()


pro = AIOProducer({"bootstrap.servers": "localhost:9092"})
prod = confluent_kafka.Producer({"bootstrap.servers": "localhost:9092"})
print(prod)
