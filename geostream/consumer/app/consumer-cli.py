import asyncio

from aiokafka import AIOKafkaConsumer
from loguru import logger

loop = asyncio.get_event_loop()

topic_name = 'geostream'
project_name = 'consumer-cli'
kafka_instance = 'localhost:9092'


async def do_consume():
    consumer = AIOKafkaConsumer(
        topic_name,
        loop=loop,
        client_id=project_name,
        bootstrap_servers=kafka_instance,
        enable_auto_commit=False,
    )

    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:

        # Consume messages
        while True:
            async for msg in consumer:
                data = msg.value.decode()
                logger.info(data)

    finally:
        await consumer.stop()


loop.run_until_complete(do_consume())
