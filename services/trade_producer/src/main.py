from typing import Dict, List
from quixstreams import Application
from time import sleep

#from src.config import config
from src.kraken_api import KrakenWebsocketTradeAPI
from websocket._exceptions import WebSocketConnectionClosedException



def produce_trades(
    kafka_broker_address: str,
    kafka_topic: str,
    product_ids: List[str]
) -> None:
    ''' 
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
          kafka_broker_address (str) : The address of the Kafka Broker.
          kafka_topic (str): The name of the Kafka topic.
    Returns:
          None
    '''
    app = Application(broker_address=kafka_broker_address)


    #the topic where trades will be saved
    topic = app.topic(name=kafka_topic, value_serializer='json')

    kraken_api = KrakenWebsocketTradeAPI(product_ids=product_ids)

    # Create a Producer instance
    with app.get_producer() as producer:

        while True:
            trades = kraken_api.get_trades()

            for trade in trades:
                trades: List[Dict] = kraken_api.get_trades()
                

                # Serialize an event using the defined Topic 
                message = topic.serialize(key=trade['product_id'], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(
                    topic=topic.name, value=message.value, key=message.key
                )

            sleep(1) 


if __name__ == '__main__':
    try:
        produce_trades(
            kafka_broker_address='redpanda-0:9092',
            kafka_topic='trade',
            product_ids=['BTC/USD']
        )
    except WebSocketConnectionClosedException:
        print("Connection lost. Retrying...")

    