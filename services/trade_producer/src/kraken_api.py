from typing import List, Dict
import json
from loguru import logger
from websocket import create_connection

class KrakenWebsocketTradeAPI:

    URL = 'wss://ws.kraken.com/v2'
    def __init__(self, product_ids: List[str]):
        self.product_ids = product_ids

        self._ws = create_connection(self.URL)
        logger.info("Connection established to Kraken API.")

        self._subscribe(product_ids)


    def _subscribe(self, product_ids: List[str]):
        '''
        Establishes a connection to the Kraken websocket API and subscribes to the trade channel for the given product_id.'''
        logger.info("Subscribing to the Kraken API...")
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [
                    product_ids
                ],
                "snapshot": False }
        }
        self._ws.send(json.dumps(msg))
        logger.info("Subscribed to the Kraken API.")

        # for each product_id, we dump the first two messages because they are not trade data, 
        # but rather connection status messages
        for pro in product_ids:
            _=self._ws.recv()
            _=self._ws.recv()
        

    def get_trades(self) -> List[Dict]:
        # mock_trades = [
        #     {
        #     "product_id": "BTC-USD",
        #         "price": 60000,
        #         "volume": "0.1",
        #         "time": 1612345678
        #     },
        #     {
        #         "product_id": "BTC-USD",
        #         "price": 60001,
        #         "volume": "0.2",
        #         "time": 1612345679
        #     },
        #     {
        #         "product_id": "BTC-USD",
        #         "price": 60002,
        #         "volume": "0.3",
        #         "time": 1612345680
        #     }
        # ]
        message = self._ws.recv()
        if 'heartbeat' in message:
            return []
        
        message = json.loads(message)

        trades=[]
        for trade in message['data']:
            trades.append({
                "product_id": trade['symbol'],
                "price": trade['price'],
                "volume": trade['qty'],
                "time": trade['timestamp']
            })

        return trades


