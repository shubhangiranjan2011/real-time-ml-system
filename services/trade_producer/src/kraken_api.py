from typing import List, Dict
import json
from websocket import create_connection

class KrakenWebsocketTradeAPI:

    URL = 'wss://ws.kraken.com/v2'
    def __init__(self, product_id: List[str]):
        self.product_id = product_id
        self._ws = create_connection(self.URL)
        print("Connection established")
        print(f "Subscribing to {product_id} trades")
        self._subscribe(product_id)

    def _subscribe(self, product_id: List[str]):
        '''
        Establishes a connection to the Kraken websocket API and subscribes to the trade channel for the given product_id.'''
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [
                    product_id
                ],
                "snapshot": False }
        }
        self._ws.send(json.dumps(msg))

        # for each product_id, we dump the first two messages because they are not trade data, 
        # but rather connection status messages
        for pro in product_id:
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


