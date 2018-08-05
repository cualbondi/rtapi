import asyncio
from websockets.exceptions import ConnectionClosed
from json.decoder import JSONDecodeError
from shapely import wkt
from json import dumps
from cualbondi import search, serialize_result, recorridos

feeds = {}


def get_feed(recorrido_id, app):
    if recorrido_id in feeds:
        return feeds.get(recorrido_id)
    else:
        feed = BondiFeed(recorrido_id, app=app)
        feeds[recorrido_id] = feed
        return feed


class BaseFeed:
    def __init__(self, id, app):
        self.id = id
        self.redis_channel = f'gps-{self.id}'
        self.app = app
        self.clients = {}

        # queue subscribe into the event loop
        asyncio.ensure_future(self.subscribe())

    def add_listener(self, ws, info):
        self.clients[ws] = info
        print(self.id, self.clients)

    async def broadcast_message(self, msg):
        # run in parallel
        await asyncio.gather(
            *[self.notify_sub(msg, ws, info)
              for ws, info in self.clients.items()]
        )

    async def subscribe(self):
        res = await self.app.redis.subscribe(self.redis_channel)
        ch = res[0]
        await self._subscription_handler(ch)

    async def _subscription_handler(self, ch):
        while (await ch.wait_message()):
            try:
                msg = await ch.get_json()
            except JSONDecodeError as e:
                print(e)
                continue
            # queue broadcasting into the event loop and keep recieving
            asyncio.ensure_future(self.broadcast_message(msg))

    def remove_client(self, client):
        self.clients.pop(client)

    async def notify_sub(self, msg, ws, info):
        try:
            await self.message_handler(msg, ws, info)
        except ConnectionClosed:
            self.remove_client(ws)

    async def message_handler(self, msg, ws, info):
        pass


class BondiFeed(BaseFeed):
    async def message_handler(self, msg, ws, position):
        # {'RecorridoID': 0, 'Timestamp': '2018-07-25 00:40:03',
        # 'Point': 'POINT (-38.7431419999999989 -62.2601849999999999)',
        # 'Angle': 0, 'Speed': 0, 'IDGps': 868683028315608}
        recorrido_id = msg["RecorridoID"]
        bus_position = wkt.loads(msg["Point"])
        try:
            ruta = recorridos.loc[recorrido_id].ruta
        except KeyError:
            print('no matching route found for id ', recorrido_id)
            return
        user_position = wkt.loads(position)
        result = search(ruta, bus_position, user_position)
        if result is None:
            print("no results found")
            return
        response = serialize_result(result)
        response['timestamp'] = msg["Timestamp"]
        response['id_gps'] = msg['IDGps']
        await ws.send(dumps(response))
