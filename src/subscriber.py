from shapely import wkt, wkb
from json import dumps

from feed import get_feed
from cualbondi import df_recorridos, search, serialize_result


class Subscriber:
    def __init__(self, ws, app):
        self.app = app
        self.ws = ws
        self.recorridos = []
        self.position = None

    def unsubscribe(self):
        for recorrido in self.recorridos:
            feed = get_feed(recorrido, self.app)
            feed.remove_listener(self)

    def subscribe(self):
        for recorrido in self.recorridos:
            feed = get_feed(recorrido, self.app)
            feed.add_listener(self)

    def __hash__(self):
        return hash(self.ws)

    def __eq__(self, other):
        self.ws == other.ws

    async def process_message(self, msg):
        recorrido_id = msg["RecorridoID"]
        bus_position = wkt.loads(msg["Point"])
        await self.send_update(recorrido_id, bus_position, msg['Timestamp'], msg['IDGps'])

    async def send_initial_data(self):
        async with self.app.pgpool.acquire() as conn:
            async with conn.cursor() as cur:
                for recorrido in self.recorridos:
                    query = """
                        select * from gps
                        where timestamp in (
                            select max(timestamp)
                            from gps
                            where recorrido_id = %s
                            group by id_gps
                        )
                        and recorrido_id = %s
                        and extract(epoch from now() - timestamp) < 60*15
                    """
                    await cur.execute(query, (recorrido, recorrido))
                    fields = ('id', 'timestamp', 'latlng', 'id_gps',
                              'speed', 'angle', 'recorrido_id', 'meta')
                    async for row in cur:
                        res = dict(zip(fields, row))
                        recorrido_id = res['recorrido_id']
                        bus_position = wkb.loads(res['latlng'], hex=True)
                        await self.send_update(recorrido_id, bus_position, res['timestamp'].isoformat(), res['id_gps'])

    async def send_update(self, recorrido_id, bus_position, timestamp, id_gps):
        try:
            ruta = df_recorridos.loc[recorrido_id].ruta
        except KeyError:
            print('no matching route found for id ', recorrido_id)
            return
        user_position = wkt.loads(self.position)
        result = search(ruta, bus_position, user_position)
        if result is None:
            print("no results found")
            return
        response = serialize_result(result)
        response['timestamp'] = timestamp
        response['id_gps'] = id_gps
        await self.ws.send(dumps(response))
