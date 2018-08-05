from json import loads
from sanic import Sanic
from sanic.response import json
from websockets.exceptions import ConnectionClosed
from shapely import wkt, wkb
import aioredis
import aiopg
from feed import get_feed
from cualbondi import connection, search, serialize_result, recorridos as recorridos_df
from json import dumps

app = Sanic(__name__)


@app.route('/')
async def test(request):
    return json({'hello': 'world'})


@app.websocket('/subscribe')
async def feed(request, ws):
    # TODO: without this while true frontend throws error on new messages
    prev_recorridos = []
    while True:
        try:
            data = await ws.recv()
            print('recieved: ', data)
            message = loads(data)
            position = message["position"]
            for prev_recorrido in prev_recorridos:
                feed = get_feed(prev_recorrido, app)
                feed.remove_client(ws)
            recorridos = message["recorridos"]
            for recorrido in recorridos:
                feed = get_feed(recorrido, app)
                feed.add_listener(ws, position)
                
            await send_initial_data(ws, recorridos, position)
        except ConnectionClosed:
            break

async def send_initial_data(ws, recorridos, position):
    async with app.pgpool.acquire() as conn:
        async with conn.cursor() as cur:
            for recorrido in recorridos:
                query = """
                select * from gps
                where timestamp in (
                select max(timestamp)
                from gps
                where recorrido_id = %s
                group by id_gps
                )
                and recorrido_id = %s
                and extract(epoch from now() - timestamp) < 60*1500
                """
                await cur.execute(query, (recorrido, recorrido))
                fields = ('id', 'timestamp', 'latlng', 'id_gps', 'speed', 'angle', 'recorrido_id', 'meta')
                async for row in cur:
                    print(row)
                    res = dict(zip(fields, row))
                    print(res)
                    try:
                        recorrido_id = res['recorrido_id']
                        ruta = recorridos_df.loc[recorrido_id].ruta
                    except KeyError:
                        print('no matching route found for id ', recorrido_id)
                        continue
                    user_position = wkt.loads(position)
                    bus_position = wkb.loads(res['latlng'], hex=True)
                    result = search(ruta, bus_position, user_position)
                    if result is None:
                        print("no results found")
                        continue
                    response = serialize_result(result)
                    response['timestamp'] = res["timestamp"].isoformat()
                    response['id_gps'] = res['id_gps']
                    print('sending result')
                    await ws.send(dumps(response))
                
    # connect db
    # query para c/ recorrido
    # search
    # send messages


@app.listener('after_server_start')
async def sub_redis(app, loop):
    redis = await aioredis.create_redis('redis://redis')
    app.redis = redis
    pgpool = await aiopg.create_pool(connection)
    app.pgpool = pgpool


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, workers=1, debug=True)
