from collections import defaultdict
from json import loads, dumps
from sanic import Sanic
from sanic.response import json
from shapely.geometry import LineString
from shapely import wkt
import aioredis
# from sanic.websocket import ConnectionClosed
# from sanic_redis import SanicRedis
# import asyncio
# import pandas as pd

from cualbondi import get_recorridos, search, serialize_result


app = Sanic(__name__)

df = get_recorridos()
d = defaultdict(list)


@app.route('/')
async def test(request):
    return json({'hello': 'world'})


@app.websocket('/subscribe')
async def feed(request, ws):
    while True:
        data = await ws.recv()
        print('recieved: ', data)
        message = loads(data)
        position = message["position"]
        recorridos = message["recorridos"]
        for recorrido in recorridos:
            d[recorrido] += [{
                'ws': ws,
                'position': position
            }]


@app.listener('after_server_start')
async def sub_redis(app, loop):
    sub = await aioredis.create_redis('redis://redis')
    res = await sub.subscribe('update')
    ch1 = res[0]

    # read redis pubsub stream
    while (await ch1.wait_message()):
        try:
            msg = await ch1.get_json()
        except json.decoder.JSONDecodeError as e:
            print(e)
            continue
        print("from redis: ", msg)
        await process_update(msg)


async def process_update(msg):
    # fake update redis with:
    # publish update '{"id": 1, "position": "POINT (0 1)", "timestamp": "2018-07-01"}'
    mid = msg["id"]
    # bus_position = Point(0, 1)
    bus_position = wkt.loads(msg["position"])
    # ruta = df[df.id == mid].ruta
    ruta = LineString([(-2, 2), (4, 2), (4, 1.5), (-2, 1.5)])
    # get the listers for this recorrido_id
    listeners = d[mid]
    for sub in listeners:
        # user_position = Point(1, 1)
        user_position = wkt.loads(sub["position"])
        result = search(ruta, bus_position, user_position)
        if result is None:
            print("no results found")
            continue

        ans = serialize_result(result)
        ans['timestamp'] = msg["timestamp"]
        await sub['ws'].send(dumps(ans))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, workers=1)
