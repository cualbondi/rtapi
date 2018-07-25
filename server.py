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

# TODO: remove this after development is done
from aoiklivereload import LiveReloader
reloader = LiveReloader()
reloader.start_watcher_thread()


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
    res = await sub.subscribe('gps-<id_recorrido>')
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
    # {'RecorridoID': 0, 'Timestamp': '2018-07-25 00:40:03', 'Point': 'POINT (-38.7431419999999989 -62.2601849999999999)', 'Angle': 0, 'Speed': 0, 'IDGps': 868683028315608}
    mid = msg["RecorridoID"]
    # bus_position = Point(0, 1)
    bus_position = wkt.loads(msg["Point"])
    ruta = df.loc[mid].ruta
    # ruta = LineString([(-2, 2), (4, 2), (4, 1.5), (-2, 1.5)])
    # get the listers for this recorrido_id
    listeners = d[mid]
    print('amount of listeners: ', len(listeners))
    for sub in listeners:
        # user_position = Point(1, 1)
        user_position = wkt.loads(sub["position"])
        result = search(ruta, bus_position, user_position)
        if result is None:
            print("no results found")
            continue

        ans = serialize_result(result)
        ans['timestamp'] = msg["Timestamp"]
        await sub['ws'].send(dumps(ans))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, workers=1, debug=True)
