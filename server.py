from sanic import Sanic
from sanic.response import json
from sanic.websocket import ConnectionClosed
from json import loads, dumps
from collections import defaultdict
# from sanic_redis import SanicRedis
import aioredis
import asyncio


app = Sanic(__name__)
# app.config.update(
#     {
#         'REDIS': {
#             'address': ('redis', 6379),
#             # 'db': 0,
#             # 'password': 'password',
#             # 'ssl': None,
#             # 'encoding': None,
#             # 'minsize': 1,
#             # 'maxsize': 10
#         }
#     }
# )
# redis = SanicRedis(app)


@app.route('/')
async def test(request):
    return json({'hello': 'world'})

d = defaultdict(list)

@app.websocket('/subscribe')
async def feed(request, ws):
    while True:
        data = 'hello!'
        print('Sending: ' + data)
        await ws.send(data)
        data = await ws.recv()
        message = loads(data)
        position = message["position"]
        recorridos = message["recorridos"]
        for recorrido in recorridos:
            d[recorrido] += [{
                'ws': ws,
                'position': position
            }]
        print('Received: ' + data)

@app.listener('after_server_start')
async def sub_redis(app, loop):
    sub = await aioredis.create_redis('redis://redis')
    res = await sub.subscribe('update')
    ch1 = res[0]

    while (await ch1.wait_message()):
        msg = await ch1.get_json()
        print("got Message", msg)
        print(d)
        for recorrido, subscribers in d.items():
            for sub in subscribers:
                await sub['ws'].send('hola')
        

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, workers=1)