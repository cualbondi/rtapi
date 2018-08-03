from json import loads
from sanic import Sanic
from sanic.response import json
from websockets.exceptions import ConnectionClosed
import aioredis

from feed import get_feed


app = Sanic(__name__)


@app.route('/')
async def test(request):
    return json({'hello': 'world'})


@app.websocket('/subscribe')
async def feed(request, ws):
    while True:
        try:
            data = await ws.recv()
            print('recieved: ', data)
            message = loads(data)
            position = message["position"]
            recorridos = message["recorridos"]
            for recorrido in recorridos:
                feed = get_feed(recorrido, app)
                feed.add_listener(ws, position)
        except ConnectionClosed:
            break


@app.listener('after_server_start')
async def sub_redis(app, loop):
    redis = await aioredis.create_redis('redis://redis')
    app.redis = redis


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, workers=1, debug=True)
