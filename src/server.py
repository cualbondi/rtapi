from json import loads
from sanic import Sanic
from sanic.response import json
from websockets.exceptions import ConnectionClosed
import aioredis
import aiopg

from cualbondi import connection
from subscriber import Subscriber

app = Sanic(__name__)

subscribers = {}


def get_subscriber(ws):
    if ws in subscribers:
        return subscribers.get(ws)
    else:
        sub = Subscriber(ws, app=app)
        subscribers[ws] = sub
        return sub


@app.route('/')
async def test(request):
    return json({'hello': 'world'})


@app.websocket('/subscribe')
async def feed(request, ws):
    sub = get_subscriber(ws)
    while True:
        try:
            data = await ws.recv()
            print('recieved: ', data)
            message = loads(data)

            sub.unsubscribe()

            sub.position = message["position"]
            sub.recorridos = message["recorridos"]

            sub.subscribe()

            await sub.send_initial_data()

        except ConnectionClosed:
            sub.unsubscribe()
            if ws in subscribers:
                subscribers.pop(ws)


@app.listener('after_server_start')
async def sub_redis(app, loop):
    redis = await aioredis.create_redis('redis://redis')
    app.redis = redis
    pgpool = await aiopg.create_pool(connection)
    app.pgpool = pgpool


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, workers=1, debug=True)
