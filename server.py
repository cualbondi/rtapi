from sanic import Sanic
from sanic.response import json
from sanic.websocket import ConnectionClosed
from json import loads, dumps
from collections import defaultdict
# from sanic_redis import SanicRedis
import aiopg
import aioredis
import asyncio
import pandas as pd
import geopandas as gpd
import psycopg2


database_name = 'geocualbondidb'
database_host = 'db'
database_user = 'geocualbondiuser'
database_password = 'postgrespass'

connection = 'postgres://{0}:{1}@{2}/{3}'.format(database_user,
                                                 database_password,
                                                 database_host,
                                                 database_name)

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

def get_recorridos():
    conn = psycopg2.connect(connection)
    cur = conn.cursor()
    cur.execute("""
        select *
        from core_recorrido as cr
        join catastro_ciudad_recorridos as ccr
        on ccr.recorrido_id = cr.id
        join catastro_ciudad as cc
        on cc.id = ccr.ciudad_id
        where cc.nombre = 'Bahía Blanca'
    """)
    rows = cur.fetchall()
    print(rows)


df = get_recorridos()

print(df)

async def get_pool():
    return await aiopg.create_pool(connection)

async def prepare_db(loop1, loop2):
    """
    Let's create some table and add some data
    """
    async with aiopg.create_pool(connection) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('DROP TABLE IF EXISTS sanic_polls')
                await cur.execute("""CREATE TABLE sanic_polls (
                                        id serial primary key,
                                        question varchar(50),
                                        pub_date timestamp
                                    );""")
                for i in range(0, 100):
                    await cur.execute("""INSERT INTO sanic_polls
                                    (id, question, pub_date) VALUES ({}, {}, now())
                    """.format(i, i))

@app.route('/')
async def test(request):

    # async with aiopg.create_pool(connection) as pool:
    #         async with pool.acquire() as conn:
    #             async with conn.cursor() as cur:
    #                 await cur.execute("SELECT * from catastro_ciudad_recorridos;")
    #                 async for row in cur:
    #                     print(row)

    return json({'hello': 'world'})

d = defaultdict(list)

@app.websocket('/subscribe')
async def feed(request, ws):
    while True:
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
        await process_update(msg)
       
async def process_update(msg):
    # msg = {id: 1, speed: "123", lat: 30, lng: 50}
    
    listeners = d[msg["id"]]
    for sub in listeners:
        sub['ws'].send("hola")




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, workers=1)