import asyncio
from websockets.exceptions import ConnectionClosed
from json.decoder import JSONDecodeError

feeds = {}


def get_feed(recorrido_id, app):
    if recorrido_id in feeds:
        return feeds.get(recorrido_id)
    else:
        feed = Feed(recorrido_id, app=app)
        feeds[recorrido_id] = feed
        return feed


class Feed:
    """ 
    Feed handles subscriptions to a redis channels.
    Listeners should implement a process_message(self, msg) method
    """

    def __init__(self, id, app):
        self.id = id
        self.redis_channel = f'gps-{self.id}'
        self.app = app
        self.listeners = set()

        # queue subscribe into the event loop
        asyncio.ensure_future(self.subscribe())

    def add_listener(self, listener):
        self.listeners.add(listener)

    async def broadcast_message(self, msg):
        # run in parallel
        await asyncio.gather(
            *[self.notify_sub(msg, listener)
              for listener in self.listeners]
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

    def remove_listener(self, listener):
        if listener in self.listeners:
            self.listeners.remove(listener)

    async def notify_sub(self, msg, listener):
        try:
            await listener.process_message(msg)
        except ConnectionClosed:
            self.remove_listener(listener)
