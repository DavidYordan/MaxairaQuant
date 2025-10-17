class Service:
    async def start(self):
        raise NotImplementedError

    async def stop(self):
        raise NotImplementedError