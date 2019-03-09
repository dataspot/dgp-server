import logging


class Poster():

    def __init__(self, uid, send):
        self.send = send
        self.uid = uid

    async def post_config(self, config):
        await self.send(dict(
            t='c',
            p=config,
            uid=self.uid,
        ))

    async def post_errors(self, errors):
        await self.send(dict(
            t='e',
            e=errors,
            uid=self.uid,
        ))

    async def post_row(self, phase, index, row, errors=None):
        await self.send(dict(
            t='r',
            p=row,
            j=phase,
            i=index,
            e=errors,
            uid=self.uid,
        ))

    async def post_row_count(self, phase, index):
        await self.send(dict(
            t='n',
            j=phase,
            i=index,
            uid=self.uid,
        ))
