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

    async def post_failure(self, message):
        await self.send(dict(
            t='f',
            e=message,
            uid=self.uid,
        ))

    async def post_row(self, phase, index, row, errors=None):
        if '__errors' in row:
            errors = row.pop('__errors')
        if '__errors_field' in row:
            errors_field = row.pop('__errors_field')
        else:
            errors_field = None
        await self.send(dict(
            t='r',
            p=row,
            j=phase,
            i=index,
            e=errors,
            ef=errors_field,
            uid=self.uid,
        ))

    async def post_row_count(self, phase, index):
        await self.send(dict(
            t='n',
            j=phase,
            i=index,
            uid=self.uid,
        ))

    async def post_done(self, phase):
        await self.send(dict(
            t='d',
            j=phase,
            uid=self.uid,
        ))
