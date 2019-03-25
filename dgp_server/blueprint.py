import asyncio
import uuid
import os
import logging
import yaml

from dataflows import Flow

from aiohttp import web
from aiohttp_sse import sse_response

from sqlalchemy import create_engine

import tableschema_sql

from dgp.core import Config, Context
from dgp.genera.simple import SimpleDGP
from dgp.taxonomies import TaxonomyRegistry

from .poster import Poster
from .row_sender import post_flow
from .publish_flow import publish_flow

from dataflows.helpers.extended_json import ejson as json

tableschema_sql.writer.BUFFER_SIZE = 10


class DgpServer(web.Application):

    CORS_HEADERS = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
    }

    def __init__(self, base_path, db_url):
        super().__init__()
        self.base_path = base_path
        self.engine = create_engine(db_url)
        self.router.add_route('GET', '/events/{uid}', self.events)
        self.router.add_route('POST', '/config', self.config)
        self.router.add_route('OPTIONS', '/config', self.config_options)

    # Utils:
    def path_for_uid(self, uid, *args):
        return os.path.join(self.base_path, uid, *args)

    def sender(self, resp):
        async def func(item):
            await resp.send(json.dumps(item))
        return func

    # Flows aux
    async def run_flow(self, flow, tasks):
        ds = flow.datastream()
        for res in ds.res_iter:
            for row in res:
                while len(tasks) > 0:
                    task = tasks.pop(0)
                    await asyncio.gather(task)

    # Routes:
    async def events(self, request: web.Request):
        # loop = request.app.loop

        uid = request.match_info['uid']
        # error_code = None
        # exception = None
        try:
            async with sse_response(request,
                                    headers=self.CORS_HEADERS) as resp:
                try:
                    config = Config(self.path_for_uid(uid, 'config.yaml'))
                    taxonomy_registry = TaxonomyRegistry('taxonomies/index.yaml')
                    context = Context(config, taxonomy_registry)
                    poster = Poster(uid, self.sender(resp))

                    tasks = []
                    dgp = SimpleDGP(
                        config, context,
                    )

                    try:
                        ret = dgp.analyze()
                        logging.info('ANALYZED')
                        logging.info('%r', config._unflatten()['source'])
                        logging.info('%r', config._unflatten()['structure'])
                        if config.dirty:
                            await poster.post_config(config._unflatten())
                        if not ret:
                            await poster.post_errors(list(map(list, dgp.errors)))

                        dgp.post_flows = [
                            post_flow(0, poster, tasks, config, cache=False),
                            post_flow(1, poster, tasks, config),
                            post_flow(2, poster, tasks, config),
                        ]
                        dgp.publish_flow = Flow(
                            publish_flow(config, self.engine),
                            post_flow(3, poster, tasks, config)
                        )
                        flow = dgp.flow()

                        await self.run_flow(flow, tasks)

                    finally:
                        for task in tasks:
                            await asyncio.gather(task)
                except Exception:
                    logging.exception('Error while executing')
                finally:
                    try:
                        await resp.send('close')
                    except Exception as e:
                        logging.error('Error while closing: %s', e)
                    return resp
        except Exception as e:
            logging.error('Error while finalizing: %s', e)

    async def config(self, request: web.Request):
        body = await request.json()
        uid = request.query.get('uid')
        if uid:
            if not os.path.exists(self.path_for_uid(uid)):
                uid = None
        if not uid:
            uid = uuid.uuid4().hex
            if not os.path.exists(self.path_for_uid(uid)):
                os.mkdir(self.path_for_uid(uid))

        with open(self.path_for_uid(uid, 'config.yaml'), 'w') as conf:
            yaml.dump(body, conf)
        return web.json_response({'ok': True, 'uid': uid},
                                 headers=self.CORS_HEADERS)

    async def config_options(self, request: web.Request):
        return web.json_response({}, headers=self.CORS_HEADERS)
