import asyncio
import uuid
import os
import logging
import yaml
import traceback

import tableschema_sql
tableschema_sql.writer.BUFFER_SIZE = 10

from dataflows import Flow

from aiohttp import web
from aiohttp_sse import sse_response

from sqlalchemy import create_engine


from dgp.core import Config, Context, BaseDataGenusProcessor
from dgp.genera import SimpleDGP, LoaderDGP, TransformDGP, EnricherDGP
from dgp.config.consts import CONFIG_PUBLISH_ALLOWED
from dgp.taxonomies import TaxonomyRegistry

from .poster import Poster
from .row_sender import post_flow
from .publish_flow import publish_flow

from dataflows.helpers.extended_json import ejson as json


class ResultsPoster(BaseDataGenusProcessor):

    def __init__(self, config, context, id, poster, tasks):
        super().__init__(config, context)
        self.id = id
        self.poster = poster
        self.tasks = tasks

    def flow(self):
        return post_flow(self.id, self.poster, self.tasks, self.config)


class PublishFlow(BaseDataGenusProcessor):

    def __init__(self, config, context, lazy_engine):
        super().__init__(config, context)
        self.lazy_engine = lazy_engine

    def flow(self):
        if not self.config.get(CONFIG_PUBLISH_ALLOWED):
            return None
        if self.lazy_engine() is not None:
            return Flow(
                publish_flow(self.config, self.lazy_engine()),
            )


class DgpServer(web.Application):

    CORS_HEADERS = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
    }

    def __init__(self, base_path, db_url):
        super().__init__()
        self.base_path = base_path
        self.db_url = db_url
        self.router.add_route('GET', '/events/{uid}', self.events)
        self.router.add_route('POST', '/config', self.config)
        self.router.add_route('OPTIONS', '/config', self.config_options)
        self.engine = None

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

    def loader_dgps(self, config, context):
        return []

    def publish_flow(self, config, context):
        return [
            PublishFlow(config, context, self.lazy_engine())
        ]

    def lazy_engine(self):
        def func():
            if self.engine is None:
                if self.db_url is not None:
                    self.engine = create_engine(self.db_url)
            return self.engine
        return func

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
                    publish_flow = self.publish_flow(config, context) or []

                    tasks = []
                    dgp = SimpleDGP(
                        config, context,
                        steps=[
                            LoaderDGP,
                            *self.loader_dgps(config, context),
                            ResultsPoster(config, context, 0, poster, tasks),
                            TransformDGP,
                            ResultsPoster(config, context, 1, poster, tasks),
                            EnricherDGP,
                            ResultsPoster(config, context, 2, poster, tasks),
                            *publish_flow,
                            ResultsPoster(config, context, 3, poster, tasks),
                        ]
                    )

                    try:
                        ret = dgp.analyze()
                        logging.info('ANALYZED - success=%r', ret)
                        logging.info('%r', config._unflatten()['source'])
                        logging.info('%r', config._unflatten()['structure'])
                        if config.dirty:
                            logging.info('sending config')
                            to_send = config._unflatten()
                            to_send.setdefault('publish', {})['allowed'] = False
                            await poster.post_config(to_send)
                        if not ret:
                            await poster.post_errors(list(map(list, dgp.errors)))

                        logging.info('preparing flow')
                        flow = dgp.flow()

                        logging.info('running flow')
                        await self.run_flow(flow, tasks)
                        logging.info('flow done')
                    except Exception:
                        await poster.post_failure(traceback.format_exc())
                        raise
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
