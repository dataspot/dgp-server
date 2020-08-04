import asyncio
import uuid
import os
import yaml
import traceback
import aiopg.sa
from aiohttp import web
from aiohttp_sse import sse_response

from sqlalchemy import create_engine

import tableschema_sql
tableschema_sql.writer.BUFFER_SIZE = 10

from dataflows import Flow

from dgp.core import Config, Context, BaseDataGenusProcessor
from dgp.genera import SimpleDGP, LoaderDGP, PostLoaderDGP, TransformDGP, EnricherDGP
from dgp.config.consts import CONFIG_PUBLISH_ALLOWED
from dgp.taxonomies import TaxonomyRegistry

from .poster import Poster
from .row_sender import post_flow
from .publish_flow import publish_flow
from .configurations import configs, ConfigHeaderMappings, ConfigColumnTypes
from .log import logger

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
            txn_config = self.context.taxonomy.config
            mode = txn_config.get('db-update-mode', 'append')
            return Flow(
                publish_flow(self.config, self.lazy_engine(), mode),
            )


class DgpServer(web.Application):

    CORS_HEADERS = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
    }

    def __init__(self, base_path, db_url, conf_db_url=None):
        super().__init__()
        self.base_path = base_path
        self.db_url = db_url
        self.conf_db_url = conf_db_url or db_url
        self.router.add_route('GET', '/events/{uid}', self.events)
        self.router.add_route('POST', '/config', self.config)
        self.router.add_route('OPTIONS', '/config', self.config_options)
        self.router.add_get('/configs', configs)
        self.taxonomy_registry = TaxonomyRegistry('taxonomies/index.yaml')
        self.header_mappings = ConfigHeaderMappings(self.taxonomy_registry)
        self.column_type_refresher = ConfigColumnTypes(self.taxonomy_registry)
        self.engine = None
        self.on_startup.append(self.init_pg)
        self.on_cleanup.append(self.close_pg)

    async def init_pg(self, app):
        engine = await aiopg.sa.create_engine(self.conf_db_url)
        app['db'] = engine

    async def close_pg(self, app):
        app['db'].close()
        await app['db'].wait_closed()

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

    def preload_dgps(self, config, context):
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
                    await self.column_type_refresher.refresh(request)
                    for tid, txn in self.taxonomy_registry.index.items():
                        txn.header_mapping.update(await self.header_mappings.header_mapping(tid, request))
                    context = Context(config, self.taxonomy_registry)
                    poster = Poster(uid, self.sender(resp))
                    publish_flow = self.publish_flow(config, context) or []

                    tasks = []

                    try:
                        needs_to_send_config = False
                        phase = 0
                        while True:
                            phase += 1
                            dgp = SimpleDGP(
                                config, context,
                                steps=[
                                    *self.preload_dgps(config, context),
                                    LoaderDGP,
                                    PostLoaderDGP,
                                    ResultsPoster(config, context, 0, poster, tasks),
                                    TransformDGP,
                                    ResultsPoster(config, context, 1, poster, tasks),
                                    EnricherDGP,
                                    ResultsPoster(config, context, 2, poster, tasks),
                                    *publish_flow,
                                    ResultsPoster(config, context, 3, poster, tasks),
                                ]
                            )
                            ret = dgp.analyze()
                            logger.info('ANALYZED #%d - success=%r, source=%r',
                                        phase, ret, config._unflatten().get('source'))
                            if config.dirty:
                                needs_to_send_config = True
                            else:
                                break
                            assert phase < 5, 'Too many analysis phases!'
                        logger.info('%r', config._unflatten().get('source'))
                        logger.info('%r', config._unflatten().get('structure'))
                        if needs_to_send_config:
                            logger.info('sending config')
                            to_send = config._unflatten()
                            to_send.setdefault('publish', {})['allowed'] = False
                            await poster.post_config(to_send)
                        if not ret:
                            errors = list(map(list, dgp.errors))
                            logger.warning('analysis errors %s', errors)
                        else:
                            errors = [] 
                        await poster.post_errors(errors)

                        logger.info('preparing flow')
                        flow = Flow(
                            dgp.flow(),
                            lambda row: None
                        )

                        logger.info('running flow')
                        await self.run_flow(flow, tasks)
                        await self.header_mappings.refresh(request)
                        logger.info('flow done')
                    except Exception:
                        await poster.post_failure(traceback.format_exc())
                        raise
                    finally:
                        for task in tasks:
                            await asyncio.gather(task)
                except Exception:
                    logger.exception('Error while executing')
                finally:
                    try:
                        await resp.send('close')
                    except Exception as e:
                        logger.error('Error while closing: %s', e)
                    return resp
        except Exception as e:
            logger.error('Error while finalizing: %s', e)

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
