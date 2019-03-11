import asyncio
import uuid
import os
import logging
import yaml

from dataflows import Flow, dump_to_sql, add_computed_field, printer
from tabulator.exceptions import TabulatorException

from aiohttp import web
from aiohttp_sse import sse_response

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

import tableschema_sql

from dgp.core import Config, Context
from dgp.genera.consts import CONFIG_TAXONOMY_ID, RESOURCE_NAME, CONFIG_URL, CONFIG_SHEET
from dgp.genera.simple import SimpleDGP
from dgp.taxonomies import TaxonomyRegistry

from .poster import Poster
from .row_sender import post_flow

from dataflows.helpers.extended_json import ejson as json

tableschema_sql.writer.BUFFER_SIZE = 100
BASE_PATH = os.environ.get('BASE_PATH', '/var/dgp')


def path_for_uid(uid, *args):
    return os.path.join(BASE_PATH, uid, *args)


CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
}


def sender(resp):
    async def func(item):
        await resp.send(json.dumps(item))
    return func


_engine: Engine = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        if 'DATABASE_URL' in os.environ:
            logging.info('CREATING ENGINE')
            _engine = create_engine(os.environ['DATABASE_URL'])
            logging.info('DONE')
    return _engine


def clear_by_source(engine: Engine, table_name, source):
    def func(package):
        yield package.pkg
        for i, resource in enumerate(package):
            if i == 0:
                with engine.connect() as conn:
                    s = text("delete from %s where _source=:source" % table_name)
                    try:
                        logging.info('DELETING PAST ROWS')
                        conn.execute(s, source=source)
                        logging.info('DONE DELETING')
                    except ProgrammingError as e:
                        logging.error('Failed to remove rows %s', e)
            yield resource

    return func


def buffer(num):
    def func(rows):
        _buf = []
        for row in rows:
            _buf.append(row)
            if len(_buf) > num:
                yield _buf.pop(0)
        yield from _buf
    return func


def append_to_primary_key(*fields):
    def func(package):
        res = package.pkg.resources[-1]
        schema = res.descriptor.get('schema', {})
        schema.setdefault('primaryKey', []).extend(fields)
        yield package.pkg
        yield from package


def publish_flow(config):
    engine = get_engine()
    table_name = config.get(CONFIG_TAXONOMY_ID).replace('-', '_')
    source = config.get(CONFIG_URL)
    if CONFIG_SHEET in config:
        source += '#sheet-{}'.format(config.get(CONFIG_SHEET))
    if engine is not None:
        return Flow(
            add_computed_field(
                [
                    dict(
                        target='_source',
                        with_=source,
                        operation='constant',
                    )
                ],
                resources=RESOURCE_NAME
            ),
            append_to_primary_key('_source'),
            buffer(1000),
            clear_by_source(engine, table_name, source),
            dump_to_sql(
                dict([
                    (table_name, {
                        'resource-name': RESOURCE_NAME,
                        'mode': 'append'
                    })
                ]),
                engine=engine,
            ),
        )


async def run_flow(flow, tasks):
    ds = flow.datastream()
    for res in ds.res_iter:
        for row in res:
            while len(tasks) > 0:
                task = tasks.pop(0)
                await asyncio.gather(task)


async def events(request: web.Request):
    loop = request.app.loop

    uid = request.match_info['uid']
    error_code = None
    exception = None
    try:
        async with sse_response(request, headers=CORS_HEADERS) as resp:
            try:
                config = Config(path_for_uid(uid, 'config.yaml'))
                taxonomy_registry = TaxonomyRegistry('taxonomies/index.yaml')
                context = Context(config, taxonomy_registry)
                poster = Poster(uid, sender(resp))

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
                        publish_flow(config),
                        post_flow(3, poster, tasks, config)
                    )
                    flow = dgp.flow()

                    await run_flow(flow, tasks)

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


async def config(request: web.Request):
    body = await request.json()
    uid = request.query.get('uid')
    if uid:
        if not os.path.exists(path_for_uid(uid)):
            uid = None
    if not uid:
        uid = uuid.uuid4().hex
        if not os.path.exists(path_for_uid(uid)):
            os.mkdir(path_for_uid(uid))

    with open(path_for_uid(uid, 'config.yaml'), 'w') as conf:
        yaml.dump(body, conf)
    return web.json_response({'ok': True, 'uid': uid}, headers=CORS_HEADERS)


async def config_options(request: web.Request):
    return web.json_response({}, headers=CORS_HEADERS)


app = web.Application()
app.router.add_route('GET', '/events/{uid}', events)
app.router.add_route('POST', '/config', config)
app.router.add_route('OPTIONS', '/config', config_options)

logging.getLogger().setLevel(logging.INFO)

if __name__ == "__main__":
    web.run_app(app, host='127.0.0.1', port=8000, access_log=logging.getLogger())
