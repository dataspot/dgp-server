import asyncio
import uuid
import os
import logging
import yaml

from dataflows import Flow
from tabulator.exceptions import TabulatorException

from aiohttp import web
from aiohttp_sse import sse_response

from dgp.core import Config, Context
from dgp.genera.simple import SimpleDGP
from dgp.taxonomies import TaxonomyRegistry

from .poster import Poster
from .row_sender import post_flow

from dataflows.helpers.extended_json import ejson as json


BASE_PATH = os.environ.get('BASE_PATH', '/var/datapip.es')


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
                print('ANALYZED')
                print(config._unflatten()['source'])
                print(config._unflatten()['structure'])
                if config.dirty:
                    await poster.post_config(config._unflatten())
                if not ret:
                    await poster.post_errors(list(map(list, dgp.errors)))

                dgp.post_flows = [
                    post_flow(0, poster, tasks, config, cache=True),
                    post_flow(1, poster, tasks, config),
                    post_flow(2, poster, tasks, config),
                ]
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
            except Exception:
                logging.exception('Error while closing')
            return resp


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

if __name__ == "__main__":
    web.run_app(app, host='127.0.0.1', port=8000)
