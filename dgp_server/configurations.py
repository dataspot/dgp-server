import os
from aiohttp import web
from sqlalchemy import (
    MetaData, Table, Column,
    String, JSON, create_engine
)
from .log import logger

__all__ = ['configuration']


meta = MetaData()

configuration = Table(
    'configurations', meta,

    Column('source', String, primary_key=True),
    Column('snippets', JSON, nullable=True),
    Column('key_values', JSON, nullable=True),
    Column('config', JSON, nullable=True),
)


async def _configs(request):
    configurations = []
    try:
        async with request.app['db'].acquire() as conn:
            try:
                configurations = await conn.execute(
                    configuration.select()
                )
                configurations = await configurations.fetchall()
                configurations = [dict(x) for x in configurations]
            except Exception:
                meta.create_all(create_engine(os.environ['DATABASE_URL']))
                raise
    except Exception:
        logger.exception('EMPTY CONFIGS %r', request.app)
    return configurations


async def configs(request):
    configurations = await _configs(request)
    res = {
        'configurations': configurations
    }
    return web.json_response(res)


class ConfigColumnTypes():

    def __init__(self, taxonomy_registry):
        pass

    async def refresh(self, request):
        pass


class ConfigHeaderMappings():

    def __init__(self, taxonomy_registry):
        self._header_mappings = {}
        for txn_id in taxonomy_registry.all_ids():
            for column_type in taxonomy_registry.get(txn_id).column_types:
                title = column_type.get('title')
                if title:
                    self._header_mappings.setdefault(txn_id, {})[title] = dict(
                        type=column_type['name']
                    )

    async def header_mapping(self, taxonomy_id, request):
        if not self._header_mappings.get(taxonomy_id):
            try:
                await self.refresh(request)
            except Exception:
                logger.exception('Failed to read header mappings from configuration, skipping')
        return self._header_mappings.get(taxonomy_id, {})

    async def fetch(self, request):
        return await _configs(request)

    async def refresh(self, request):
        configurations = await self.fetch(request)
        taxonomy_id = None
        for config in configurations:
            config = config.get('config', {})
            taxonomy_id = config.get('taxonomy', {}).get('id')
            if not taxonomy_id:
                continue
            mapping = config.get('model', {}).get('mapping')
            if not mapping:
                continue
            for m in mapping:
                name = m.get('name')
                if not name:
                    continue
                columnType = m.get('columnType')
                normalize = m.get('normalize')
                normalizeTarget = m.get('normalizeTarget')
                if normalize and normalizeTarget:
                    h = dict(
                        normalize=dict(
                            header=normalizeTarget,
                            using=normalize
                        )
                    )
                elif columnType:
                    h = dict(
                        type=columnType
                    )
                else:
                    continue
                self._header_mappings.setdefault(taxonomy_id, {})[name] = h
