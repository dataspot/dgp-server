from .log import logger

from sqlalchemy.exc import DatabaseError
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy.engine import ResultProxy


from dataflows import Flow, add_computed_field, dump_to_sql

from dgp.config.consts import CONFIG_TAXONOMY_ID, CONFIG_SHEET, \
    RESOURCE_NAME, CONFIG_URL, CONFIG_PRIMARY_KEY


def clear_by_source(engine: Engine, table_name, value, field_name):
    index_name = table_name + '__s'

    def func(package):
        yield package.pkg
        for i, resource in enumerate(package):
            if i == 0:
                with engine.connect() as conn:
                    if conn.engine.driver != 'pysqlite':
                        # Create index in DB (unless it's sqlite)
                        s = text('create index ' +
                                    f'"{index_name}" on "{table_name}" ({field_name})')
                        try:
                            logger.info('CREATING INDEX')
                            conn.execute(s)
                            logger.info('DONE CREATING INDEX')
                        except DatabaseError as e:
                            logger.error('Failed to create index %s', e)
                    s = text(f'delete from "{table_name}" where {field_name}=:value'
                             ).params(value=value)
                    try:
                        logger.info('DELETING PAST ROWS with %s "%s"', field_name, value)
                        result: ResultProxy = conn.execute(s)
                        logger.info('DONE DELETING, %d rows', result.rowcount)
                    except DatabaseError as e:
                        logger.error('Failed to remove rows %s', e)
            yield resource

    return func


def append_to_primary_key(*fields):
    def func(package):
        res = None
        for r in package.pkg.descriptor['resources']:
            if r['name'] == RESOURCE_NAME:
                res = r
        assert res is not None
        schema = res.setdefault('schema', {})
        pk = schema.setdefault('primaryKey', [])
        for f in fields:
            if f not in pk:
                pk.append(f)
        yield package.pkg
        yield from package
    return func


def get_source(config):
    source = config.get(CONFIG_URL)
    if CONFIG_SHEET in config:
        source += '#sheet-{}'.format(config.get(CONFIG_SHEET))
    return source


def publish_flow(config, engine, mode='append', fast=False, source=None, source_field_name='_source'):
    if not config.get(CONFIG_TAXONOMY_ID):
        return None
    primaryKey = [f.replace(':', '-') for f in config.get(CONFIG_PRIMARY_KEY)]
    table_name = config.get(CONFIG_TAXONOMY_ID).replace('-', '_')
    if source is None:
        source = get_source(config)
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
            *([
                append_to_primary_key(*primaryKey) if len(primaryKey) > 0 else None,
                clear_by_source(engine, table_name, source, source_field_name),
                dump_to_sql(
                    dict([
                        (table_name, {
                            'resource-name': RESOURCE_NAME,
                            'mode': 'append'
                        })
                    ]),
                    engine=engine,
                    batch_size=1000 if fast else 10,
                ),
            ] if mode == 'append' else [
                dump_to_sql(
                    dict([
                        (table_name, {
                            'resource-name': RESOURCE_NAME,
                            'mode': 'update'
                        })
                    ]),
                    engine=engine,
                    batch_size=1000 if fast else 10,
                    use_bloom_filter=fast,
                ),
            ])
        )
