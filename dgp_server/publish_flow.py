import logging

from sqlalchemy.exc import ProgrammingError
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text


from dataflows import Flow, add_computed_field, dump_to_sql

from dgp.genera.consts import CONFIG_TAXONOMY_ID, CONFIG_SHEET, RESOURCE_NAME, CONFIG_URL


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
    return func


def publish_flow(config, engine):
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
