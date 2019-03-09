import os
import json
import hashlib
import asyncio

from dataflows import Flow, schema_validator, ValidationError, checkpoint


from dgp.core import Config

from .poster import Poster

from .line_selector import LineSelector


def row_validator(phase, poster: Poster, tasks):

    def wrapped_yielder(it):
        try:
            for i in it:
                yield i
        except ValidationError as e:
            errors = list(map(str, e.cast_error.errors))
            if len(errors) == 0:
                errors.append(str(e.cast_error))
            tasks.append(poster.post_row(phase, e.index, e.row,
                                         errors=errors))

    def func(package):
        yield package.pkg
        for res in package:
            yield wrapped_yielder(schema_validator(res.res, res))
    return func


def row_sender(phase, poster: Poster, tasks):
    ls = LineSelector()

    def func(package):
        field_names = [f['name']
                       for f in
                       package.pkg.resources[-1].descriptor['schema']['fields']]
        tasks.append(poster.post_row(phase, -1, field_names))
        yield package.pkg

        def sender(rows):
            buffer = []
            max_sent = -1
            for i, row in enumerate(rows):
                buffer.append((i, row))
                if len(buffer) > 10:
                    buffer.pop(0)
                if ls(i):
                    tasks.append(poster.post_row(phase, i, row))
                    max_sent = i
                elif i % 100 == 0:
                    tasks.append(poster.post_row_count(phase, i))
                yield row
            for i, row in buffer:
                if i > max_sent:
                    tasks.append(poster.post_row(phase, i, row))

        for i, rows in enumerate(package):
            if i == len(package.pkg.resources) - 1:
                yield sender(rows)
            else:
                yield rows
    return func


def post_flow(phase, poster, tasks, config: Config, cache=False):
    if cache:
        config = config._unflatten()

        config_json = [config.get('source'), config.get('structure')]
        config_json = json.dumps(config_json, sort_keys=True)
        checkpoint_name = hashlib.md5(config_json.encode('utf8')).hexdigest()

        if config.get('source'):
            path = config.get('source').get('path')
            if path:
                checkpoint_name += '_' + os.path.basename(path)

        cache = [checkpoint(checkpoint_name)]
    else:
        cache = []
    steps = [
        row_validator(phase, poster, tasks)
    ] + cache + [
        row_sender(phase, poster, tasks)
    ]
    return Flow(
        *steps
    )
