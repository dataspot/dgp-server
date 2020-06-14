import os
import json
import hashlib
import asyncio
from copy import deepcopy

from dataflows import Flow, schema_validator, ValidationError, checkpoint


from dgp.core import Config
from dgp.config.consts import RESOURCE_NAME

from .poster import Poster

from .line_selector import LineSelector


def row_validator(phase, poster: Poster, tasks):

    def on_error(res_name, row, i, e, f):
        if hasattr(e, 'cast_error'):
            errors = list(map(str, e.cast_error.errors))
        elif hasattr(e, 'errors'):
            errors = e.errors
        else:
            errors = []
        if len(errors) == 0:
            errors.append(str(e))
        row['__errors'] = errors
        row['__errors_field'] = f .name if f is not None else None
        return True

    def func(package):
        yield package.pkg
        for res in package:
            yield schema_validator(res.res, res, on_error=on_error)
    return func


def row_sender(phase, poster: Poster, tasks):
    ls = LineSelector()

    def func(package):
        resource = next(iter(filter(lambda res: res.name == RESOURCE_NAME,
                                    package.pkg.resources)))
        field_names = [f.name for f in resource.schema.fields]
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
                elif i % 10 == 0:
                    tasks.append(poster.post_row_count(phase, i))
                yield deepcopy(row)
            for i, row in buffer:
                if i > max_sent:
                    tasks.append(poster.post_row(phase, i, row))
            tasks.append(poster.post_done(phase))

        for i, rows in enumerate(package):
            if package.pkg.resources[i].name == RESOURCE_NAME:
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
