import os
import logging

from aiohttp import web

from .blueprint import DgpServer


BASE_PATH = os.environ.get('BASE_PATH', '/var/dgp')
DB_URL = os.environ.get('DATABASE_URL')

app = web.Application()
app.add_subapp('/api', DgpServer(BASE_PATH, DB_URL))

logging.getLogger().setLevel(logging.INFO)

if __name__ == "__main__":
    web.run_app(app, host='127.0.0.1', port=8000, access_log=logging.getLogger())
