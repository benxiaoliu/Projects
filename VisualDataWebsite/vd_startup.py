# coding:utf8
import sys
from os import path

import gevent.pywsgi
from gevent import monkey

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.options import options, define
from tornado.web import Application
from tornado.wsgi import WSGIApplication

from app_base.app_log import enable_pretty_logging, logging

from settings import DEBUG_PROJECT, BASE_DIR

from app_handler.index_handler import IndexHandler
from app_handler.sync_up_handler import SyncUpHandler
from app_handler.show_feature_difference_handler import  ShowFeatureDifferenceHandler

monkey.patch_all()

define(name='port', default=8002, type=int, help='run on the given port')
"""
+: one or many times
?: 0/once
*: zero or multiple times
"""


def get_application():
    kwargs = dict(
        handlers=[
            (r'^/(index)?(.html)?$', IndexHandler),  # homePage
            (r'^/syncup$', SyncUpHandler),  # sync up database
            (r'^/featureDifference$', ShowFeatureDifferenceHandler),  # show features difference among selected Exps

        ],
        template_path=path.join(BASE_DIR, 'web/template'),
        static_path=path.join(BASE_DIR, 'web/static'),
        cookie_secret='C4+ZSGiq/gRm06wcOo=',
        debug=DEBUG_PROJECT,
        # xsrf_cookies=True
    )
    if DEBUG_PROJECT:
        return Application(**kwargs)
    else:
        return WSGIApplication(**kwargs)


options.parse_command_line()
log_options = dict(
    log_level='WARN',
    log_to_stderr=True,
    log_dir=path.join(BASE_DIR, 'log'),
    log_file_prefix='vd_',
    log_file_postfix='.log',
    log_file_num_backups=20
)
enable_pretty_logging(options=log_options)

application = get_application()


def startup():
    reload(sys)
    print 'startup VisualDataWebsite %s...' % options.port

    if DEBUG_PROJECT:
        http_server = HTTPServer(application, xheaders=True)
        http_server.listen(options.port, '0.0.0.0')
        IOLoop().instance().start()
    else:
        gevent.pywsgi.WSGIServer(('', options.port), application, log=None,
                                 error_log=logging.getLogger()).serve_forever()


if __name__ == '__main__':
    startup()
