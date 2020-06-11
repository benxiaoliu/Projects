# coding:utf-8
from tornado.web import RequestHandler
from traceback import format_exc
from app_base.app_log import warn
from app_base.utils import get_string, get_remote_ip, sequence_to_string
import re

ERR_DEFAULT = -1
ERR_ACCESS_DENIED = -5


class BaseHandler(RequestHandler):
    def __init__(self, application, request, **kwargs):
        super(BaseHandler, self).__init__(application, request)
        self._http404_message = 'HTTPError: HTTP 404: Not Found'
        self._http500_message = 'HTTPError: HTTP 500: Internal Server Error'
        self._command_func_dict = kwargs.get('command_dict', {})

    def get(self, *args, **kwargs):
        self.post(*args, **kwargs)

    def post(self, *args, **kwargs):
        # filter dangourous characters
        # for key, arguments in self.request.arguments.iteritems():
        #     self.request.arguments[key] = filter_xss(arguments[0])
        # result = ''
        try:
            result = self.do_command(*args, **kwargs)
        except Exception, e:
            warn(ERR_DEFAULT, 'command exception', self.request.path,
                 self.request.arguments, '\n'.join([str(e), format_exc()]))
            result = self._http500_message
        finally:
            self.write(sequence_to_string(result))

    def do_command(self, *args, **kwargs):
        command = get_string(args[0])
        user_id = get_string(args[1]) if len(args) > 1 else ''
        func = self._command_func_dict.get(command)
        if func:
            command_info = self.get_command_info(command, user_id)
            return func(command_info)
        else:
            warn(ERR_ACCESS_DENIED, 'command invalid',
                 self.request.path, self.request.arguments)
            return self._http404_message

    def get_command_info(self, command, user_id):
        params = {}
        for key, arguments in self.request.arguments.iteritems():
            params[get_string(key)] = arguments[-1]
        ip = self.get_remote_ip()
        device = self.request.headers.get('User-Agent', '')
        path = self.request.path
        token = self.get_secure_cookie('token')
        return CommandInfo(
            ip=ip, device=device, path=path, command=command, params=params,
            headers=self.request.headers, token=token, user_id=user_id)

    def get_remote_ip(self):
        return get_remote_ip(self)


class ErrorHandler(BaseHandler):
    def __init__(self, application, request, **kwargs):
        super(ErrorHandler, self).__init__(application, request, **kwargs)

    def get(self, *args, **kwargs):
        self.write(self._http404_message)

    def post(self, *args, **kwargs):
        self.write(self._http404_message)


class CommandInfo(object):
    def __init__(self, ip, device, path, command, params, headers,
                 token=None, user_id=None, user_name=None):
        super(CommandInfo, self).__init__()
        self.ip = ip
        self.device = device
        self.command = command
        self.params = params
        self.token = token
        self.user_id = user_id
        self.user_name = user_name
        self.headers = headers
        self.path = path
        self.operate_cont = ''


def get_module_command_dict(modules, pattern=r'^command_(\w+)$', ):
    command_dict = dict()

    def get_module_command(module):
        module_dict = module.__dict__
        for key in module_dict:
            r = re.search(pattern, key)
            if r:
                command_dict[r.group(1)] = module_dict[key]

    if isinstance(modules, list):
        for m in modules:
            get_module_command(m)
    else:
        get_module_command(modules)
    return command_dict


