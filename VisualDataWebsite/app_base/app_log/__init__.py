# coding=utf-8

import logging
import logging.handlers
import os
import errno

from log import LogFormatter, DayRotatingFileHandler


# log default settings
_LOG_DEFAULT_SETTINGS = {
    'log_level': 'INFO',
    'log_to_stderr': True,
    'log_dir': os.path.abspath(os.path.join(os.getcwd(), 'log')),
    'log_file_prefix': str(os.getpid()) + '_',
    'log_file_postfix': '.log',
    'log_file_num_backups': 20
}


def trace(msg, *args):
    pass


def _format_msg_sen(name, content, log_level, **kwargs):
    msg = 'FUNCTION:{%s} 【%s】>> {%s}' % (str(name), log_level, str(content))
    for key, values in kwargs.items():
        msg += " | %s: {%s}" % (str(key), str(values))
    return msg


def error_sen(name, content, **kwargs):
    logging.getLogger('_log').error(_format_msg_sen(name, content, "ERROR", **kwargs))


def info_sen(name, content, **kwargs):
    logging.getLogger('_log').info(_format_msg_sen(name, content, "INFO", **kwargs))


def _format_msg(error_code, title, content, params, exception_info=''):
    return '[%s] [%s]: [%s]; $$params: [%s]; $$error: [%s]' % (
        error_code, title, content, params, exception_info)


def info(error_code, title, content, params, exception_info=''):
    logging.getLogger('_log').info(_format_msg(
        error_code, title, content, params, exception_info))


def debug(error_code, title, content, params, exception_info=''):
    logging.getLogger('_log').debug(_format_msg(
        error_code, title, content, params, exception_info))


def warn(error_code, title, content, params, exception_info=''):
    logging.getLogger('_log').warn(_format_msg(
        error_code, title, content, params, exception_info))


def error(error_code, title, content, params, exception_info=''):
    logging.getLogger('_log').error(_format_msg(
        error_code, title, content, params, exception_info))


def critical(error_code, title, content, params, exception_info=''):
    logging.getLogger('_log').critical(_format_msg(
        error_code, title, content, params, exception_info))


'''
log_options = dict(
    log_level='WARN',
    log_to_stderr=True,
    log_dir=os.path.join(base_dir, 'log'),
    log_file_prefix='file_prefix_',
    log_file_postfix='.log',
    log_file_num_backups=20
)
enable_pretty_logging(options=log_options)
'''


def enable_pretty_logging(options=None, logger=None):
    if not options:
        options = _LOG_DEFAULT_SETTINGS

    if logger is None:
        logger = logging.getLogger('_log')

    log_dir = options['log_dir']
    try:
        os.makedirs(log_dir)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(log_dir):
            pass
        else:
            raise

    log_file_prefix = options.get('log_file_prefix')
    log_path = os.path.join(log_dir, log_file_prefix)

    logger.setLevel(options.get('log_level', 'INFO').upper())
    file_handler = DayRotatingFileHandler(
        filename=log_path,
        suffix='%Y%m%d',
        postfix=options.get('log_file_postfix'),
        backup_count=options.get('log_file_num_backups', 20))
    file_handler.setFormatter(LogFormatter(color=False))
    logger.addHandler(file_handler)

    if options.get('log_to_stderr', False):
        ch = logging.StreamHandler()
        ch.setFormatter(LogFormatter())
        logger.addHandler(ch)


if __name__ == '__main__':
    enable_pretty_logging()
    info_sen("test_func", "sdfasdfsfs", url="ssfdf", time="2017")
    error_sen("test_func", "dddd")