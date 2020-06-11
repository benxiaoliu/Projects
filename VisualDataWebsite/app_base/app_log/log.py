# coding:utf8
import os
import sys
import time
import logging
import logging.handlers

try:
    import curses
except ImportError:
    curses = None


if type('') is not type(b''):
    def u(s):
        return s

    bytes_type = bytes
    unicode_type = str
    basestring_type = str
else:
    def u(s):
        return s.decode('unicode_escape')

    bytes_type = str
    unicode_type = unicode
    basestring_type = basestring


def _get_unicode_string(s, d=u''):
    try:
        if isinstance(s, str):
            return s.decode('utf8')
        elif isinstance(s, unicode):
            return s
        else:
            return unicode(s)
    except:
        pass
    return d


def _stderr_supports_color():
    color = False
    if curses and hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
        try:
            curses.setupterm()
            if curses.tigetnum("colors") > 0:
                color = True
        except Exception:
            pass
    return color


class DayRotatingFileHandler(logging.handlers.BaseRotatingHandler):
    def __init__(self, filename, suffix, postfix, backup_count=0, encoding=None, delay=True):
        t = time.time()
        self.suffix = suffix
        self._baseFilename = filename
        self._postfix = postfix
        self.delay = delay
        super(DayRotatingFileHandler, self).__init__(self.get_file_name(t), 'a', encoding, delay)

        self.backup_count = backup_count
        self.rollover_time = self.compute_rollover(time.time())

    def get_file_name(self, current_time):
        return self._baseFilename + time.strftime(self.suffix, time.localtime(current_time)) + self._postfix

    @staticmethod
    def compute_rollover(current_time):
        return current_time - (current_time + 28800) % 86400 + 86400

    def get_files_to_delete(self):
        dir_name, base_name = os.path.split(self._baseFilename)
        result = [os.path.join(dir_name, file_name) for file_name in os.listdir(dir_name)
                  if file_name.startswith(base_name)]
        result.sort()
        if len(result) < self.backup_count:
            result = []
        else:
            result = result[:len(result) - self.backup_count]
        return result

    # overloaded
    def shouldRollover(self, record):
        t = int(time.time())
        if t >= self.rollover_time:
            return 1
        return 0

    # overloaded
    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        current_time = int(time.time())
        self.baseFilename = self.get_file_name(current_time)
        if self.backup_count > 0:
            for s in self.get_files_to_delete():
                os.remove(s)
        if not self.delay:
            self.stream = self._open()
        self.rollover_time = self.compute_rollover(current_time)


class LogFormatter(logging.Formatter):
    DEFAULT_FORMAT = '%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d]%(end_color)s %(message)s'
    DEFAULT_DATE_FORMAT = '%y%m%d %H:%M:%S'
    DEFAULT_COLORS = {
        logging.DEBUG: 4,  # Blue
        logging.INFO: 2,  # Green
        logging.WARNING: 3,  # Yellow
        logging.ERROR: 1,  # Red
    }

    def __init__(self, color=True, fmt=DEFAULT_FORMAT,
                 datefmt=DEFAULT_DATE_FORMAT, colors=DEFAULT_COLORS):
        logging.Formatter.__init__(self, datefmt=datefmt)
        self._fmt = fmt

        self._colors = {}
        if color and _stderr_supports_color():
            fg_color = (curses.tigetstr("setaf") or
                        curses.tigetstr("setf") or "")
            if (3, 0) < sys.version_info < (3, 2, 3):
                fg_color = unicode_type(fg_color, "ascii")

            for levelno, code in colors.items():
                self._colors[levelno] = unicode_type(curses.tparm(fg_color, code), "ascii")
            self._normal = unicode_type(curses.tigetstr("sgr0"), "ascii")
        else:
            self._normal = ''

    def format(self, record):
        try:
            message = record.getMessage()
            assert isinstance(message, basestring_type)
            record.message = _get_unicode_string(message)
        except Exception as e:
            record.message = "Bad message (%r): %r" % (e, record.__dict__)

        record.asctime = self.formatTime(record, self.datefmt)

        if record.levelno in self._colors:
            record.color = self._colors[record.levelno]
            record.end_color = self._normal
        else:
            record.color = record.end_color = ''

        formatted = self._fmt % record.__dict__

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            lines = [formatted.rstrip()]
            lines += [_get_unicode_string(ln) for ln in record.exc_text.split('\n')]
            formatted = '\n'.join(lines)
        return formatted.replace("\n", "\n    ")
