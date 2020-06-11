# coding=utf-8

from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from inspect import stack
import cgi
import json
import random
import string
import sys
import time
import uuid

IPS_LENGTH = -2

def get_dict_items(d, *keys):
    return {k: v for k, v in get_utf8_dict(d).iteritems() if k in keys}


def convert_dict_key(convert_dict, keys_dict):
    return {keys_dict.get(k, k): v for k, v in convert_dict.iteritems()}


def gen_random_string(n):
    random_seq = string.ascii_letters + string.digits
    u_num = ''.join([random.choice(random_seq) for _ in xrange(n)])
    return u_num


def gen_random_string_1(n):
    random_seq = string.digits
    u_num = ''.join([random.choice(random_seq) for _ in xrange(n)])
    return u_num


def gen_random_string_2(n):
    random_seq = string.ascii_lowercase + string.digits
    u_num = ''.join([random.choice(random_seq) for _ in xrange(n)])
    return u_num


def random_int_str(n):
    return ''.join([str(random.randint(0, 9)) for _ in xrange(n)])


def get_bill_no():
    return datetime.now().strftime('%Y%m%d%H%M%S%f')


def json_loads_utf8_util(data):
    """
    json.loads解码后没有str,只有unicode
    此函数将unicode全部转化为utf8 str
    """
    data = json.loads(get_string(data))
    return {get_string(k): get_utf8_str(v) for k, v in data.iteritems()}


def get_utf8_str(s):
    if isinstance(s, unicode):
        s = s.encode('utf8')
    return s


def get_utf8_dict(data):
    return {get_string(k): get_utf8_str(v) for k, v in data.iteritems()}


class HttpJsonEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, (Decimal, )):
            return '%s' % o
        return super(HttpJsonEncoder, self).default(o)


def json_dumps_util(data):
    return json.dumps(data, cls=HttpJsonEncoder)


def load_json_util(data):
    return json_loads_utf8_util(json_dumps_util(data))


def load_json_array_util(data):
    return [json_loads_utf8_util(json_dumps_util(v)) for v in data]


def is_string(s):
    return isinstance(s, basestring)


def is_float_string(float_string):
    if not is_string(float_string):
        return False
    try:
        f = float(float_string)
        return True
    except:
        pass
    return False


def is_int_string(int_string):
    if not is_string(int_string):
        return False
    if int_string.startswith('-'):
        return int_string[1:].isdigit()
    return int_string.isdigit()


def is_time_string(time_string):
    if not is_int_string(time_string) or len(time_string) != 14:
        return False
    return True


def is_ip_string(ip_string):
    return is_string(ip_string) and ip_string.count('.') == 3


def is_date_string(date_string):
    if not is_int_string(date_string) or len(date_string) != 8:
        return False
    return True


def sequence_to_string(data):
    if isinstance(data, (list, tuple)):
        return [sequence_to_string(x) for x in data]
    elif isinstance(data, dict):
        return {sequence_to_string(k): sequence_to_string(v)
                for k, v in data.iteritems()}
    return get_string(data)


def sequence_to_unicode_string(data):
    if isinstance(data, (list, tuple)):
        return [sequence_to_unicode_string(x) for x in data]
    elif isinstance(data, dict):
        return {sequence_to_unicode_string(k): sequence_to_unicode_string(
            v) for k, v in data.iteritems()}
    return get_unicode_string(data)


def get_int(i, d=0):
    try:
        return int(i)
    except:
        pass
    return d


def get_float(f, d=0.0):
    try:
        return float(f)
    except:
        pass
    return d


def get_bool(b, d=False):
    try:
        return bool(b)
    except:
        pass
    return d


def get_string(s, d=''):
    try:
        if isinstance(s, str):
            return s
        elif isinstance(s, unicode):
            return s.encode('utf8')
        elif isinstance(s, Decimal):
            return str(s)
        elif isinstance(s, float):
            return '%.2f' % s
        elif isinstance(s, bool):
            return '1' if s else '0'
        elif s is None:
            return ''
        else:
            return str(s)
    except:
        pass
    return d


def get_unicode_string(s, d=u''):
    try:
        if isinstance(s, str):
            return s.decode('utf8')
        elif isinstance(s, unicode):
            return s
        elif isinstance(s, Decimal):
            return unicode(s.quantize(Decimal('1.000')))
        elif isinstance(s, float):
            return u'%.3f' % s
        elif isinstance(s, bool):
            return u'0' if s else u'1'
        elif s is None:
            return u''
        else:
            return unicode(s)
    except:
        pass
    return d


def get_decimal(dec, d=Decimal(0)):
    try:
        if isinstance(dec, Decimal):
            return dec
        return Decimal(str(dec))
    except:
        pass
    return d


def get_decimal_with_prec(num, d=Decimal(0), prec=28, rounding=ROUND_HALF_UP):
    """
    e.g 1:
    > print get_decimal_with_prec('1.5', prec=3)
    1.500

    e.g2:
    > print get_decimal_with_prec('1.566', prec=2)
    1.57
    """
    prec = max(prec, 0)

    try:
        dec_prcs = Decimal('1.' + '0' * prec)
        if isinstance(num, Decimal):
            return num.quantize(dec_prcs, rounding=rounding)
        else:
            return Decimal(str(num)).quantize(dec_prcs, rounding=rounding)
    except:
        pass
    return d


def get_time_object(t='', d=None):
    if is_time_string(t):
        return datetime(year=int(t[:4]), month=int(t[4:6]), day=int(t[6:8]),
                        hour=int(t[8:10]), minute=int(t[10:12]), second=int(t[12:14]))
    return d


def get_time_string(t='', d=None):
    if isinstance(t, (float,)):
        return time.strftime('%Y%m%d%H%M%S', time.localtime(t))
    elif isinstance(t, (datetime,)):
        return t.strftime('%Y%m%d%H%M%S')

    return time.strftime('%Y%m%d%H%M%S', time.localtime(d))


def format_time_string(t):
    return ''.join([
        t[:4], '-',
        t[4:6], '-',
        t[6:8], ' ',
        t[8:10], ':',
        t[10:12], ':',
        t[12:14]
    ])


def get_date_object(s=''):
    if isinstance(s, basestring):
        if is_date_string(s):
            return date(year=int(s[:4]), month=int(s[4:6]), day=int(s[6:8]))
    return date.today()


def get_datetime(time_string=''):
    if not time_string or not is_time_string(time_string):
        return datetime.now()
    return datetime.strptime(time_string, '%Y%m%d%H%M%S')


def get_date_string(t='', d=None):
    if isinstance(t, (float,)):
        return time.strftime('%Y%m%d', time.localtime(t))
    elif isinstance(t, (datetime, date)):
        return t.strftime('%Y%m%d')
    return time.strftime('%Y%m%d', time.localtime(d))


def string_to_datetime(time_string=''):
    if is_time_string(time_string):
        return datetime.strptime(time_string, '%Y%m%d%H%M%S')
    elif is_date_string(time_string):
        return datetime.strptime(time_string + '000000', '%Y%m%d%H%M%S')
    return datetime.now()


def calc_list_page(total_count, page_index, page_size):
    page_size = get_int(page_size)
    total_page = (total_count + page_size - 1) / page_size
    page_index = get_int(page_index, 1)
    if page_index > total_page:
        page_index = total_page
    if page_index <= 0:
        page_index = 1
    return total_page, page_index, (page_index - 1) * page_size


def get_current_function_name():
    try:
        raise Exception
    except:
        f = sys.exc_info()[2].tb_frame.f_back
    return f.f_code.co_name


def get_permutation_count(a, r):
    count = 1
    for x in xrange(a, a - r, -1):
        count *= x
    return count


def get_combination_count(c, r):
    if 0 > r:
        return 0
    elif 0 == r:
        return 1
    elif 1 == r:
        return c
    elif 2 == r:
        return c * (c - 1) / 2
    return get_permutation_count(c, r) / get_permutation_count(r, r)


def get_combination_list_count(lc, lr):
    if not (len(lc) == len(lr) == 2):
        return 0

    repeat_count = 0
    for x in lc[0]:
        if x in lc[1]:
            repeat_count += 1

    return get_combination_count(len(lc[0]), lr[0]) * get_combination_count(len(lc[1]), lr[1]) - get_combination_count(
        len(lc[0]) - 1, lr[0] - 1) * get_combination_count(len(lc[1]) - 1, lr[1] - 1) * repeat_count


def div_list(l, n):
    return [l[i:i + n] for i in xrange(0, len(l), n)]


def chain_for(array, index=0):
    if index < len(array):
        for v in array[index]:
            for item in chain_for(array, index + 1):
                yield (v,) + item
    else:
        yield ()


def continuations(iterable, r):
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    for i in xrange(n):
        if i + r > n:
            break
        yield pool[i: i + r]


def lstrip0(s):
    s = s.lstrip('0')
    if not s:
        s = '0'
    return s


def get_remote_ip(req):
    heads = req.request.headers
    ip = heads.get("X-Forwarded-For", '')
    ip = ip.split(',')
    ip = ip[0].strip()
    print ip
    if not ip:
        ip = heads.get("X-Real-IP", '')
    if not ip:
        ip = req.request.remote_ip
    return ip


def get_remote_ips(req):
    heads = req.request.headers
    ip = heads.get("X-Forwarded-For", '')
    ip = ip.split(',')
    if len(ip) > 2:
        ip = ip[:IPS_LENGTH]
    else:
        ip = ip[0]
    print ip
    if not ip:
        ip = heads.get("X-Real-IP", '')
    if not ip:
        ip = req.request.remote_ip
    return ip


def get_uuid1_safety(is_upper=True, with_bar=True):
    """打散MAC地址"""
    uuid_obj = uuid.uuid1()
    if with_bar:
        uuid_str = get_string(uuid_obj)
        id_list = uuid_str.split('-')
        if len(id_list) == 5:
            id_list[4] = id_list[4][4:] + id_list[4][0:4]

        ret_str = '-'.join(id_list)
    else:
        uuid_str = get_string(uuid_obj.hex)
        ret_str = uuid_str[0:20] + uuid_str[24:] + uuid_str[20:24]
    if is_upper:
        ret_str = ret_str.upper()
    return ret_str


def get_uuid1_1(is_upper=True, with_bar=True):
    uuid_obj = uuid.uuid1()
    if with_bar:
        uuid_str = get_string(uuid_obj)
        id_list = uuid_str.split('-')
        ret_str = '-'.join(id_list[:4])
    else:
        uuid_str = get_string(uuid_obj.hex)
        ret_str = uuid_str[0:20]
    if is_upper:
        ret_str = ret_str.upper()
    return ret_str


def xss_filter(str):
    success = True
    if isinstance(str, basestring):
        _str = str
        str = cgi.escape(str)
        if str != _str:
            success = False
    return success, str


def xss_filter_conversion(s):
    if isinstance(s, basestring) or isinstance(s, unicode):
        s = cgi.escape(s)
    return s


class TimeLogger(object):

    def __init__(self):
        self.t = datetime.now()

    def __del__(self):
        t = datetime.now()
        print stack()[1][2:4], t - self.t
        self.t = datetime.now()

    def print_time_delta(self):
        t = datetime.now()
        print stack()[1][2:4], t - self.t
        self.t = datetime.now()


def escape_string(s):
    """
    字符串防注入
    < --> &lt;
    > --> &gt;
    """
    if not s:
        return ""
    s = get_string(s)
    for k, v in ((">", "&gt;"), ("<", "&lt;")):
        s = s.replace(k, v)
    return s


def dict_format_str(d, m, *c):
    """
    dict
    model
    condition

    m:0 记录去除在c中
    m:1 记录在c中
    m:2 记录所有
    """
    s = ''
    for k, v in d.iteritems():
        if m == 0:
            if k in c:
                continue
            s += '{0}:{1};'.format(get_string(k), get_string(v))
        elif m == 1:
            if k in c:
                s += '{0}:{1};'.format(get_string(k), get_string(v))
        elif m == 2:
            s += '{0}:{1};'.format(get_string(k), get_string(v))
    return s


def cont_dict_format_str(d, m, *c):
    """
    dict
    model
    condition

    m:0 记录去除在c中
    m:1 记录在c中
    m:2 记录所有
    """
    s = ''
    for k, v in d.iteritems():
        if m == 0:
            if k in c:
                continue
            s += '{};'.format(get_string(v))
        elif m == 1:
            if k in c:
                s += '{};'.format(get_string(v))
        elif m == 2:
            s += '{};'.format(get_string(v))
    return s


def human_len(s):
    try:
        if isinstance(s, str):
            pass
        elif isinstance(s, unicode):
            s = s.encode('utf8')
        elif isinstance(s, Decimal):
            s = str(s)
        elif isinstance(s, bool):
            s = '1' if s else '0'
        elif s is None:
            s = ''
        else:
            s = str(s)
        s = s.decode('utf8').encode('utf8').decode('utf8')
        return len(s)
    except:
        pass
    return 0


if __name__ == '__main__':
    # print gen_random_string(64)
    # b = get_decimal_with_prec('1.557', prec=2)
    # print b
    # c = '%.28f' % b
    # print c
    # for i in xrange(50):
    #     print gen_random_string(4)
    # print get_uuid1_1(True, False)
    print get_decimal_with_prec('1.04943', prec=2)
