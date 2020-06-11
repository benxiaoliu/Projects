# coding: utf8

from decimal import Decimal
import pickle
import json


class JSONEncoderEx(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (Decimal, )):
            return '%s' % obj
        elif hasattr(obj, "__json__"):
            return obj.__json__()
        super(JSONEncoderEx, self).default(obj)


class JSONDataEncoder(object):

    @classmethod
    def encode(cls, obj):
        return json.dumps(obj, cls=JSONEncoderEx)

    @classmethod
    def decode(cls, s):
        return json.loads(s)


class PickleDataEncoder(object):

    @classmethod
    def encode(cls, obj):
        return pickle.dumps(obj)

    @classmethod
    def decode(cls, s):
        return pickle.loads(s)
