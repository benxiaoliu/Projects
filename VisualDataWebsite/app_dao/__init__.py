# -*- coding: UTF-8 -*-
# __author__ = 'Benxiao'
from traceback import format_exc
import DBUtils.PooledDB
from app_base.app_log import error
from app_base.utils import get_string, sequence_to_string
from app_base.utils.singleton import Singleton
import pymssql
from settings import DB_CONFIG, DB_CONN_NUM
from contextlib import contextmanager


class DBPool(object):
    __metaclass__ = Singleton
    used_conn = 0

    def __init__(self):
        super(DBPool, self).__init__()

        self.pool = None
        self.config = DB_CONFIG

        self.min_cached = DB_CONN_NUM
        self.max_connections = 100
        self.blocking = True

        self.init_pool()

    def init_pool(self):
        self.pool = DBUtils.PooledDB.PooledDB(
            pymssql, mincached=self.min_cached,
            maxconnections=self.max_connections,
            blocking=self.blocking, **self.config)

    def connection(self):
        conn = self.pool.connection()
        return conn


class DBConn(object):
    def __init__(self, dict_cursor=False, server_cursor=False):
        self.cursor = None
        self.dict_cursor = None
        self.server_cursor = None
        pool = DBPool()
        self.conn = pool.connection()
        self.set_cursor(dict_cursor, server_cursor)

    def __del__(self):
        self.close()

    def close(self):
        try:
            if getattr(self, 'cursor', None) is not None:
                self.cursor.close()
                self.cursor = None
            if getattr(self, 'conn', None) is not None:
                self.conn.close()
                self.conn = None
        except Exception as e:
            error(15004, 'close_db_connection', get_string(e), '', format_exc())

    def get_message(self):
        for x in self.cursor.messages:
            try:
                if isinstance(x[1], (tuple, list)):
                    yield x[1][2]
                else:
                    yield x[1].args[1]
            except:
                yield ''

    def set_cursor(self, dict_cursor, server_cursor):
        if dict_cursor == self.dict_cursor and \
                        server_cursor == self.server_cursor:
            return
        self.dict_cursor = dict_cursor
        self.server_cursor = self.server_cursor

        self.cursor = self.conn.cursor()

    def commit(self):
        try:
            self.conn.commit()
            return True
        except Exception as e:
            error(15002, 'commit_execute', get_string(e), '', format_exc())
            return False

    def rollback(self):
        try:
            self.conn.rollback()
        except Exception as e:
            print e
            error(15003, 'rollback_execute', get_string(e), '', format_exc())

    def execute(self, query, args=None, kwargs=None):
        self.cursor.execute(query, args or kwargs)

    def execute_rowcount(self, query, args=None, kwargs=None):
        self.cursor.execute(query, args or kwargs)
        return self.cursor.rowcount

    def execute_lastrowid(self, query, args=None, kwargs=None):
        self.cursor.execute(query, args or kwargs)
        return self.cursor.lastrowid

    def executemany(self, query, args=None):
        self.cursor.executemany(query, args)

    def executemany_rowcount(self, query, args=None):
        self.cursor.executemany(query, args)
        return self.cursor.rowcount

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def execute_fetchall(self, query, args=None):
        try:
            self.execute(query, args)
            return self.fetchall()
        except Exception as e:
            error(15000, 'execute_fetchall', query, sequence_to_string(args), get_string(e))
        return None

    def execute_fetchone(self, query, args=None):
        try:
            self.execute(query, args)
            return self.fetchone()
        except Exception as e:
            error(15000, 'execute_fetchone', query, sequence_to_string(args), get_string(e))
        return None

    def insert(self, query, args=None):
        try:
            return self.execute_rowcount(query, args)
        except Exception as e:
            error(15005, 'execute_insert', query, sequence_to_string(args), get_string(e))
        return 0

    def insert_lastrowid(self, query, args=None):
        try:
            return self.execute_lastrowid(query, args)
        except Exception as e:
            error(15005, 'execute_insert', query, sequence_to_string(args), get_string(e))
        return 0

    def insert_many(self, query, args=None):
        try:
            return self.executemany_rowcount(query, args)
        except Exception as e:
            error(15005, 'execute_insert', query, sequence_to_string(args), get_string(e))
        return 0

    def update(self, query, args=None):
        try:
            self.execute(query, args)
            return True
        except Exception as e:
            error(15006, 'execute_update', query, sequence_to_string(args), get_string(e))
        return False

    def update_rowcount(self, query, args=None):
        try:
            return self.execute_rowcount(query, args)
        except Exception as e:
            error(15006, 'execute_update', query, sequence_to_string(args), get_string(e))
        return 0

    def update_many(self, query, args=None):
        try:
            self.executemany(query, args)
            return True
        except Exception as e:
            error(15006, 'execute_update', query, sequence_to_string(args), get_string(e))
        return False

    def update_many_rowcount(self, query, args=None):
        try:
            return self.executemany_rowcount(query, args)
        except Exception as e:
            error(15006, 'execute_update', query, sequence_to_string(args), get_string(e))
        return 0

    def delete(self, query, args=None):
        try:
            self.execute(query, args)
            return self.cursor.rowcount
        except Exception as e:
            error(15007, 'execute_delete', query, sequence_to_string(args), get_string(e))
        return 0

    def query_for_str(self, query, args=None):
        result = self.execute_fetchone(query, args)
        if not result:
            return ''

        if self.dict_cursor:
            return get_string(result.popitem()[1])
        else:
            return get_string(result[0])


@contextmanager
def db_conn_guard(dict_cursor=False, server_cursor=False):
    conn = DBConn(dict_cursor, server_cursor)
    yield conn
    conn.close()


def db_delete(query, args=None):
    with db_conn_guard() as conn:
        result = conn.delete(query, args)
        if result:
            conn.commit()
    return result


def db_insert(query, args=None):
    with db_conn_guard() as conn:
        result = conn.insert(query, args)
        if result:
            conn.commit()
    return result


def db_insert_lastrowid(query, args=None):
    with db_conn_guard() as conn:
        result = conn.insert_lastrowid(query, args)
        if result:
            conn.commit()
    return result


def db_insert_many(query, args=None):
    with db_conn_guard() as conn:
        result = conn.insert_many(query, args)
        if result:
            conn.commit()
    return result


def db_query_for_list(query, args=None):
    with db_conn_guard() as conn:
        result = conn.execute_fetchall(query, args)
        if not result:
            result = []
    return result


def db_query_for_one(query, args=None):
    with db_conn_guard() as conn:
        result = conn.execute_fetchone(query, args)
        if not result:
            result = ""
    return result[0]


if __name__ == '__main__':

    sql = ""
    db_insert_many()