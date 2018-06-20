import sqlalchemy
import os


class DbConnection(object):
    def __init__(self):
        self.connect(self.creds_from_env())

    def creds_from_env(self):
        creds = {}

        creds['host'] = os.getenv('EST_HOST')
        creds['login'] = os.getenv('EST_LOGIN')
        creds['port'] = os.getenv('EST_PORT')
        creds['password'] = os.getenv('EST_PASSWORD')
        creds['database'] = os.getenv('EST_DATABASE')

        return creds

    def connect(self, creds):
        self.engine = sqlalchemy.create_engine('postgres://%s:%s@%s:%s/%s' %
            (creds['login'], creds['password'], creds['host'], creds['port'], creds['database']))

    def execute(self, sql, parameters=None):
        if not parameters:
            return self.engine.execute(sql)

        query = sqlalchemy.text(sql)
        return self.engine.execute(query, **parameters)
