import os
import pyodbc

from config import settings


class HiveConnectionManager(object):
    __connections = {}

    @staticmethod
    def get_instance(db_name=settings.HIVE_DEFAULT_DB):
        """ Static access method. """
        HiveConnectionManager.__authenticate()
        if HiveConnectionManager.__connections.get(db_name) is None:
            HiveConnectionManager(db_name)
        cnxn = HiveConnectionManager.__connections[db_name]
        cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        cnxn.setencoding(encoding='utf-8')
        return cnxn

    def __init__(self, db_name):
        """ Virtually private constructor. """
        if HiveConnectionManager.__connections.get(db_name) is not None:
            raise Exception("This class is a singleton!")
        else:
            HiveConnectionManager.__connections.update(
                {
                    db_name: pyodbc.connect("DSN=krb", autocommit=True)
                }
            )

    @staticmethod
    def __authenticate():
        os.system('kinit -kt ~/{0}.keytab {0}@CORP.AD.CTC'.format(settings.USER))


if __name__ == "__main__":
    con = HiveConnectionManager.get_instance()
