import os

from config import settings
from app.util.logging_config import get_logger
from app.services.hive_manager import HiveConnectionManager
# from app.services.hql_queries.manager import HQLQueriesManager

logger = get_logger(__name__)


class HiveExecutorException(Exception):
    pass


class HiveExecutor(object):
    def __init__(self, repo_name=None, script_name=None):
        self.connection = None
        self.cursor = None
        # self.hql_queries_manager = HQLQueriesManager()
        self.script_name = script_name
        self.repo_name = repo_name

    def process(self, *args, **kwargs):
        try:
            self.connection = HiveConnectionManager.get_instance()
            self.cursor = self.connection.cursor()
            self.__executor()
            return True
        except Exception as ex:
            raise ex

    def process_raw_sql(self, sql):
        try:
            self.connection = HiveConnectionManager.get_instance()
            self.cursor = self.connection.cursor()
            self.__executor_raw_sql(sql)
        except Exception as ex:
            raise ex

    def validate(self, is_sensor=True, *args, **kwargs):
        """

        :param is_sensor:
        :param args:
        :param kwargs:
        :return: Return True or False if validator use as a sensor
        Return True or Raise Exception if validator used as a Post validator
        """
        try:
            self.connection = HiveConnectionManager.get_instance()
            self.cursor = self.connection.cursor()
            df = self.__executor()
            logger.info("validation process ::  is data frame empty?:: {}".format(df.empty))
            if is_sensor:
                return not df.empty
            elif not df.empty:
                return True
            else:
                raise HiveExecutorException('Validation Failed!')
        # TODO: Replace general exception with specific one
        except Exception as ex:
            raise ex

    def get_data(self):
        try:
            self.connection = HiveConnectionManager.get_instance()
            self.cursor = self.connection.cursor()
            self.__authenticate()
            logger.info('Executing {}'.format(self.script_name))
            return self.hql_queries_manager.execute_script(self.cursor, self.script_name)
        except Exception as ex:
            raise ex

    def __executor(self):
        try:
            self.__authenticate()
            logger.info('Executing {}'.format(self.script_name))
            return self.hql_queries_manager.execute_script(self.cursor, self.repo_name, self.script_name)
        except Exception as ex:
            raise ex

    def __executor_raw_sql(self, sql):
        try:
            self.__authenticate()
            self.hql_queries_manager.execute_raw_sql(self.cursor, sql)
        except Exception as ex:
            raise ex

    def __authenticate(self):
        os.system('kdestroy ; kinit -kt ~/{0}.keytab {0}@CORP.AD.CTC'.format(settings.USER))


hive_executor = HiveExecutor()