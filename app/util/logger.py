import logging

__all__ = (
    'configure_console_logger',
    'LogMixin',
)


def configure_console_logger(logger, level=logging.INFO):
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt='{asctime} - {name} - {funcName} - {levelname} - {message}',
                                  datefmt='%Y-%m-%d %H:%M:%S',
                                  style='{'
                                  )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)

class LogMixin:
    def __init__(self, **kwargs):
        super().__init__(**kwargs) 
        self._logger = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
        configure_console_logger(self._logger)

    @property
    def logger(self) -> logging.Logger:
        try:
            return self._logger
        except AttributeError:
            self._logger = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
            return self._logger


# class LogMixin:
#     def __init__(self, class_file_path: str, **kwargs):
#         super().__init__(**kwargs)
#         self.class_file_path = Path(class_file_path)
#         self._logger = logging.getLogger(self.logger_name)
#         configure_console_logger(self._logger)
#
#     @property
#     def logger(self):
#         return self._logger
#
#     @property
#     def class_parent(self) -> list:
#         return self.class_file_path.absolute().relative_to(PROJECT_PATH).parts[:-1]
#
#     @property
#     def logger_name(self) -> str:
#         return f"{'.'.join(self.class_parent)}.{self.__class__.__name__}"

