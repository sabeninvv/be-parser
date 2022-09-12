from functools import wraps
import logging
# from pythonjsonlogger.jsonlogger import JsonFormatter
from os import getenv
from typing import Any


# class ElkJsonFormatter(JsonFormatter):
#     def add_fields(self, log_record, record, message_dict):
#         super(ElkJsonFormatter, self).add_fields(log_record, record, message_dict)
#         log_record['source'] = f"{record.filename}.{record.funcName}.{record.lineno}"
#         log_record['level'] = record.levelname
#         log_record['app_id'] = f"ai.{getenv('MODULE_NAME', 'noModuleName').lower()}"
#         log_record['app_version'] = getenv("MODULE_VERSION", "noVersion")


def get_logger(logger_name: str):
    """
    Создать и настроить логгер для ELK
    """
    __logger = logging.getLogger(logger_name)
    __logger.setLevel(logging.INFO)
    log_handler = logging.StreamHandler()
    # formatter = ElkJsonFormatter()
    # log_handler.setFormatter(formatter)
    __logger.addHandler(log_handler)
    return __logger


def curse_loggs(__logger: logging.Logger, value_in_case_error: Any = None):
    """
    Обёртка логирования над функцией
    https://stackoverflow.com/questions/11731136/class-method-decorator-with-self-arguments
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as ex:
                __logger.error(ex, exc_info=True)
                return value_in_case_error
        return wrapper
    return decorator


logger = get_logger(getenv("MODULE_NAME", __name__))
