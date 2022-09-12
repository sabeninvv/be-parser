def add_vals2env(config_separator: str = ";"):
    from os import environ, sep
    from os.path import realpath, join

    root_path = realpath(__file__).split("src")[0]
    tail = sep.join(('src', 'config', 'config.setenv'))
    try:
        with open(join(root_path, tail), "r") as f:
            for row in f.readlines():
                row = row.strip()
                if row:
                    key, val, *_ = row.split(config_separator)
                    environ.setdefault(key, val)
            del row, key, val
    except FileNotFoundError:
        pass
    except Exception as ex:
        print(ex)
    return root_path


ROOT_PATH = add_vals2env()
from .config import *
