import logging.config
from pathlib import Path


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "{levelname}|{module}|{asctime}|{message}",
            "style": "{",
        },
    },
    "handlers": {
        "error-log": {
            "level": "ERROR",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "/app/sc_wgs_monitoring/logs/errors.log",
            "formatter": "simple",
            "maxBytes": 5242880,  # 5MB
            "backupCount": 2,
        },
        "warning-log": {
            "level": "WARNING",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "/app/sc_wgs_monitoring/logs/warnings.log",
            "formatter": "simple",
            "maxBytes": 5242880,
            "backupCount": 2,
        },
        "debug-log": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "/app/sc_wgs_monitoring/logs/debug.log",
            "formatter": "simple",
            "maxBytes": 5242880,
            "backupCount": 2,
        },
    },
    # Loggers
    "loggers": {
        "basic": {
            "handlers": ["debug-log", "error-log", "warning-log"],
            "level": "DEBUG",
            "propagate": True,
        },
    },
}


def set_up_logger():
    """Set up the logger using the config dict in this file + create the log
    folder if it doesn't exist"""

    log_path = Path("/app/sc_wgs_monitoring/logs/")
    log_path.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig(LOGGING)


def log(logger: logging.Logger, type_log: str, msg: str):
    """Helper function allowing logging with multiple types of logs

    Parameters
    ----------
    logger : logging.Logger
        Logger object to log to
    type_log : str
        Type of log to write
    msg : str
        Message to write
    """

    getattr(logger, type_log)(msg)
    print(msg, flush=True)
