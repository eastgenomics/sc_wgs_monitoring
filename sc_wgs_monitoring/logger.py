import logging.config


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
    logging.config.dictConfig(LOGGING)
