from loguru import logger


def setup_logger():
    logger.remove()
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level="INFO",
        colorize=True,
        backtrace=False,
        diagnose=False,
    )
    return logger
