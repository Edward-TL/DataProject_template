import sys
import logging
from logging.handlers import RotatingFileHandler


logger = logging.getLogger(__name__)

log_name = 'testing.log'

logging.basicConfig(
    filename = log_name,
    encoding = 'utf-8',
    level = logging.DEBUG
    )

stdout_handler = logging.StreamHandler(stream=sys.stdout)

file_handler = RotatingFileHandler(
    log_name,
    maxBytes = 5 * 1024,
    backupCount = 1
)

format_output = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s') # <-

# Register the formatter to the stdout handler
stdout_handler.setFormatter(format_output)      # <-
logger.addHandler(stdout_handler)
logger.addHandler(file_handler)

if __name__ == '__main__':
    logger.debug('This message should go to the log file')
    logger.info('So should this')
    logger.warning('And this, too')
    logger.error('And non-ASCII stuff, too, like Øresund and Malmö')
