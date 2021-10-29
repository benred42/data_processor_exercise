import logging


formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# we want to log normal application functioning in addition to warnings, so
# INFO should be fine
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# create our logger
console_logger = logging.getLogger('data collection logger')
console_logger.setLevel(logging.INFO)
console_logger.addHandler(console_handler)
