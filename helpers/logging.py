import logging

def set_logging(logger_name, logging_level=logging.WARN):
	logger = logging.getLogger(logger_name)
	logger.setLevel(logging.INFO)
	fh = logging.FileHandler(logger_name + ".log")
	fh.setLevel(logging_level)
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	logger.addHandler(fh)

	return logger