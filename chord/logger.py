import logging, sys

class MyFilter(object):
    def __init__(self, level):
        self.__level = level

    def filter(self, logRecord):
        return logRecord.levelno == self.__level


logger = logging.getLogger("TCP Logger")
logger.setLevel(logging.INFO)
#if not logger.handlers:
handler = logging._StderrHandler()

formatter = logging.Formatter('* %(levelname)s * : %(message)s')
handler.setFormatter(formatter)

handler.addFilter(MyFilter(logging.ERROR))
logger.addHandler(handler)
logger.propagate = False