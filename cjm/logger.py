#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

DEFAULT_LOGGER_FORMATTER = logging.Formatter(
    fmt = '[cjm|%(levelname)8s|%(asctime)s|%(module)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

DEFAULT_LOGGER_NAME = 'cjm'

def setup_logger(name=DEFAULT_LOGGER_NAME, formatter=DEFAULT_LOGGER_FORMATTER):
    """
    Creates a logger

    :param name: Name of the logger
    :type name: str, optional
    :param formatter: logging.Formatter object which determines the log string format
    :type formatter: logging.Formatter
    """

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

def add_file_handler(filename):
    handler = logging.FileHandler(filename)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger('cjm')
    logger.addHandler(handler)
    logger.info('Started logging to %s', filename)
