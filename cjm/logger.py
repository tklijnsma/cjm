#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging, os, glob
import os.path as osp

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

def add_file_handler(filename, formatter=DEFAULT_LOGGER_FORMATTER, delete_other_handlers=False):
    handler = logging.FileHandler(filename)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger = logging.getLogger('cjm')
    if delete_other_handlers:
        logger.handlers = []
    logger.addHandler(handler)
    logger.info('Started logging to %s', filename)
    if delete_other_handlers:
        logger.info('Other logging handlers were destroyed')
    return handler

def add_rotating_file_handler(filename, formatter=DEFAULT_LOGGER_FORMATTER, delete_other_handlers=False):
    """
    Like a file handler, but rolls over the file if it already exists, and throws away
    old logs.
    """
    should_perform_rotation = osp.isfile(filename) # handler will auto-open a file, so check now if a previous file existed
    handler = RotatingFileHandler(filename)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger = logging.getLogger('cjm')
    if delete_other_handlers:
        logger.handlers = []
    logger.addHandler(handler)
    if should_perform_rotation: handler.perform_rotation()
    logger.info('Started logging to %s', filename)
    if delete_other_handlers:
        logger.info('Other logging handlers were destroyed')
    return handler


class RotatingFileHandler(logging.FileHandler):
    """
    Like a file handler, but rolls over the file if it already exists, and throws away old
    logs. Does not do any automatic rollover, `perform_rotation` must be called explicitily.
    """

    n_backups = 10

    def __init__(self, filename, **kwargs):
        super(RotatingFileHandler, self).__init__(filename, **kwargs)
        self.basename = osp.basename(filename)
        self.dirname = osp.dirname(filename)

    def get_index(self, logfile):
        """
        Gets the index of a logfile as an integer
        """
        if len(logfile) == 0:
            raise ValueError('Log filename should have a length of at least 1')
        # If the logfile name is <some_name>.log.5, the following line should
        # yield '5':
        index_str = (
            osp.basename(logfile)
            .replace(self.basename, '')
            .replace('.', '')
            )
        if len(index_str) == 0:
            # No '.d' extension found, so this must be index zero
            return 0
        else:
            try:
                return int(index_str)
            except ValueError:
                # Could not be converted to an integer, so skip this logfile
                return None

    def perform_rotation(self):
        logfiles = glob.glob(self.baseFilename + '*')
        indices = [ self.get_index(f) for f in logfiles ]
        pairs = [ (index, logfile) for index, logfile in zip(indices, logfiles) if not index is None ]
        pairs.sort()

        if len(pairs) == 0:
            # No file exists at all, so do not do any rollovers
            return

        self.close() # Inherited, make sure current log file is closed

        # Increase the counters, start with the last pair to avoid overwriting a 
        # not-to-be-replaced logfile
        for index, logfile in pairs[::-1]:
            if index == self.n_backups-1:
                # Let the logfile be overwritten if it is at the n_backups limit
                continue
            new_logfile = self.baseFilename + '.{0}'.format(index+1)
            os.rename(logfile, new_logfile)

        self._open() # Inherited, open the file again

        # Little dangerous but logger 'cjm' should already exist here, and this message helps in
        # understanding what is happening
        logging.getLogger('cjm').info(
            'Performed rollover; increased counters for %s and performed move',
            [ f for i, f in pairs ]
            )
