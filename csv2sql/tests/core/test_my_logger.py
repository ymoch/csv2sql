from unittest import TestCase
from logging import Logger

from nose.tools import ok_

from csv2sql.core.my_logging import get_logger


class TestGetLogger(TestCase):
    @staticmethod
    def test():
        logger = get_logger()
        ok_(logger)
        ok_(isinstance(logger, Logger))
