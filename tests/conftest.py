# -*- coding: utf-8 -*-
import pytest
from pymongo import MongoClient

import settings


@pytest.fixture
def order_coll():
    yield MongoClient(settings.MONGO_URI)[settings.TEST_PAY_DB][settings.ORDER_COLL]
    MongoClient(settings.MONGO_URI).drop_database(settings.TEST_PAY_DB)
