# -*- coding: utf-8 -*-
import datetime
from pymongo import MongoClient

import settings
from tasks import create_order

order_coll = MongoClient(settings.MONGO_URI)[settings.TEST_PAY_DB][settings.ORDER_COLL]


class TestCreateOrder:

    @classmethod
    def teardown_method(cls):
        return MongoClient(settings.MONGO_URI).drop_database(settings.TEST_PAY_DB)

    def test_create_order(cls):
        create_order(order_coll)
        orders = list(order_coll.find())
        assert len(orders) == 1
        assert orders[0]['price'] == 1

    def test_create_order_with_time(cls):
        created_time = datetime.datetime(2017, 1, 1)
        create_order(order_coll, created_time)
        assert order_coll.find_one()['created_time'] == created_time
