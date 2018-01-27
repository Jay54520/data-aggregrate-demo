# -*- coding: utf-8 -*-
import datetime

import settings
from tasks import create_order


class TestCreateOrder:

    def test_create_order(cls, order_coll):
        create_order(order_coll)
        orders = list(order_coll.find())
        assert len(orders) == 1
        assert orders[0][settings.PRICE] == 1

    def test_create_order_with_time(cls, order_coll):
        created_time = datetime.datetime(2017, 1, 1)
        create_order(order_coll, created_time)
        assert order_coll.find_one()[settings.CREATED_TIME] == created_time
