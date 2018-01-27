# -*- coding: utf-8 -*-

from pymongo import MongoClient

import settings


def get_collections(use_test_db=False):
    """
    获取数据表
    :param use_test_db: 是否使用测试数据库
    :return: {'collection_name': collection}
    """
    if use_test_db:
        order_coll = MongoClient(settings.MONGO_URI)[settings.TEST_PAY_DB][settings.ORDER_COLL]
        aggregate_coll = MongoClient(settings.MONGO_URI)[settings.TEST_PAY_DB][settings.AGGREGATE_COLL]
    else:
        order_coll = MongoClient(settings.MONGO_URI)[settings.PAY_DB][settings.ORDER_COLL]
        aggregate_coll = MongoClient(settings.MONGO_URI)[settings.PAY_DB][settings.AGGREGATE_COLL]
    return {
        settings.ORDER_COLL: order_coll,
        settings.AGGREGATE_COLL: aggregate_coll
    }