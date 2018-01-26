# -*- coding: utf-8 -*-
import datetime
from celery import Celery
from pymongo import MongoClient

import settings

app = Celery(
    'tasks',
    broker="redis://127.0.0.1:6379/0",
    backend="redis://127.0.0.1:6379/0",
)


@app.task
def create_order(order_coll=None, created_time=None):
    """生成数据价格为 1 的，创建时间为当前时间的订单"""

    # 为了方便写单元测试，所以允许设置 order_coll 和 created_time
    order_coll = order_coll or MongoClient(settings.MONGO_URI)[settings.PAY_DB][settings.ORDER_COLL]
    order_coll.insert(
        {
            'created_time': created_time or datetime.datetime.utcnow(),
            'price': 1,
        }
    )