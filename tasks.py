# -*- coding: utf-8 -*-
import datetime

import pytz
from celery import Celery
from dateutil.relativedelta import relativedelta

import settings
from utils import get_collections

app = Celery(
    'tasks',
    broker="redis://127.0.0.1:6379/1",
    backend="redis://127.0.0.1:6379/0",
)
app.conf.task_serializer = 'pickle'
app.conf.result_serializer = 'pickle'
app.conf.accept_content = ['pickle']
app.conf.task_routes = {
    'tasks._calculate_sales': {'queue': 'aggregate'},
    'tasks.aggregate': {'queue': 'aggregate'},
}


@app.task
def create_order(use_test_db=False, created_time=None, date_type=None):
    """生成数据价格为 1 的，创建时间为当前时间的订单"""

    collections = get_collections(use_test_db)
    order_coll = collections[settings.ORDER_COLL]
    aggregate_coll = collections[settings.AGGREGATE_COLL]

    if date_type:
        aggregate_coll.insert({
            settings.TIME_START: created_time or datetime.datetime.utcnow(),
            settings.PRICE: 1,
            settings.DATE_TYPE: date_type
        })
    else:
        order_coll.insert({
            settings.CREATED_TIME: created_time or datetime.datetime.utcnow(),
            settings.PRICE: 1,
        })


@app.task
def _calculate_sales(match_condition, aggregate_date_type, use_test_db=False):
    """根据 match_condition，计算出销售额"""

    collections = get_collections(use_test_db)
    order_coll = collections[settings.ORDER_COLL]
    aggregate_coll = collections[settings.AGGREGATE_COLL]

    if aggregate_date_type == settings.DATE_TYPE_MINUTELY:
        result = list(order_coll.aggregate([
            {'$match': match_condition},
            {'$group': {'_id': None, settings.SALES: {'$sum': '${}'.format(settings.PRICE)}}}
        ]))
        time_key = settings.CREATED_TIME
    else:
        result = list(aggregate_coll.aggregate([
            {'$match': match_condition},
            {'$group': {'_id': None, settings.SALES: {'$sum': '${}'.format(settings.PRICE)}}}
        ]))
        time_key = settings.TIME_START

    # 如果没有匹配的记录，那么销售额是 0
    sales = 0 if not result else result[0][settings.SALES]
    aggregate_coll.update_one(
        {
            settings.DATE_TYPE: aggregate_date_type,
            time_key: match_condition[time_key]['$gte'],
        },
        {'$set': {settings.SALES: sales}},
        upsert=True
    )


@app.task
def aggregate(use_test_db=False, local_now=None):
    local_now = local_now or datetime.datetime.now(pytz.timezone(settings.LOCAL_TZ))

    # 聚合上一分钟数据
    current_minute = local_now.replace(microsecond=0)
    last_minute = current_minute - datetime.timedelta(minutes=1)
    signature = _calculate_sales.si(
        match_condition={
            settings.CREATED_TIME: {'$gte': last_minute, '$lt': current_minute},
        },
        aggregate_date_type=settings.DATE_TYPE_MINUTELY,
        use_test_db=use_test_db
    )

    # 聚合上一小时的数据
    if local_now.minute == 0:
        current_hour = local_now.replace(second=0, microsecond=0)
        last_hour = current_minute - datetime.timedelta(hours=1)
        signature |= _calculate_sales.si(
            match_condition={
                settings.TIME_START: {'$gte': last_hour, '$lt': current_hour},
                settings.DATE_TYPE: settings.DATE_TYPE_MINUTELY
            },
            aggregate_date_type=settings.DATE_TYPE_HOURLY,
            use_test_db=use_test_db
        )

    # 聚合昨天的数据
    if local_now.hour == 0 and local_now.minute == 0:
        current_day = local_now.replace(second=0, microsecond=0)
        last_day = current_minute - datetime.timedelta(days=1)
        signature |= _calculate_sales.si(
            match_condition={
                settings.TIME_START: {'$gte': last_day, '$lt': current_day},
                settings.DATE_TYPE: settings.DATE_TYPE_HOURLY
            },
            aggregate_date_type=settings.DATE_TYPE_DAILY,
            use_test_db=use_test_db
        )

    # 聚合上一周的数据
    if local_now.isoweekday() == 1 and local_now.hour == 0 and local_now.minute == 0:
        current_monday = local_now.replace(second=0, microsecond=0)
        last_monday = current_minute - datetime.timedelta(weeks=1)
        signature |= _calculate_sales.si(
            match_condition={
                settings.TIME_START: {'$gte': last_monday, '$lt': current_monday},
                settings.DATE_TYPE: settings.DATE_TYPE_DAILY
            },
            aggregate_date_type=settings.DATE_TYPE_WEEKLY,
            use_test_db=use_test_db
        )

    # 聚合上一月的数据
    if local_now.day == 1 and local_now.hour == 0 and local_now.minute == 0:
        current_month = local_now.replace(second=0, microsecond=0)
        last_month = current_month - relativedelta(months=1)
        signature |= _calculate_sales.si(
            match_condition={
                settings.TIME_START: {'$gte': last_month, '$lt': current_month},
                settings.DATE_TYPE: settings.DATE_TYPE_DAILY
            },
            aggregate_date_type=settings.DATE_TYPE_MONTHLY,
            use_test_db=use_test_db
        )

    # 聚合上一年的数据
    if local_now.month == 1 and local_now.day == 1 and local_now.hour == 0 and \
            local_now.minute == 0:
        current_year = local_now.replace(second=0, microsecond=0)
        last_year = current_year - relativedelta(years=1)
        signature |= _calculate_sales.si(
            match_condition={
                settings.TIME_START: {'$gte': last_year, '$lt': current_year},
                settings.DATE_TYPE: settings.DATE_TYPE_MONTHLY
            },
            aggregate_date_type=settings.DATE_TYPE_YEARLY,
            use_test_db=use_test_db
        )
    return signature()
