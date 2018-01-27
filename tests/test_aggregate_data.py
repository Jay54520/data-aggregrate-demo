# -*- coding: utf-8 -*-
import datetime
import time

import pytz
from dateutil.relativedelta import relativedelta

import settings
from tasks import create_order, aggregate
from tests.conftest import order_coll, aggregate_coll


class TestAggregateData:

    def setup_class(self):
        self.order_coll = next(order_coll())
        self.aggregate_coll = next(aggregate_coll())

    def _generate_data(self, date_type, end_time=None):
        end_time = end_time or datetime.datetime.now(pytz.timezone(settings.LOCAL_TZ))

        if date_type == settings.DATE_TYPE_MINUTELY:
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(minutes=2))
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(minutes=1))
        elif date_type == settings.DATE_TYPE_HOURLY:
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(minutes=2),
                         date_type=settings.DATE_TYPE_MINUTELY)
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(minutes=1),
                         date_type=settings.DATE_TYPE_MINUTELY)
        elif date_type == settings.DATE_TYPE_DAILY:
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(hours=2),
                         date_type=settings.DATE_TYPE_HOURLY)
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(hours=1),
                         date_type=settings.DATE_TYPE_HOURLY)
        elif date_type == settings.DATE_TYPE_WEEKLY:
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(days=2),
                         date_type=settings.DATE_TYPE_DAILY)
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(days=1),
                         date_type=settings.DATE_TYPE_DAILY)
        elif date_type == settings.DATE_TYPE_MONTHLY:
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(days=2),
                         date_type=settings.DATE_TYPE_DAILY)
            create_order(use_test_db=True, created_time=end_time - datetime.timedelta(days=1),
                         date_type=settings.DATE_TYPE_DAILY)
        elif date_type == settings.DATE_TYPE_YEARLY:
            create_order(use_test_db=True, created_time=end_time - relativedelta(months=2),
                         date_type=settings.DATE_TYPE_MONTHLY)
            create_order(use_test_db=True, created_time=end_time - relativedelta(months=1),
                         date_type=settings.DATE_TYPE_MONTHLY)
        else:
            raise ValueError('日期类型不正确')

    def teardown_method(self):
        self.order_coll.database.client.drop_database(settings.TEST_PAY_DB)

    def test_minutely(self):
        local_now = datetime.datetime(
            2016, 1, 1,
            tzinfo=pytz.timezone(settings.LOCAL_TZ)
        )
        self._generate_data(settings.DATE_TYPE_MINUTELY, local_now)
        async_result = aggregate(use_test_db=True, local_now=local_now)
        self.is_task_success(async_result)
        docs = list(self.aggregate_coll.find({
            settings.DATE_TYPE: settings.DATE_TYPE_MINUTELY
        }))
        assert len(docs) == 1
        doc = docs[0]
        assert doc[settings.DATE_TYPE] == settings.DATE_TYPE_MINUTELY
        assert doc[settings.SALES] == 1

    def test_hourly(self):
        local_now = datetime.datetime(
            2016, 1, 1,
            tzinfo=pytz.timezone(settings.LOCAL_TZ)
        )
        self._generate_data(settings.DATE_TYPE_HOURLY, local_now)
        async_result = aggregate(use_test_db=True, local_now=local_now)
        self.is_task_success(async_result)
        docs = list(self.aggregate_coll.find({
            settings.DATE_TYPE: settings.DATE_TYPE_HOURLY
        }))
        assert len(docs) == 1
        doc = docs[0]
        assert doc[settings.DATE_TYPE] == settings.DATE_TYPE_HOURLY
        assert doc[settings.SALES] == 2

    def test_daily(self):
        local_now = datetime.datetime(
            2016, 1, 1,
            tzinfo=pytz.timezone(settings.LOCAL_TZ)
        )
        self._generate_data(settings.DATE_TYPE_DAILY, local_now)
        async_result = aggregate(use_test_db=True, local_now=local_now)
        self.is_task_success(async_result)
        docs = list(self.aggregate_coll.find({
            settings.DATE_TYPE: settings.DATE_TYPE_DAILY
        }))
        assert len(docs) == 1
        doc = docs[0]
        assert doc[settings.DATE_TYPE] == settings.DATE_TYPE_DAILY
        assert doc[settings.SALES] == 2

    def test_weekly(self):
        local_now = datetime.datetime(
            2016, 1, 4,
            tzinfo=pytz.timezone(settings.LOCAL_TZ)
        )
        self._generate_data(settings.DATE_TYPE_WEEKLY, local_now)
        async_result = aggregate(use_test_db=True, local_now=local_now)
        self.is_task_success(async_result)

        docs = list(self.aggregate_coll.find({
            settings.DATE_TYPE: settings.DATE_TYPE_WEEKLY
        }))
        assert len(docs) == 1
        doc = docs[0]
        assert doc[settings.DATE_TYPE] == settings.DATE_TYPE_WEEKLY
        assert doc[settings.SALES] == 2

    def test_monthly(self):
        local_now = datetime.datetime(
            2016, 1, 1,
            tzinfo=pytz.timezone(settings.LOCAL_TZ)
        )
        self._generate_data(settings.DATE_TYPE_MONTHLY, local_now)
        async_result = aggregate(use_test_db=True, local_now=local_now)
        self.is_task_success(async_result)

        docs = list(self.aggregate_coll.find({
            settings.DATE_TYPE: settings.DATE_TYPE_MONTHLY
        }))
        assert len(docs) == 1
        doc = docs[0]
        assert doc[settings.DATE_TYPE] == settings.DATE_TYPE_MONTHLY
        assert doc[settings.SALES] == 2

    def test_yearly(self):
        local_now = datetime.datetime(
            2016, 1, 1,
            tzinfo=pytz.timezone(settings.LOCAL_TZ)
        )
        self._generate_data(settings.DATE_TYPE_YEARLY, local_now)
        async_result = aggregate(use_test_db=True, local_now=local_now)
        self.is_task_success(async_result)
        docs = list(self.aggregate_coll.find({
            settings.DATE_TYPE: settings.DATE_TYPE_YEARLY
        }))
        assert len(docs) == 1
        doc = docs[0]
        assert doc[settings.DATE_TYPE] == settings.DATE_TYPE_YEARLY
        assert doc[settings.SALES] == 2

    def is_task_success(self, async_result):
        counter = 0
        while not async_result.successful():
            if counter > 30:
                raise Exception('任务超时')

            time.sleep(0.1)
            counter += 1
        return True
