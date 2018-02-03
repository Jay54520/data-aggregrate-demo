# 数据聚合示例

[![Build Status](https://travis-ci.org/Jay54520/data-aggregrate-demo.svg?branch=master)](https://travis-ci.org/Jay54520/data-aggregrate-demo)
[![codecov](https://codecov.io/gh/Jay54520/data-aggregrate-demo/branch/master/graph/badge.svg)](https://codecov.io/gh/Jay54520/data-aggregrate-demo)

聚合级别由分钟到年的聚合示例：小时的数据由分钟聚合，天的数据由小时聚合，
周、月的数据由天聚合，年的数据由月聚合。适用于数据量大，实时计算太过耗时的情况。

这里的例子是由订单价格聚合出销售额。

## 开始

### 依赖

* Redis
* MongoDB
* Python3

### 安装

安装 Python 依赖: `pip install -r requirements.txt`

## 运行测试

在项目目录下，运行 `pytest`。

## 许可证

本项目使用 Apache License 2.0 许可证，详情见 [LICENSE](LICENSE)