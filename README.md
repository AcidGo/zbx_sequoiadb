## zbx_sequoiadb

通过 Zabbix 监控平台的自定义模块去定期采集和监控 SequoiaDB 数据库中各个角色的数据，结合监控平台的主机自动发现和监控项自动发现功能来自动化地添加监控单元至 Zabbix 平台。

Zabbix 配置的监控单元尽可能地使用相关项目模式来统一获取统一层面的监控数据，并减少与 SequoiaDB 数据库的交互频率。

### 功能

| 调用键                          | 功能                                             |
| ------------------------------- | ------------------------------------------------ |
| discovery_collectionspaces      | 自动发现集群内 CS 单元。                         |
| discovery_collectionspaces_host | 以适配主机名的方式自动发现集群内 CS 单元。       |
| discovery_inst                  | 自动发现配置内集群实例。                         |
| discovery_all_cs                | 自动发现集群内所有 CS 单元，供自动注册主机使用。 |
| collectionspaces                | 获取指定 CS 的监控信息。                         |
| listCollectionSpaces            | 列具连接集群中 CS 的清单。                       |
| sessions                        | 获取会话快照的监控信息。                         |

### 配置

主要配置文件为同级目录下的 config.py，特殊或底层配置需要在源码修改。

```python
# -*- coding: utf8 -*-
# 数据库集群实例清单，如果是要监控完整集群，需保证连接至 Coord 节点
db_config = {
    # 由于 Zabbix 主机命名中不能出现 : 符号，因此使用 _ 来分割端口。
    "10.10.32.186_11810": {
        # 可提供连接的角色指定和附加信息注入
        "flag": {
            "coord": True,
        },
        # 注释说明，自动注册主机时会参考此格式
        "info": "测试巨杉数据库",
        # 连接参数
        "connect": {
            "host": "10.10.32.186",
            "service": 11810,
            "user": "",
            "password": "",
        }
    }
}

```

### 使用

根据配置申明清单自动注册主机，并由发现的主机继续派生出需要监控的 CS 等对象。

目前在监控 3.4 版本的 SequoiaDB 中无异常情况，并符合预期运行，但快照中官方的 Python SDK 存在一个编码问题，已提交原厂待其后续解决。

