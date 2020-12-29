## zbx_oceanbase

通过 Zabbix 监控平台的自定义模块去定期采集和监控 OceanBase 数据库、OCP 元数据库，结合监控平台的主机自动发现和监控项自动发现功能来自动化地添加监控单元至 Zabbix 平台。

Zabbix 配置的监控单元尽可能地使用相关模式来统一获取统一层面的监控数据，并减少与 OceanBase 数据库的交互频率。

### 功能

| 函数                                | 功能                                         |
| ----------------------------------- | -------------------------------------------- |
| discovery_server                    | 自动发现集群内 server 单元。                 |
| discovery_zone                      | 自动发现集群内 zone 单元。                   |
| discovery_cluster                   | 自动发现配置申明中定义的 cluster 单元。      |
| discovery_tenant                    | 自动发现集群内 tenant 单元。                 |
| discovery_format_zone               | 以适配主机名的方式自动发现集群内 zone 单元。 |
| discovery_tenant_resource_pool      | 自动发现租户内 resource_pool 单元。          |
| discovery_tenant_database           | 自动发现租户内 database 单元。               |
| discovery_backup_inst               | 自动发现备份恢复单元。                       |
| discovery_backup_inc                | 自动发现增量备份单元。                       |
| multi_server_info                   | 多行返回 server 基础数据信息。               |
| multi_zone_resource_state           | 多行返回 zone 资源使用状况信息。             |
| multi_zone_info                     | 多行返回 zone 基础数据信息。                 |
| multi_server_stat                   | 多行返回 server 资源使用状况信息。           |
| multi_cluster_info                  | 多行返回 cluster 基础数据信息。              |
| multi_tenant_info                   | 多行返回 tenant 基础数据信息。               |
| multi_tenant_resource               | 多行返回 tenant 资源情况信息。               |
| multi_tenant_memstore               | 多行返回 tenant memstore 信息。              |
| multi_bakrst_inc_status             | 多行返回备份恢复实例中增量备份的状态细节。   |
| tenant_minor_freeze_times           | 监控租户 minor freeze 的次数。               |
| multi_tenant_object_size            | 多行返回 tenant 内部对象的大小（2.0+支持）。 |
| cluster_clog_notsync_num            | 监控集群 clog 同步情况。                     |
| cluster_no_primary_table_num        | 监控集群无主副本的表数量。                   |
| cluster_index_no_available_num      | 监控失效索引的数量。                         |
| backup_check_error_basebackup       | 监控基线备份中的错误检查。                   |
| backup_check_last_basebackup_status | 监控最近一次基线备份的状态。                 |
| backup_check_error_restore          | 监控近期恢复操作的错误检查。                 |

### 配置

主要配置文件为同级目录下的 config.py，特殊或底层配置需要在源码修改。

```python
# 需要连接并搜集自动发现单元，并注册至 Zabbix
# 可以配置多个集群，在 Zabbix 端可以根据反馈信息自定义主机样式
cls_config = {
    "f5-dgbob": {
        # OceanBase 连接地址，可 ob-dns、F5，不能单独连接 observer
        "host": "127.0.0.1",
        # OceanBase 连接端口
        "port": 2883,
        # 连接集群名
        "cluster": "testcl",
        # 连接用户，请使用最低权限的只读用户，且需要连接 sys 租户
        "user": "testcl:sys:query",
        # 连接用户密码
        "password": "passwd",
    },
}
# OCP 备份恢复模块监控
backup_restore_config = {
    # 备份恢复平台，可多个实例
    "bakrst_inst_1": {
        # 备份恢复元数据库连接地址
        "host": "127.0.0.1",
        # 备份恢复元数据库连接端口
        "port": 2883,
        # 连接用户，请使用最低权限的只读用户
        "user": "obcluster:ocp_meta:query",
        # 连接用户密码
        "password": "passwd",
        # choose backup-restore platform meta database
        # 元数据库的版本配置库，请确认版本正确
        "database": "backup2230",
    }
}
```

### 使用

按照自动发现的 cluster、server、zone、tenant 作为注册主机，其余作为内部的自动发现监控，结合使用，从而仅编写一份配置申明即可通过 Zabbix 自动辐射对 OceanBase 各个维度与粒度的监控。

