# -*- coding:utf-8 -*-
# Author: AcidGo
# Usage: Zabbix Monitor OceanBase.(Only >= Py3.6)


import json
import os
import logging
import pymysql

from decimal import Decimal

def init_logging(level):
    """
    """
    script_path = os.path.split(os.path.realpath(__file__))[0]
    log_path = os.path.join(script_path, "zbx_ob.log")
    logging.basicConfig(
        level = getattr(logging, level.upper()),
        format = "%(asctime)s [%(levelname)s] %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
        filename = log_path,
        filemode = "a"
    )

def _json_dumps(dct):
    """对于 datetime.datetime 类型的 value ，进行适当规格转换后 dump 为 json。
    对于 value 中嵌套的字典格式，会进行最多一层的嵌套转换。

    Args:
        dct <dict>: 待转换为 json 的字典类型。
    Returns:
        res <str>: json 格式，如果有 datetime.datetime 类型则会转换时间格式。
    """
    from datetime import datetime
    res = {}
    for k0, v0 in dct.items():
        if isinstance(v0, datetime):
            v0 = v0.strftime("%Y%m%d-%H:%M:%S.%f")
        elif isinstance(v0, Decimal):
            v0 = [str(i) for i in [v0]][0]
        elif isinstance(v0, dict):
            t_1 = {i:j for i,j in v0.items()}
            for k1, v1 in v0.items():
                if isinstance(v1, datetime):
                    v1 = v1.strftime("%Y%m%d-%H:%M:%S.%f")
                t_1[k1] = v1
            v0 = t_1
        res[k0] = v0
    res = json.dumps(res)
    return res

def _microsecond2unixtime(dct):
    """由于 zabbix 监控项目的 unixtime 单位无法识别微秒，因此将微秒取消并指使用秒。
    对于 value 中嵌套的字典格式，会进行最多一层的嵌套转换。

    Args:
        dct <dict>: 待处理的字典。
    Returns:
        res <dict>: 处理完成后的字典。
    """
    res = {}
    for k0, v0 in dct.items():
        if isinstance(v0, int) and len(str(v0)) == 16 and k0.endswith("_time"):
            v0 = v0/(1000000.0)
        elif isinstance(v0, dict):
            t_1 = {i:j for i,j in v0.items()}
            for k1, v1 in v0.items():
                if isinstance(v1, int) and len(str(v1)) == 16:
                    v1 = v1/(1000000.0)
                t_1[k1] = v1
            v0 = t_1
        res[k0] = v0
    return res    

def _server_split(server):
    symbol_lst = [":", "-", "_"]

    for s in symbol_lst:
        if s in server:
            server_addr = server.split(s)[0].strip()
            server_port = server.split(s)[1].strip()
            return server_addr, server_port

    return server.split()

def _tenant_split(tenant):
    """
    """
    symbol_lst = [":", "-"]

    for s in symbol_lst:
        if s in tenant:
            cluster     = tenant.split(s)[0].strip()
            tenant_name = tenant.split(s)[1].strip()
            tenant_id   = tenant.split(s)[2].strip()
            return cluster, tenant_name, tenant_id
    return server.split()

def oceanbase_select(cluster=None, host=None, port=3306, user=None, password=None, database="oceanbase", sql=""):
    """
    """
    if "" in (host, port, user, password):
        raise Exception("Your input is invalid.")
    sql = sql.strip()
    if not sql.endswith(";"):
        sql += ";"
    db_config = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
    result = {}
    connect = pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **db_config)
    with connect.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    connect.commit()
    connect.close()
    return result

def discovery_server(db_config):
    """连接到 OceanBase 集群，检索发现该集群内的 observer 节点，以 zabbix 自动发现报文返回。

    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
    Returns:
        res <str>: zabbix 自动发现格式的 json，每项字段为：
            {
                "{#CLUSTER}": xxx,
                "{#ZONE}": xxx,
                "{#SVR_IP}": xxx,
                "{#SVR_PORT}": xxx,
            }
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ svr_ip, svr_port, zone from __all_server;
    """
    logging.debug("In discovery_server, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    res = {"data": []}

    for t in t_res:
        tmp = {
            "{#CLUSTER}":   db_config["cluster"],
            "{#ZONE}":      t["zone"],
            "{#SVR_IP}":    t["svr_ip"],
            "{#SVR_PORT}":  t["svr_port"],
        }
        res["data"].append(tmp)

    res = json.dumps(res)
    logging.debug("The res of discovery_server:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def discovery_zone(db_config):
    """连接到 OceanBase 集群，检索发现该集群内的 zone，以 zabbix 自动发现报文返回。
    其中 cluster 这个 zone 是为 zone 集群设置的名字。

    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
    Returns:
        res <str>: zabbix 自动发现格式的 json，每项字段为：
            {
                "{#ZONE}": xxx,
                "{#CLUSTER}": xxx,
            }
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ zone 
        from __all_zone where zone != '' group by zone union all select 'cluster';
    """
    logging.debug("In discovery_zone, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    res = {"data": []}
    res["data"] = [{"{#ZONE}": i["zone"], "{#CLUSTER}": db_config["cluster"]} for i in t_res]
    res = json.dumps(res)
    logging.debug("The res of discovery_zone:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def discovery_cluster(db_config):
    """连接到 OceanBase 集群，检索发现该集群内的 cluster，以 zabbix 自动发现报文返回。
    这里其实与 discovery_zone 非常相似，甚至都是一致的，
    因为目前 zabbix 无法固定自动发现的值，只能额外多写一个为集群服务的自动发现函数。

    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
    Returns:
        res <str>: zabbix 自动发现格式的 json，每项字段为:
            {
                "{#CLUSTER}": xxx,
            }
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ info
        from __all_zone where zone = '' and name = 'cluster';
    """
    logging.debug("In discovery_cluster, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    res = {"data": []}
    res["data"] = [{"{#CLUSTER}": i["info"]} for i in t_res]
    res = json.dumps(res)
    logging.debug("The res of discovery_cluster:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def discovery_tenant(db_config):
    """连接到 OceanBase 集群，检索发现该集群内的 租户 ，以 zabbix 自动发现报文返回。

    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
    Returns:
        res <str>: zabbix 自动发现格式的 json，每项字段为：
            {
                "{#CLUSTER}": xxx,
                "{#TENANT_NAME}": xxx,
                "{#TENANT_ID}": xxx,
            }
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ tenant_name, tenant_id from __all_tenant;
    """
    logging.debug("In discovery_tenant, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    res = {"data": []}
    res["data"] = [{"{#CLUSTER}": db_config["cluster"], "{#TENANT_NAME}": i["tenant_name"], "{#TENANT_ID}": i["tenant_id"]} for i in t_res]
    res = json.dumps(res)
    logging.debug("The res of discovery_tenant:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def discovery_format_zone(db_config):
    """返回全面格式化 zone 信息。

    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
    Returns:
        res <str>: zabbix 自动发现格式的 json，字段为：
            {
                "{#ZONE}": xxx,
                "{#MEMBER}": ip:port,ip:port ....
            }
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ svr_ip, svr_port, zone from __all_server;
    """
    logging.debug("In discovery_format_zone, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    res = {}
    for i in t_res:
        if i["zone"] in res:
            res[i["zone"]] = "{!s},{!s}:{!s}".format(res[i["zone"]], i["svr_ip"], i["svr_port"])
        else:
            res[i["zone"]] = "{!s}:{!s}".format(i["svr_ip"], i["svr_port"])
    res = json.dumps(res)
    logging.debug("The res of discovery_format_zone:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def discovery_tenant_resource_pool(db_config, tenant):
    """

    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
        tenant <str>: 受监控租户的标识。
    Returns:
        res <str>: zabbix 自动发现格式的 json，每项字段为：
            {
                "{#NAME}": xxx,
                "{#SVR_IP}": xxx,
                "{#SVR_PORT}": xxx,
            }
    """
    cluster, tenant_name, tenant_id = _tenant_split(tenant)
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */
        rp.name, rp.unit_count, rp.tenant_id, u.zone, u.svr_ip, u.svr_port 
        from __all_resource_pool rp left join __all_unit u on rp.resource_pool_id = u.resource_pool_id 
        where rp.tenant_id != -1 and rp.tenant_id = {tenant_id};
    """
    logging.debug("In discovery_tenant_resource_pool, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    res = {"data": []}
    res["data"] = [{"{#NAME}": i["name"], "{#SVR_IP}": i["svr_ip"], "{#SVR_PORT}": i["svr_port"]} for i in t_res]
    res = json.dumps(res)
    logging.debug("The res of discovery_tenant_resource_pool:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def discovery_tenant_database(db_config, tenant_id):
    """连接到 OceanBase 集群，检索发现指定租户内的数据库，以 zabbix 自动发现报文返回。

    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
        tenant_id <int>: 租户的 ID。
    Returns:
        res <str>: zabbix 自动发现格式的 json，每项字段为：
            {
                "{#CLUSTER}": xxx,
                "{#DATABASE_NAME}": xxx,
                "{#DATABASE_ID}": xxx,
            }
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ t.info as cluster, _ad.database_name, _ad.database_id from __all_database _ad
        join (select /*+ read_consistency(weak) query_timeout(30000000) */ info
        from __all_zone where name = 'cluster') t where tenant_id = {tenant_id};
    """
    logging.debug("In discovery_tenant_database, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    res = {"data": []}
    res["data"] = [{"{#CLUSTER}": i["cluster"], "{#DATABASE_NAME}": i["database_name"], "{#DATABASE_ID}": i["database_id"]} for i in t_res]
    res = json.dumps(res)
    logging.debug("The res of discovery_tenant_database:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def discovery_backup_inst(backup_restore_config):
    """自动发现备份恢复平台节点。

    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
    Returns:
        res <str>: Zabbix 自动发现格式的 json，每项字段为：
        {
            "{#INST}": xxx,
        }
    """
    sql = "select 1+1 as 'res' from dual;"
    res = {"data": []}
    logging.debug("In discovery_backup_inst, sql:[{!s}].".format(sql))
    for inst, db_config in backup_restore_config.items():
        t_res = oceanbase_select(**db_config, sql=sql)
        if len(t_res) < 1 or str(t_res[0]["res"]) != "2":
            logging.error("Get unexpected res from check")
            continue
        res["data"].append({"#INST": inst})
    res = json.dumps(res)
    logging.debug("The res of discovery_backup_inst:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def discovery_backup_inc(backup_restore_config):
    """
    """
    sql = "select cluster_name from inc_data_backup;"
    res = {"data": []}
    logging.debug("In discovery_backup_inc, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**backup_restore_config, sql=sql)
    res["data"] = [{"{#INC}": i["cluster_name"]} for i in t_res]
    res = json.dumps(res)
    logging.debug("The res of discovery_tenant_database:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_server_info(db_config, server):
    """一次多数据获取某个 server 的所有基础信息。
    当前对每个 server 获取的多数信息包括如下：
        zone: 该 server 所在的 zone。
        status: 该 server 的状态。
        with_rootserver: 该 server 是否为管控节点。
        stop_time: 该 server 的停止时间，单位为微妙 μs。
        start_service_time: 该 server 的服务上线时间，单位为微妙 μs。
        last_offline_time: 该 server 最近一次的服务下线时间，单位为微妙 μs。

    Args:
        server: server 的标志，使用格式为 svr_ip:svr_port。
        **db_config <dict>: 连接 OceanBase 集群的配置。
    Returns:
        res <str>: 符合预期的多数据 json 格式。
    """
    svr_ip, svr_port = _server_split(server)
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ * from __all_server where svr_ip='{svr_ip}' and svr_port={svr_port};
    """
    logging.debug("In multi_server_info, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    t_res = _microsecond2unixtime(t_res)
    res = _json_dumps(t_res)
    logging.debug("The res of multi_server_info:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_zone_resource_state(db_config, zone):
    """一次多数据获取某个 zone 的硬件资源状态。
    当前对每个 zone 获取的多数信息包括如下：
        cpu_total: 该 zone 总共的 CPU 个数。
        cpu_assigned: 该 zone 已经分配的 CPU 个数。
        cpu_assigned_percent: 该 zone 已分配 CPU 占比。
        mem_total: 该 zone 总共的内存容量，单位为字节 B。
        mem_assigned: 该 zone 已经分配的内存量，单位为字节 B。
        mem_assigned_percent: 该 zone 已经分配的内存占比。
        disk_total: 该 zone 总共的磁盘容量，单位为字节 B。
        disk_in_use: 该 zone 已使用的磁盘容量，单位为字节 B。
        disk_in_use_percent: 该 zone 已使用的磁盘容量占比。
        unit_num: 该 zone 的 unit 数量。
        leader_count: 该 zone 的主副本数量。
    Args:
        db_config <dict>: 连接 OceanBase 集群的配置。
        zone <string>: 指定的 zone。
    Returns:
        res <str>: 符合预期的多数据 json 格式。
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ 
            sum(cpu_total) as cpu_total,
            sum(cpu_assigned) as cpu_assigned,
            sum(cpu_assigned)/sum(cpu_total)*100 as cpu_assigned_percent,
            sum(mem_total) as mem_total,
            sum(mem_assigned) as mem_assigned,
            sum(mem_assigned)/sum(mem_total)*100 as mem_assigned_percent,
            sum(disk_total) as disk_total,
            sum(disk_in_use) as disk_in_use,
            sum(disk_in_use)/sum(disk_total)*100 as disk_in_use_percent,
            sum(unit_num) as unit_num,
            sum(leader_count) as leader_count
        from __all_virtual_server_stat where zone = '{zone}';
    """
    logging.debug("In multi_zone_resource_state, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = _json_dumps(t_res)
    logging.debug("The res of multi_zone_resource_state:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_zone_info(db_config, zone):
    """一次多数据获取某个 zone 的所有基础信息。
    当前对每个 zone 获取的多数信息包括如下：
        - cluster(cluster of zones):
            cluster: zone 集群的名字。
                0: xxx(cluster name)
            config_version: 集群配置的版本。
                int: Null
            frozen_time: 集群基线数据的冻结时间，单位为微秒 μs。
                int: Null
            frozen_version: 集群基线数据冻结的版本号。
                int: Null
            global_broadcast_version: 集群广播的基线版本号。
                int: Null
            is_merge_error: 集群是否具有合并的错误，0 为没有错误。
                0: Null(no error, it is ok.)
            merge_status: 集群合并的状态。
                0: IDLE(空闲)
        - *(zone):
            all_merged_version: 该 zone 所有已合并的版本。
                int: Null
            broadcast_version: 该 zone 对自身基线广播的版本。
                int: Null
            idc: 该 zong 所处的 IDC (数据中心)。
                0: Null
            is_merge_timeout: 该 zone 合并是否超时。
                0: Null(no timeout)
            is_merging: 该 zone 是否正在合并。
                0: Null(no merging now)
            last_merged_time: 该 zone 最近一次合并完成的时间，单位为微秒 μs。
                int: Null
            last_merged_version: 该 zone 最近一次合并完成产生的版本。
                int: Null
            merge_start_time: 该 zone 最近一次开始合并的时间，单位为微秒 μs。
                int: Null
            merge_status: 该 zone 合并的状态。
                0: IDEL(空闲)
            region: 该 zone 所处的 region (区)。
                0: xxx(设置的区)
            status: 该 zone 的状态。
                2: ACTIVE
            suspend_merging: 该 zone 暂停了合并过程。
                0: Null(no suspend)
            zone_type: 该 zone 的类型。
                0: ReadWrite

    Args:
        server: server 的标志，使用格式为 svr_ip:svr_port。
        **db_config <dict>: 连接 OceanBase 集群的配置。
    Returns:
        res <str>: 符合预期的多数据 json 格式。
    """
    if zone == "cluster":
        sql = f"""
            select /*+ read_consistency(weak) query_timeout(30000000) */ * from __all_zone where zone = '';
        """
    else:
        sql = f"""
            select /*+ read_consistency(weak) query_timeout(30000000) */ * from __all_zone where zone = '{zone}';
        """
    logging.debug("In multi_zone_info, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    t_res = {i["name"]: i for i in t_res}
    t_res = _microsecond2unixtime(t_res)
    res = _json_dumps(t_res)
    logging.debug("The res of multi_zone_info:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_server_stat(db_config, server):
    """
    """
    svr_ip, svr_port = _server_split(server)
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ 
            cpu_total, cpu_assigned, cpu_assigned_percent, 
            mem_total, mem_assigned, mem_assigned_percent,
            disk_total, disk_assigned, disk_assigned_percent,
            unit_num, clock_deviation,
            round(cpu_weight, 8) as cpu_weight, 
            round(memory_weight, 8) as memory_weight, 
            round(disk_weight, 8) as disk_weight
        from __all_virtual_server_stat
        where svr_ip = '{svr_ip}' and svr_port = {svr_port};
    """
    logging.debug(f"In multi_server_stat, sql:[{sql}].")
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = _json_dumps(t_res)
    logging.debug("The res of multi_server_stat:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_cluster_info(db_config, cluster):
    """一次多数据获取某个集群的所有基础信息。
    主要还是对 zone 的自动发现在 zabbix 上不够灵活的一种补充。
    当前对每个集群获取的多数信息包括如下：
        cluster: zone 集群的名字。
            0: xxx(cluster name)
        config_version: 集群配置的版本。
            int: Null
        frozen_time: 集群基线数据的冻结时间，单位为微秒 μs。
            int: Null
        frozen_version: 集群基线数据冻结的版本号。
            int: Null
        global_broadcast_version: 集群广播的基线版本号。
            int: Null
        is_merge_error: 集群是否具有合并的错误，0 为没有错误。
            0: Null(no error, it is ok.)
        merge_status: 集群合并的状态。
            0: IDLE(空闲)
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ * from __all_zone 
            where zone = (select /*+ read_consistency(weak) query_timeout(30000000) */ zone
                from __all_zone where name = 'cluster' and info = '{cluster}');
        """
    logging.debug("In multi_cluster_info, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)
    t_res = {i["name"]: i for i in t_res}
    t_res = _microsecond2unixtime(t_res)
    res = _json_dumps(t_res)
    logging.debug("The res of multi_cluster_info:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_tenant_info(db_config, tenant):
    """一次多数据获取某个 tenant 的基础信息。
    当前对每个 tenant 获取的多数信息包括如下：
        zone_list: 使用的 zone。
        primary_zone: 主 zone 顺序。
        locked: 是否被锁。
        read_only: 是否只读。
        locality: 副本分布。
        logonly_replica_num: 只读副本数量。

    Args:
        db_config: 连接 OceanBase 集群的配置。
        tenant_id: 租户的 ID。
    Returns:
        res <str>: 符合预期的多数据 json 格式。
    """
    cluster, tenant_name, tenant_id = _tenant_split(tenant)
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ * from __all_tenant
        where tenant_id = {tenant_id};
    """
    logging.debug("In multi_tenant_info, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = _json_dumps(t_res)
    logging.debug("The res of multi_tenant_info:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_tenant_resource(db_config, tenant):
    """一次多数据获取某个 tenant 的分配资源信息。
    当前对每个 tenant 获取的多数信息包括如下：
        resource_pool_name: 分配的 resource pool 的名字。
        resource_pool_conf: 分配的 resource pool 对应的配置。
        unit_count: 分配的 unit 数量。
        unit_total_cpu: 所有分配的 unit 的 CPU 数量。
        unit_total_memory: 所有分配的 unit 的内存量，单位为 B。
    Returns:
        res <str>: 符合预期的多数据 json 格式。
    """
    cluster, tenant_name, tenant_id = _tenant_split(tenant)
    sql = f"""
        SELECT /*+ read_consistency(weak) query_timeout(30000000) */ 
        c.tenant_id, 
        e.tenant_name, 
        c.name as resource_pool_name, 
        d.name as resource_pool_conf, 
        c.unit_count as unit_count,
        sum(d.min_cpu) as unit_total_cpu,
        round(sum(d.min_memory)/1024/1024/1024, 4) as unit_total_memory_gb
        FROM __all_resource_pool c, __all_unit_config d, __all_tenant e 
        WHERE c.tenant_id = {tenant_id} AND c.unit_config_id=d.unit_config_id AND c.tenant_id=e.tenant_id;
    """
    logging.debug("In multi_tenant_resource, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = _json_dumps(t_res)
    logging.debug("The res of multi_tenant_resource:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_tenant_memstore(db_config, tenant, server):
    """
    """
    cluster, tenant_name, tenant_id = _tenant_split(tenant)
    svr_ip, svr_port = _server_split(server)
    # 这里使用了 hint，会导致经常返回空值，已向蚂蚁工程师提交工单，暂且不用 hint
    sql = f"""
        select
        round(m.ACTIVE/1024/1024/1024,4) active_gb, 
        round(m.TOTAL/1024/1024/1024,4) total_gb, 
        round(m.FREEZE_TRIGGER/1024/1024/1024,4) freeze_trigger_gb, 
        round(m.ACTIVE/m.FREEZE_TRIGGER,4)*100 percent_trigger, 
        round(m.MEM_LIMIT/1024/1024/1024,4) mem_limit_gb 
        from oceanbase.gv$memstore m
        where m.tenant_id = {tenant_id} and m.ip = "{svr_ip}" and m.port = {svr_port};
    """
    logging.debug("In multi_tenant_memstore, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = _json_dumps(t_res)
    logging.debug("The res of multi_tenant_memstore:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_bakrst_inc_status(backup_restore_config, cluster):
    """一次多数据获取某个集群的增量备份状态。
    当前对每个 tenant 获取的多数信息包括如下：
        tenant_white_list: 租户白名单信息。
        cpu_core_number: 任务占用 CPU 核数。
        lease_expire_time: lease 过期时间，UNIXTIME 时间戳。
        diff_lease_expire_time: 租期的差异时间，如果小于 0 表示租期过期。
        checkpoint: 当前备份进度，UNIXTIME 时间戳。
        diff_checkpoint: checkpoint 的时延，单位为秒。
        status: 增量备份状态。
        error_msg: 增量备份报错信息。
    Returns:
        res <str>: 符合预期的多数据 json 格式。
    """
    sql = f"""
        select 
        tenant_white_list,
        cpu_core_number,
        cast(unix_timestamp(lease_expire_time) as unsigned) as lease_expire_time,
        timestampdiff(second, now(), lease_expire_time) as diff_lease_expire_time,
        cast(unix_timestamp(checkpoint) as unsigned) as checkpoint,
        timestampdiff(second, checkpoint, now()) as diff_checkpoint,
        status,
        error_msg
        from inc_data_backup where cluster_name = "{cluster}";
    """
    logging.debug("In multi_bakrst_inc_status, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**backup_restore_config, sql=sql)[0]
    res = _json_dumps(t_res)
    logging.debug("The res of multi_bakrst_inc_status:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def tenant_minor_freeze_times(db_config, tenant, val_min=30):
    """某租户在最近 val_min 分钟内的冻结次数。
    """
    import time
    from datetime import datetime
    cluster, tenant_name, tenant_id = _tenant_split(tenant)
    res_datetime = datetime.fromtimestamp(time.time()-int(val_min)*60).strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        SELECT /*+ read_consistency(weak) query_timeout(30000000) */  count(*) as num_minor_freeze
        FROM __all_server_event_history 
        WHERE name1 = '{tenant_id}' and event like 'do minor freeze%' and gmt_create > "{res_datetime}";
    """
    logging.debug("In tenant_minor_freeze_times, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = t_res["num_minor_freeze"]
    logging.debug("The res of tenant_minor_freeze_times:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def multi_tenant_object_size(db_config, tenant):
    """某租户占有的各个对象的数量和大小（GB）。
    """
    cluster, tenant_name, tenant_id = _tenant_split(tenant)
    obj_index_sql = f"""
        select /*+READ_CONSISTENCY(WEAK), QUERY_TIMEOUT(30000000)*/
            count(*) as cnt,
            coalesce(round(sum(a.required_size)/1024/1024/1024, 2), 0) as obj_size_gb
        from __all_virtual_meta_table a, __all_virtual_table b, __all_zone c
        where
            a.role = 1 
            and instr(a.member_list, a.svr_ip) > 0 
            and a.tenant_id = b.tenant_id 
            and a.table_id = b.table_id 
            and b.table_name like '\_\_idx\_%'
            and c.zone != ''
            and b.tenant_id = {tenant_id};
    """
    obj_recycle_sql = f"""
        select /*+READ_CONSISTENCY(WEAK), QUERY_TIMEOUT(30000000)*/
            count(*) as cnt,
                coalesce(round(sum(a.required_size)/1024/1024/1024, 2), 0) as obj_size_gb
        from __all_virtual_meta_table a, __all_virtual_table b
        where
            a.role = 1 
            and instr(a.member_list, a.svr_ip) > 0 
            and a.tenant_id = b.tenant_id 
            and a.table_id = b.table_id 
            and b.table_name like '\_\_recycle\_%'
            and b.tenant_id = {tenant_id};
    """
    obj_data_sql = f"""
        select /*+READ_CONSISTENCY(WEAK), QUERY_TIMEOUT(30000000)*/
            count(*) as cnt,
            coalesce(round(sum(a.required_size)/1024/1024/1024, 2), 0) as obj_size_gb
        from __all_virtual_meta_table a, __all_virtual_table b
        where
            a.role = 1 
            and instr(a.member_list, a.svr_ip) > 0 
            and a.tenant_id = b.tenant_id 
            and a.table_id = b.table_id 
            and b.table_name not like '\_\_recycle\_%'
            and b.table_name not like '\_\_index\_%'
            and b.tenant_id = {tenant_id};
    """
    res = {}
    logging.debug("In tenant_object_size, obj_index_sql:[{!s}]".format(obj_index_sql))
    t_res = oceanbase_select(**db_config, sql=obj_index_sql)[0]
    res["index"] = {
        "count": t_res["cnt"],
        "size": float(t_res["obj_size_gb"]),
    }
    logging.debug("In tenant_object_size, obj_recycle_sql:[{!s}]".format(obj_recycle_sql))
    t_res = oceanbase_select(**db_config, sql=obj_recycle_sql)[0]
    res["recycle"] = {
        "count": t_res["cnt"],
        "size": float(t_res["obj_size_gb"]),
    }
    logging.debug("In tenant_object_size, obj_data_sql:[{!s}]".format(obj_data_sql))
    t_res = oceanbase_select(**db_config, sql=obj_data_sql)[0]
    res["data"] = {
        "count": t_res["cnt"],
        "size": float(t_res["obj_size_gb"]),
    }
    res = json.dumps(res)
    logging.debug("The res of tenant_object_size:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def cluster_clog_notsync_num(db_config):
    """当前集群中未同步的副本数量。
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */  count(*) as num_clog_not_sync from __all_virtual_clog_stat where is_in_sync= 0 and is_offline = 0 and replica_type != 16;
    """
    logging.debug("In cluster_clog_notsync_num, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = t_res["num_clog_not_sync"]
    logging.debug("The res of cluster_clog_notsync_num:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def cluster_no_primary_table_num(db_config):
    """当前集群无主表的数量。
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */ count(*) as num_no_primary_table
        from 
          (select table_id, partition_idx from __all_virtual_election_info group by table_id, partition_idx except select table_id, partition_idx from __all_virtual_election_info where role = 1 ) 
          a,__all_table b,
          __all_tenant c 
        where a.table_id=b.table_id and b.tenant_id=c.tenant_id;
    """
    logging.debug("In cluster_no_primary_table_num, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = t_res["num_no_primary_table"]
    logging.debug("The res of cluster_no_primary_table_num:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def cluster_index_no_available_num(db_config):
    """状态不是 available 的索引数量。
    """
    sql = f"""
        select /*+ read_consistency(weak) query_timeout(30000000) */  count(*) as num_index_no_available from __tenant_virtual_table_index where comment <> 'available';
    """
    logging.debug("In cluster_index_no_available_num, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**db_config, sql=sql)[0]
    res = json.dumps(t_res)
    logging.debug("The res of cluster_index_no_available_num:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def backup_check_error_basebackup(backup_restore_config, interval=1):
    """备份恢复平台中最近 interval 小时异常的基线备份任务数量。
    """
    sql = "select count(*) as res from base_data_backup_task_history where status not in ('done') and create_time >= date_sub(now(), interval %d hour);" % (interval)
    res = None
    logging.debug("In backup_check_error_basebackup, sql:[{!s}].".format(sql))
    t_res = oceanbase_select(**backup_restore_config, sql=sql)[0]
    res = t_res["res"]
    logging.debug("The res of backup_check_error_basebackup:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def backup_check_last_basebackup_status(backup_restore_config):
    """最近一个版本备份的的版本及备份任务的状态统计。
    """
    sql_1 = "select max(data_version) as data_version from base_data_backup_task_history where data_version is not null and data_version > 0;"
    logging.debug("In backup_check_last_basebackup_status, sql:[{!s}].".format(sql_1))
    data_version = oceanbase_select(**backup_restore_config, sql=sql_1)[0]["data_version"]
    sql_2 = "select lower(status) as s, count(*) as cnt from base_data_backup_task_history where data_version = {!s} group by status;".format(str(data_version))
    logging.debug("In backup_check_last_basebackup_status, sql:[{!s}].".format(sql_2))
    t_res = oceanbase_select(**backup_restore_config, sql=sql_2)
    t_res = {i["s"]: i["cnt"] for i in t_res}
    if "pending" not in t_res:
        t_res["pending"] = 0
    if "doing" not in t_res:
        t_res["doing"] = 0
    if "done" not in t_res:
        t_res["done"] = 0
    if "failed" not in t_res:
        t_res["failed"] = 0
    t_res["data_version"] = data_version
    res = json.dumps(t_res)
    logging.debug("The res of backup_check_last_basebackup_status:")
    logging.debug(res)
    logging.debug("="*30)
    return res

def backup_check_error_restore(backup_restore_config, interval=1):
    """备份恢复平台最近 interval 小时异常的恢复任务数量。
    """
    sql = "select count(*) as res from oceanbase_restore_history where status not in ('FINISH') and job_create_time >= date_sub(now(), interval %d hour);" % (interval)
    res = None
    logging.debug("In backup_check_error_restore, sql:[{!s}].".format(sql))
    res = oceanbase_select(**backup_restore_config, sql=sql)[0]["res"]
    logging.debug("The res of backup_check_error_restore:")
    logging.debug(res)
    logging.debug("="*30)
    return res

if __name__ == "__main__":
    init_logging("debug")
    import sys
    if len(sys.argv) < 2:
        raise Exception("Input is error.")

    # ---------- CONFG ----------
    import zbx_ob_config
    cls_config = zbx_ob_config.cls_config
    backup_restore_config = {}
    if "backup_restore_config" in dir(zbx_ob_config):
        backup_restore_config = zbx_ob_config.backup_restore_config
    # ========== CONFG ==========

    pre_funcs = {
        "ob_cluster": [
            "discovery_server",
            "discovery_zone",
            "discovery_cluster",
            "discovery_tenant",
            "discovery_format_zone",
            "discovery_tenant_resource_pool",
            "discovery_tenant_database",
            "multi_server_info",
            "multi_zone_resource_state",
            "multi_zone_info",
            "multi_server_stat",
            "multi_cluster_info",
            "multi_tenant_info",
            "multi_tenant_resource",
            "multi_tenant_memstore",
            "multi_tenant_object_size",
            "tenant_minor_freeze_times",
            "cluster_clog_notsync_num",
            "cluster_no_primary_table_num",
            "cluster_index_no_available_num",
        ],
        "backup_restore": [
            "backup_check_error_basebackup",
            "backup_check_error_restore",
            "backup_check_last_basebackup_status",
            "multi_bakrst_inc_status",
            "discovery_backup_inc",
        ]
    }

    try:
        logging.debug("Get the cmd line input: {!s}".format(sys.argv[1:]))
        func_name = sys.argv[2]
        func = locals().get(sys.argv[2])

        if func_name in pre_funcs.get("backup_restore", []):
            bakrst_name = sys.argv[1].strip()
            if bakrst_name not in backup_restore_config:
                raise Exception("Not The bakrst_name:[{!s}] in backup_restore_config.".format(bakrst_name))
            bakrst_config = backup_restore_config[bakrst_name]

            if len(sys.argv) == 3:
                print(func(bakrst_config))
            elif len(sys.argv) > 3:
                print(func(bakrst_config, *sys.argv[3:]))
            else:
                raise Exception("The length of argv is invalid")
        elif func_name in pre_funcs.get("ob_cluster"):
            cls_name = sys.argv[1].strip()
            if cls_name not in cls_config:
                raise Exception("Not The cls_name:[{!s}] in cls_config.".format(cls_name))
            db_config = cls_config[cls_name]

            if len(sys.argv) == 3:
                print(func(db_config))
            elif len(sys.argv) > 3:
                print(func(db_config, *sys.argv[3:]))
            else:
                raise Exception("The length of argv is invalid")
        else:
            raise Exception(f"Not support the func: {func_name}")
    except Exception as e:
        logging.exception(e)
        raise e
