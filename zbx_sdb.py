# -*- coding: utf8 -*-
"""
Author: AcidGo
Usage: 监控巨杉数据库 SequoiaDB。
"""

import logging, json, os, sys, time
import pysequoiadb
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pysequoiadb import client
from pysequoiadb.error import SDBBaseError, SDBEndOfCursor

def init_logger(level, logfile=None):
    """日志功能初始化。
    如果使用日志文件记录，那么则默认使用 RotatinFileHandler 的大小轮询方式，
    默认每个最大 10 MB，最多保留 5 个。
    Args:
        level: 设定的最低日志级别。
        logfile: 设置日志文件路径，如果不设置则表示将日志输出于标准输出。
    """
    import os
    import sys
    if not logfile:
        logging.basicConfig(
            level = getattr(logging, level.upper()),
            format = "%(asctime)s [%(levelname)s] %(message)s",
            datefmt = "%Y-%m-%d %H:%M:%S"
        )
    else:
        logger = logging.getLogger()
        logger.setLevel(getattr(logging, level.upper()))
        if logfile.lower() == "local":
            logfile = os.path.join(sys.path[0], os.path.basename(os.path.splitext(__file__)[0]) + ".log")
        handler = RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logging.info("Logger init finished.")

class SDBInst(object):
    """
    """
    SupportSnapType = {
        # 数据库快照：列出数据库的状态和监控信息。
        "SDB_SNAP_COLLECTIONSPACES": 5,
        "SDB_SNAP_DATABASE": 6,
        "SDB_SNAP_SYSTEM": 7,
    }
    SupportFuncs = {
        "snapshot",
        "sessions",
        "cs_report_local",
        "cs_report_remote",
        "cs_report",
        # "discovery_collectionspaces",
        "collectionspaces",
        # "listCollectionSpaces",
        "discovery_collectionspaces_host",
    }
    SnapType = {
        "SDB_SNAP_COLLECTIONSPACES": 5,
        "SDB_SNAP_DATABASE": 6,
        "SDB_SNAP_SESSIONS": 2,
        "SDB_SNAP_SYSTEM": 7
    }

    def __init__(self, host="localhost", service=11810, user="", password=""):
        self.host = host
        self.service = service
        self.user = user
        self.password = password

        self.db = None
        self.info = "UNKNOW"
        self.conf = {}

    def __del__(self):
        self.close()

    def add_conf(self, dct):
        self.conf = dct

    def connect(self):
        self.db = client(
            host = self.host,
            service = self.service,
            user = self.user,
            psw = self.password
        )

    def set_info(self, info):
        self.info = info

    def close(self):
        if self.db:
            self.db.disconnect()
        self.db = None

    def call(self, func, *args):
        func = func.replace(".", "_") if "." in func else func
        if func not in self.SupportFuncs:
            msg = "the func called {!s} is not in support funcs".format(func)
            raise ValueError(msg)
        f = getattr(self, func)
        return f(*args)

    def _cs_report_local_file(self):
        return os.path.join(sys.path[0], os.path.basename(os.path.splitext(__file__)[0]) + ".cs_report_local.{!s}_{!s}".format(self.host, str(self.service)) + ".json")

    def _walk_snap_cursor_dict(self, cr):
        res_dict = {}
        while 1:
            try:
                record = cr.next()
            except SDBEndOfCursor:
                break
            except SDBBaseError as e:
                pysequoiadb._print(e)
            else:
                res_dict.update(record)
        return res_dict

    def _walk_snap_cursor_list(self, cr):
        res_list = []
        while 1:
            try:
                record = cr.next()
            except SDBEndOfCursor:
                break
            except SDBBaseError as e:
                pysequoiadb._print(e)
            else:
                res_list.append(record)
        return res_list

    def snapshot(self, snap_type_str):
        if snap_type_str not in self.SupportSnapType:
            msg = "not support the snap_type_str: {!s}".format(snap_type_str)
        snap_type_int = self.SupportSnapType[snap_type_str]
        cr = self.db.get_snapshot(snap_type_int)
        res_dict = self._walk_snap_cursor_dict(cr)
        cr.close()
        return res_dict

    def sessions(self):
        """由于 SDB_SNAP_SESSIONS 输出会话的详细信息，单独交给zabbix肯定很大，于是这里做了汇总处理。

        Returns:
            {
                "split_type": {
                    "Agent": {
                        "Creating": <int>,
                        "Running": <int>,
                        "Waiting": <int>,
                        "Idle": <int>,
                        "Destroying": <int>,
                    },
                    "ShardAgent": {
                        "Creating": <int>,
                        "Running": <int>,
                        "Waiting": <int>,
                        "Idle": <int>,
                        "Destroying": <int>,
                    },
                    "ReplAgent": {
                        "Creating": <int>,
                        "Running": <int>,
                        "Waiting": <int>,
                        "Idle": <int>,
                        "Destroying": <int>,
                    },
                    "HTTPAgent": {
                        "Creating": <int>,
                        "Running": <int>,
                        "Waiting": <int>,
                        "Idle": <int>,
                        "Destroying": <int>,
                    },
                },
                "total": <int>,
                "Creating": <int>,
                "Running": <int>,
                "Waiting": <int>,
                "Idle": <int>,
                "Destroying": <int>,
                "n_95userCPU": <int>,
                "n_95sysCPU": <int>,
            }
        """
        snap_type_str = "SDB_SNAP_SESSIONS"
        snap_type_int = self.SnapType[snap_type_str]
        cr = self.db.get_snapshot(snap_type_int)
        res_list = self._walk_snap_cursor_list(cr)
        cr.close()
        res_data = {
            "split_type": {
                "Agent": {
                    "Creating": 0,
                    "Running": 0,
                    "Waiting": 0,
                    "Idle": 0,
                    "Destroying": 0,
                },
                "ShardAgent": {
                    "Creating": 0,
                    "Running": 0,
                    "Waiting": 0,
                    "Idle": 0,
                    "Destroying": 0,
                },
                "ReplAgent": {
                    "Creating": 0,
                    "Running": 0,
                    "Waiting": 0,
                    "Idle": 0,
                    "Destroying": 0,
                },
                "HTTPAgent": {
                    "Creating": 0,
                    "Running": 0,
                    "Waiting": 0,
                    "Idle": 0,
                    "Destroying": 0,
                },
            },
            "total": 0,
            "Creating": 0,
            "Running": 0,
            "Waiting": 0,
            "Idle": 0,
            "Destroying": 0,
            "n_95userCPU": 0,
            "n_95sysCPU": 0,
            "slowest_second": 0,
        }
        list_userCPU = []
        list_sysCPU = []
        now_time = time.time()
        for s in res_list:
            if "UserCPU" in s:
                list_userCPU.append(s["UserCPU"])
            if "SysCPU" in s:
                list_sysCPU.append(s["SysCPU"])
        list_userCPU.sort()
        _95userCPU = list_userCPU[int(len(list_userCPU)*0.95)]
        list_sysCPU.sort()
        _95sysCPU = list_sysCPU[int(len(list_sysCPU)*0.95)]

        oldest_query_time = now_time
        for s in res_list:
            res_data["total"] += 1
            status = s["Status"]
            type_ = s["Type"]
            userCPU = s["UserCPU"]
            sysCPU = s["SysCPU"]
            if type_ in res_data["split_type"]:
                if status in res_data["split_type"][type_]:
                    res_data["split_type"][type_][status] += 1
            if status in res_data:
                res_data[status] += 1
            if userCPU > _95userCPU:
                res_data["n_95userCPU"] += 1
            if sysCPU > _95sysCPU:
                res_data["n_95sysCPU"] += 1
            if status == "Running" and s["LastOpType"] == "Query" and s["LastOpBegin"] != "--":
                # 2020-07-03-14.34.49.465546
                t = datetime.strptime(s["LastOpBegin"], "%Y-%m-%d-%H:%M:%S.%f")
                if t < oldest_query_time:
                    oldest_query_time = t
        res_data["slowest_second"] = now_time - oldest_query_time
        return res_data

    def cs_report_local(self):
        res = {}
        snap_type_str = "SDB_SNAP_COLLECTIONSPACES"
        snap_type_int = self.SnapType[snap_type_str]
        cr = self.db.get_snapshot(
            snap_type_int, 
            condition = self.conf.get("cs_cond", {"Name": "PanicAndFillMe"}),
            selector = {
                "Name": "",
                "TotalSize": "",
                "FreeSize": "",
                "TotalDataSize": "",
                "FreeDataSize": "",
            }
        )
        res_list = self._walk_snap_cursor_list(cr)
        cr.close()
        for s in res_list:
            res[s["Name"]] = {k: v for k, v in s.items() if k != "Name"}
        jsonfile = self._cs_report_local_file()
        with open(jsonfile, "w") as f:
            json.dump(res, f)
        return "1"
        
    def cs_report_remote(self, cs_name):
        res = {}
        jsonfile = self._cs_report_local_file()
        with open(jsonfile, "r") as f:
            t = json.load(f)
        res = t[cs_name]
        res["UsedSize"] = res.get("TotalSize", 0) - res.get("FreeSize", 0)
        res["UsedDataSize"] = res.get("TotalDataSize", 0) - res.get("FreeDataSize", 0)
        return res
        
    def cs_report(self, cs_name):
        res = {}
        snap_type_str = "SDB_SNAP_COLLECTIONSPACES"
        snap_type_int = self.SnapType[snap_type_str]
        cr = self.db.get_snapshot(
            snap_type_int, 
            condition = self.conf.get("cs_cond", {"Name": cs_name}),
            selector = {
                "Name": "",
                "TotalSize": "",
                "FreeSize": "",
                "TotalDataSize": "",
                "FreeDataSize": "",
            }
        )
        res_list = self._walk_snap_cursor_list(cr)
        cr.close()
        return res_list[0]

    def discovery_collectionspaces(self):
        """
        Returns:
            {
                "data": [
                    {
                        "{#DB}": <str>,
                        "{#HOST}": <str>,
                        "{#SERVICE}": <int>,
                        "{#CSNAME}": <str>,
                        # "{#UNIQUEID}": <int>,
                    }
                ]
            }
        """
        res = {"data": []}
        snap_type_str = "SDB_SNAP_COLLECTIONSPACES"
        snap_type_int = self.SnapType[snap_type_str]
        cr = self.db.get_snapshot(snap_type_int)
        res_list = self._walk_snap_cursor_list(cr)
        cr.close()
        for s in res_list:
            name = s["Name"]
            uniqueid = s["UniqueID"]
            res["data"].append({
                "{#DB}": self.info,
                "{#HOST}": self.host,
                "{#SERVICE}": self.service,
                "{#CSNAME}": name,
                # "{#UNIQUEID}": uniqueid,
            })
        return res

    def discovery_collectionspaces_host(self):
        """
        """
        res = {"data": []}
        snap_type_str = "SDB_SNAP_COLLECTIONSPACES"
        snap_type_int = self.SnapType[snap_type_str]
        cr = self.db.get_snapshot(
            snap_type_int, 
            condition = self.conf.get("cs_cond", {"Name": "PanicAndFillMe"}),
            selector = {
                "Name": "",
            }
        )
        res_list = self._walk_snap_cursor_list(cr)
        cr.close()
        for s in res_list:
            name = s["Name"]
            res["data"].append({
                "{#CSNAME}": name
            })
        return res

    def collectionspaces(self, zbx_format):
        """
        Returns:
            {
                "csname": <str>,
                "PageSize": <int>,
                "LobPageSize": <int>,
                "TotalSize": <int>,
                "FreeSize": <int>,
                "TotalDataSize": <int>,
                "FreeDataSize": <int>,
                "TotalIndexSize": <int>,
                "FreeIndexSize": <int>,
                "TotalLobSize": <int>,
                "FreeLobSize": <int>,
                "n_Collection": <int>,
                "n_Group": <int>,
            }
        """
        tmp_lst = zbx_format.split("_", 2)
        cs_name = tmp_lst[-1]
        res = {
            "csname": None,
            "PageSize": 0,
            "LobPageSize": 0,
            "TotalSize": 0,
            "FreeSize": 0,
            "TotalDataSize": 0,
            "FreeDataSize": 0,
            "TotalIndexSize": 0,
            "FreeIndexSize": 0,
            "TotalLobSize": 0,
            "FreeLobSize": 0,
            "n_Collection": 0,
            "n_Group": 0,
        }
        snap_type_str = "SDB_SNAP_COLLECTIONSPACES"
        snap_type_int = self.SnapType[snap_type_str]
        condition = {"Name": cs_name}
        cr = self.db.get_snapshot(snap_type_int, condition=condition)
        res_dict = self._walk_snap_cursor_dict(cr)
        cr.close()
        res["csname"] = cs_name
        res["PageSize"] = res_dict.get("PageSize", -1)
        res["LobPageSize"] = res_dict.get("LobPageSize", -1)
        res["TotalSize"] = res_dict.get("TotalSize", -1)
        res["FreeSize"] = res_dict.get("FreeSize", -1)
        res["TotalDataSize"] = res_dict.get("TotalDataSize", -1)
        res["FreeDataSize"] = res_dict.get("FreeDataSize", -1)
        res["TotalIndexSize"] = res_dict.get("TotalIndexSize", -1)
        res["FreeIndexSize"] = res_dict.get("FreeIndexSize", -1)
        res["TotalLobSize"] = res_dict.get("TotalLobSize", -1)
        res["FreeLobSize"] = res_dict.get("FreeLobSize", -1)
        res["n_Collection"] = len(res_dict.get("Collection", []))
        res["n_Group"] = len(res_dict.get("Group", []))

        return res

    def listCollectionSpaces(self):
        res = []
        cr = self.db.list_collection_spaces()
        res_list = self._walk_snap_cursor_list(cr)
        cr.close()
        for i in res_list:
            if "Name" in i:
                res.append(i["Name"])
        return res

def discovery_inst(type_):
    import zbx_sdb_config
    res = {"data": []}
    for db, value in zbx_sdb_config.db_config.items():
        for k,v in value.get("flag", {}).items():
            if k == type_ and v:
                res["data"].append({
                    "{#INST}": db,
                    "{#INFO}": value.get("info", "UNKNOW"),
                    "{#IP}": value["connect"]["host"],
                    "{#SERVICE}": value["connect"]["service"],
                })
    return res

def discovery_all_cs():
    import zbx_sdb_config
    res = {"data": []}
    for name, inst in zbx_sdb_config.db_config.items():
        connect = inst["connect"]
        zbx_sdb = SDBInst(**connect)
        zbx_sdb.connect()
        for cs in zbx_sdb.listCollectionSpaces():
            res["data"].append({
                "{#DB}": name,
                "{#INFO}": inst.get("info", "UNKNOW"),
                "{#IP}": connect["host"],
                "{#SERVICE}": connect["service"],
        "{#CSNAME}": cs,
            })
        zbx_sdb.close()
    return res

def main():
    import sys
    if len(sys.argv) <= 1:
        raise ValueError("the input args is empty")
    else:
        logging.debug("the input is {!s}".format(sys.argv))

    res = None
    if sys.argv[1] in globals():
        func = sys.argv[1]
        args = sys.argv[2:]
        res = globals().get(func)(*args)
    else:
        # config for connect
        import zbx_sdb_config
        db = "_".join(sys.argv[1].split("_", 2)[0:2])
        logging.debug("after split, get the db is {!s}".format(db))
        if db not in zbx_sdb_config.db_config:
            raise ValueError("the db is not found in db_config")
        db_config_connect = zbx_sdb_config.db_config[db]["connect"]
        func = sys.argv[2]
        args = sys.argv[3:]
        zbx_sdb = SDBInst(**db_config_connect)
        zbx_sdb.add_conf(zbx_sdb_config.db_config[db])
        zbx_sdb.connect()
        zbx_sdb.set_info(zbx_sdb_config.db_config[db].get("info", "UNKNOW"))
        res = zbx_sdb.call(func, *args)

    if isinstance(res, dict):
        res = json.dumps(res, ensure_ascii=False)
        logging.debug("the res is dict, finish json dumps convert")
    logging.debug("the result of item key call is:\n{!s}".format(res))
    print(res)


if __name__ == "__main__":
    init_logger("debug", "local")
    try:
        main()
    except Exception as e:
        logging.exception(e)
        raise e