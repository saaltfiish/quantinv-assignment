#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
from typing import List, Union
import numpy as np

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

from utils import logger

# 爬虫爬一页数据条目数
PAGESIZE = 1000
# 一年的自然日数
YEAR_TNR = 365
# 一年的工作日数
BDAY_TNR = 252
# 无风险利率
RATE_R_F = 0.025
# 数据库基金个数
FUND_NUM = 20
# 数据库数据表名
NAME_TAB = "Return"
# 数据库路径
PATH_SQL = "EMFund.db"
# 本地数据文件路径
PATH_LDB = "data/local_db.csv"
# 临时数据保存路径
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)
# 爬取到的列名转成数据库列名的映射
NAME_MAP = {
    "FSRQ": "TradingDay",
    "DWJZ": "UnitNAV",
    "LJJZ": "CumNAV",
    "JZZZL": "Return",
}
# 需要保存的数据库
SAVE_COL = ["Code", "Name", "TradingDay", "UnitNAV", "CumNAV", "Return"]


class EMFund(object):
    def __init__(self, code: str, name: str) -> None:
        self.code = code
        self.name = name
        self.data = []

    def get_data(self) -> List:
        """
        获取基金数据
        
        Returns:
        - List: 基金历史数据列表
        """
        if len(self.data) == 0:
            # 如果自身没有数据缓存，从网页上爬数据
            self._scrape_data()
        return self.data

    def format_dataframe(self) -> pd.DataFrame:
        """
        格式化基金数据至DataFrame
        
        Returns:
        - pandas.DataFrame: 基金历史数据表
        """
        ret = pd.DataFrame().from_records(self.data)
        ret["Code"] = self.code
        ret["Name"] = self.name
        ret.rename(columns=NAME_MAP, inplace=True)
        ret = ret[SAVE_COL]
        return ret

    def export_data(self, path: str) -> None:
        """
        导出基金数据
        
        Parameters:
        - path (str): 导出路径
        """
        if len(self.data) == 0:
            logger.warning(
                f"{self.code} {self.name} has no data, call get_data() first"
            )
        temp = self.format_dataframe()
        if path.endswith("html"):
            temp.to_html(path, index=False)
        else:
            temp.to_csv(path, index=False)
        return

    # def refresh_data(self) -> None:
    #     self._scrape_data()
    #     return

    def _scrape_data(self) -> None:
        """
        从网页爬取基金历史数据
        
        Raises:
        - ValueError: 无法解析爬到的数据
        - KeyError: 爬到的数据缺必要字段
        """
        # 准备目标url
        page = 1
        size = PAGESIZE
        base = "http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery18305293200554312854_1705643555097"
        pars = {
            "fundCode": self.code,
            "pageIndex": page,
            "pageSize": size,
        }
        head = {"Referer": f"http://fundf10.eastmoney.com/jjjz_{self.code}.html"}
        # 请求url并解析返回的内容
        rsp = requests.get(base, headers=head, params=pars).content.decode()
        pat = re.compile(r"\((.*?)\)")
        mat = re.search(pat, rsp)
        if mat:
            dat = json.loads(mat.group(1))
            logger.debug(f"load fund metadata online success")
        else:
            msg = "load fund metadata online failure"
            logger.error(msg)
            raise ValueError(msg)
        try:
            # 获取当前基金历史数据条目总数
            cnt = dat["TotalCount"]
            self.nrec = cnt
        except KeyError:
            msg = "unknown total record count"
            logger.error(msg)
            raise KeyError(msg)
        # 算出需要请求几页
        pag = math.ceil(cnt / size)
        # 每页请求
        for idx in range(pag):
            time.sleep(0.5)
            pars["pageIndex"] = idx + 1
            rsp = requests.get(base, headers=head, params=pars).content.decode()
            mat = re.search(pat, rsp)
            if mat:
                dat = json.loads(mat.group(1))
            else:
                msg = "parse fund data failure"
                logger.error(msg)
                raise ValueError(msg)
            # 先把json转成DataFrame
            tmp = pd.DataFrame().from_records(dat["Data"]["LSJZList"])
            # 累计净值转成float
            tmp["LJJZ"] = tmp["LJJZ"].astype(float)
            # 用两天之间的净值差，倒算出精确的当日净值增长率
            tmp["JZZZL"] = (tmp["LJJZ"] - tmp["LJJZ"].shift(-1)) / tmp["LJJZ"].shift(-1)
            # 第一天的净值增长率是0
            tmp["JZZZL"].values[-1] = 0
            # 转回data规定的List格式
            self.data += tmp.to_dict(orient="records")
        return


class DBFund(object):
    def __init__(self, local: bool = False) -> None:
        # 本地模式
        self.local = local
        # pandas.DataFrame格式的data
        self.pdata = pd.DataFrame()
        # 数据库路径
        self.db_path = None
        # 数据库连接
        self.db_conn = None
        # 数据库游标
        self.db_curs = None
        # 数据库是否装载
        self.db_load = False
        if local:
            # 本地模式
            logger.warning(f"DBFund running with local file: {PATH_LDB}, ignore db")
        else:
            # 数据库模式
            self.db_path = PATH_SQL
            logger.warning(f"DBFund running with db: {PATH_SQL}, ignore local file")
        return

    def __del__(self) -> None:
        if self.db_conn:
        # 数据库模式下，销毁db实例的时候关闭数据库连接
            self.db_conn.close()
        return

    def connect(self) -> None:
        """
        连接数据库
        
        Raises:
        - sqlite3.Error: sqlite异常
        """
        try:
            self.db_conn = sqlite3.connect(self.db_path)
            self.db_curs = self.db_conn.cursor()
            logger.info(f"Connected to sqlite db: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to sqlite db: {e}")

    def load(self) -> None:
        """
        从数据库装载数据
        """
        if self.local:
            if os.path.exists(PATH_LDB):
                # 本地模式下，能找到本地数据文件，读入self.pdata
                self.pdata = pd.read_csv(PATH_LDB, dtype={"Code": str})
        else:
            # 数据库模式下
            if self.db_conn is None:
                # 如果数据连接还没建立，先建立连接
                self.connect()
            query = f"SELECT * FROM {NAME_TAB};"
            # 从数据库中读所有数据到self.pdata
            self.pdata = pd.read_sql_query(query, self.db_conn)
        return

    def empty(self) -> bool:
        """
        目前数据库实例是不是空的
        
        Returns:
        - bool: 是否为空
        """
        return len(self.pdata) == 0

    def add(self, fund: Union[EMFund, pd.DataFrame]) -> None:
        """
        将一个基金所有数据加入数据库
        
        Parameters:
        - fund: 需要加入的基金
        """
        # 如果fund是EMFund类型则需要导出pandas.DataFrame类型的数据
        tmp = fund.format_dataframe() if isinstance(fund, EMFund) else fund
        # 接下来筛选不存在于原来数据库中的条目
        if len(self.pdata) == 0:
            # 如果数据库整体为空，那么直接全加入
            new = tmp
        else:
            # 不然按key=Code+TradingDay，筛选没出现过的
            idx = ~(tmp["Code"] + tmp["TradingDay"]).isin(
                self.pdata["Code"] + self.pdata["TradingDay"]
            )
            new = tmp[idx]
        if not self.local:
            if self.db_conn is None:
                self.connect()
            # 如果不是本地模式，还要插入数据库中
            new.to_sql(NAME_TAB, self.db_conn, index=False, if_exists="append")
        # 无论是不是本地模式，都要append到self.pdata上
        self.pdata = pd.concat([self.pdata, new])
        return

    def save(self) -> None:
        """
        保存数据库状态

        """
        if self.local:
            # 本地模式，输出csv文件到默认路径
            self.pdata.to_csv(PATH_LDB, index=False)
        else:
            if self.db_conn:
                # 数据库模式，再commit一下保证所有状态都写入
                self.db_conn.commit()
        return

    def make_repo(self, month: bool = False) -> pd.DataFrame:
        """
        制作指标报告
        
        Parameters:
        - month: 是否是月度报告

        Returns:
        - pandas.DataFrame: 报告类型是pandas表
        """
        tmp = []
        # 月度报告
        if month:
            # 在拷贝上操作
            src = self.pdata.copy()
            # 切出年和月
            src["year"] = src["TradingDay"].str.slice(0, 4)
            src["month"] = src["TradingDay"].str.slice(5, 7)
            # 按基金+年+月去group
            for key, cut in src.groupby(["Code", "year", "month"]):
                # 子表按日期倒序排列
                cut.sort_values(["TradingDay"], ascending=False, inplace=True)
                # 登记该基金该年该月的指标
                row = {
                    "Code": key[0],
                    "Name": cut["Name"].values[0],
                    "Year": key[1],
                    "Month": key[2],
                    # 按照复利计算月收益，忽略最早那天的return
                    "Return": (cut["Return"].values[:-1] + 1).prod() - 1,
                }
                tmp.append(row)
        # 年度报告
        else:
            # 在拷贝上操作
            src = self.pdata.copy()
            # 按照各个基金计算
            for cc in src["Code"].unique():
                # 切出当前基金
                cut = src.loc[src["Code"] == cc, :].copy()
                # 子表按日期倒序排列
                cut.sort_values(["TradingDay"], ascending=False, inplace=True)
                # 初始化当前列
                row = {"Code": cc, "Name": cut["Name"].values[0]}
                logger.debug(f"making report for {cc} {row['Name']}")
                # 计算产品存续期总自然日天数
                cnt = (
                    pd.to_datetime(cut["TradingDay"].values[0])
                    - pd.to_datetime(cut["TradingDay"].values[-1])
                ).days
                # 总收益按复利计算
                row["TotalReturn"] = (cut["Return"] + 1).prod() - 1
                # 年化收益率 = 总收益率 / 总自然日天数 * 一年自然日天数
                row["YearReturn"] = row["TotalReturn"] / cnt * YEAR_TNR
                # 总夏普
                row["TotalShapre"] = (row["YearReturn"] - RATE_R_F) / (
                    np.std(cut["Return"]) * np.sqrt(BDAY_TNR)
                )
                # 总最大回撤 = 最低净值 - 1
                row["TotalMaxDrawDown"] = cut["CumNAV"].min() - 1
                # 按年计算指标
                cut["year"] = cut["TradingDay"].str.slice(0, 4)
                for yy in cut["year"].unique():
                    # 切出当年数据
                    yut = cut.loc[cut["year"] == yy, :].copy()
                    logger.debug(f"making report for {cc} {row['Name']} {yy}")
                    # 按照复利计算年收益，忽略最早那天的return
                    row[f"{yy}_Return"] = (yut["Return"].values[:-1] + 1).prod() - 1
                    # 年夏普
                    row[f"{yy}_Shapre"] = (row[f"{yy}_Return"] - RATE_R_F) / (
                        np.std(yut["Return"]) * np.sqrt(BDAY_TNR)
                    )
                    # 年最大回撤 = 年最低净值 - 1
                    row[f"{yy}_MaxDrawDown"] = yut["CumNAV"].min() - 1
                tmp.append(row)
        # 转成pandas表返回
        ret = pd.DataFrame().from_records(tmp)
        return ret


def inject_to_db(file: str = None) -> None:
    # 如果是用本地文件装载数据
    if file:
        fund = pd.read_csv(file)
        # 加入数据库
        db.add(fund)
        return
    # 天天基金列表url
    url = "https://fund.eastmoney.com/fund.html"
    # 发送HTTP请求并获取页面内容
    res = requests.get(url)
    txt = res.content.decode("gb2312", "ignore")
    # 使用BeautifulSoup解析HTML内容
    bss = bs(txt, "html.parser")
    # 定位基金列表的表格
    table = bss.find("table", {"id": "oTable"})
    tbody = table.find("tbody")
    # 遍历tbody中的tr
    for i, tr in enumerate(tbody.find_all("tr")):
        if i >= FUND_NUM:
            break
        # 处理每一行的内容
        code = tr.find("td", {"class": "bzdm"}).text
        name = tr.find("td", {"class": "tol"}).find("a").text
        logger.info(f"{i} code={code}, name={name}")
        # 创建一个EastMoney基金实例
        fund = EMFund(code, name)
        # 获取这个基金的数据
        fund.get_data()
        # 加入数据库
        db.add(fund)
        # 导出数据csv至data下
        fund.export_data(f"{DATA_DIR}/{code}_{name}.csv")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quantinv EMFund")
    # 添加命令行参数
    parser.add_argument(
        "-l", "--local", help="run with local files only", action="store_true"
    )
    # 解析命令行参数
    args = parser.parse_args()
    # 创建DB实例，根据命令行参数设置为local或sqlite
    db = DBFund(args.local)
    # 从默认路径装载数据
    db.load()
    # 如果没有装载到数据
    if db.empty():
        logger.warning("empty db, injecting data...")
        # 不加参数就是用爬虫爬网页注入数据库
        inject_to_db()
        # 加文件路径就是用本地文件注入数据库
        # inject_to_db(file="data/local_db.csv")
        # 保存数据库状态
        db.save()
    # 计算基金产品的年度评估指标
    d1 = db.make_repo(month=False)
    d1.to_csv("data/year_repo.csv", index=False)
    # 计算基金产品的月度评估指标
    d2 = db.make_repo(month=True)
    d2.to_csv("data/month_repo.csv", index=False)

    sys.exit(0)
