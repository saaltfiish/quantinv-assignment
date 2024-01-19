#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import json
import math
import os
import re
import sys
import time
from typing import List, Union
import numpy as np

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

from utils import logger

PAGESIZE = 1000
YEAR_NTD = 252
RATE_R_F = 2.5
PATH_LDB = "data/local_db.csv"
FUND_NUM = 20
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)
NAME_MAP = {
    "FSRQ": "TradingDay",
    "DWJZ": "UnitNAV",
    "LJJZ": "CumNAV",
    "JZZZL": "Return",
}
SAVE_COL = ["Code", "Name", "TradingDay", "UnitNAV", "CumNAV", "Return"]


class EMFund(object):
    def __init__(self, code: str, name: str) -> None:
        self.code = code
        self.name = name
        self.data = []

    def get_data(self) -> List:
        if len(self.data) == 0:
            self._scrape_data()
        return self.data

    def format_dataframe(self) -> pd.DataFrame:
        ret = pd.DataFrame().from_records(self.data)
        ret["Code"] = self.code
        ret["Name"] = self.name
        ret.rename(columns=NAME_MAP, inplace=True)
        ret = ret[SAVE_COL]
        return ret

    def export_data(self, path: str) -> None:
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

    def refresh_data(self) -> None:
        self._scrape_data()
        return

    def _scrape_data(self) -> None:
        page = 1
        size = PAGESIZE
        base = "http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery18305293200554312854_1705643555097"
        pars = {
            "fundCode": self.code,
            "pageIndex": page,
            "pageSize": size,
        }
        head = {"Referer": f"http://fundf10.eastmoney.com/jjjz_{self.code}.html"}

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
            cnt = dat["TotalCount"]
            self.nrec = cnt
        except KeyError:
            msg = "unknown total record count"
            logger.error(msg)
            raise KeyError(msg)

        pag = math.ceil(cnt / size)

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
            logger.debug(dat["PageIndex"])
            self.data += dat["Data"]["LSJZList"]
        return


class DBFund(object):
    def __init__(self, local: bool = False) -> None:
        self.local = local
        self.pdata = pd.DataFrame()

    def load(self):
        if self.local:
            if os.path.exists(PATH_LDB):
                self.pdata = pd.read_csv(PATH_LDB, dtype={"Code": str})
        return

    def empty(self) -> bool:
        if self.local:
            return len(self.pdata) == 0
        return False

    def add(self, fund: Union[EMFund, pd.DataFrame]) -> None:
        if self.local:
            if isinstance(fund, EMFund):
                self.pdata = pd.concat([self.pdata, fund.format_dataframe()])
            else:
                self.pdata = pd.concat([self.pdata, fund])
            self.pdata.drop_duplicates(
                subset=["Code", "Name", "TradingDay"], keep="first", inplace=True
            )
        return

    def save(self) -> None:
        if self.local:
            self.pdata.to_csv(PATH_LDB, index=False)
        return

    def make_repo(self, month: bool = False) -> pd.DataFrame:
        tmp = []
        if month:
            src = self.pdata.copy()
            src["year"] = src["TradingDay"].str.slice(0, 4)
            src["month"] = src["TradingDay"].str.slice(5, 7)
            for key, cut in src.groupby(["Code", "year", "month"]):
                row = {
                    "Code": key[0],
                    "Name": cut["Name"].values[0],
                    "Year": key[1],
                    "Month": key[2],
                    "Return": cut["Return"].mean() * YEAR_NTD / 12,
                }
                tmp.append(row)
        else:
            src = self.pdata.copy()
            for cc in src["Code"].unique():
                cut = src.loc[src["Code"] == cc, :].copy()
                row = {"Code": cc, "Name": cut["Name"].values[0]}
                logger.debug(f"making report for {cc} {row['Name']}")
                row["TotalReturn"] = (cut["Return"] + 1).prod() - 1
                row["YearReturn"] = cut["Return"].mean() * YEAR_NTD
                row["TotalShapre"] = (cut["Return"].mean() * YEAR_NTD - RATE_R_F) / (
                    np.std(cut["Return"]) * np.sqrt(YEAR_NTD)
                )
                row["TotalMaxDrawDown"] = cut["CumNAV"].min() - 1
                # tmp.append(row)
                cut["year"] = cut["TradingDay"].str.slice(0, 4)
                for yy in cut["year"].unique():
                    yut = cut.loc[cut["year"] == yy, :].copy()
                    logger.debug(f"making report for {cc} {row['Name']} {yy}")
                    row[f"{yy}_Return"] = yut["Return"].mean() * YEAR_NTD
                    row[f"{yy}_Shapre"] = (
                        yut["Return"].mean() * YEAR_NTD - RATE_R_F
                    ) / (np.std(yut["Return"]) * np.sqrt(YEAR_NTD))
                    row[f"{yy}_MaxDrawDown"] = yut["CumNAV"].min() - 1
                tmp.append(row)
        ret = pd.DataFrame().from_records(tmp)
        return ret


def inject_to_db(file: str = None) -> None:
    if file:
        fund = pd.read_csv(file)
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
        fund = EMFund(code, name)
        fund.get_data()
        db.add(fund)
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

    db = DBFund(args.local)
    db.load()
    if db.empty():
        logger.warning("empty db, injecting data...")
        # 不加参数就是用爬虫爬网页注入数据库
        inject_to_db()
        # 加文件路径就是用本地文件注入数据库
        # inject_to_db(file="data/copy_db.csv")
        db.save()
    d1 = db.make_repo(month=False)
    d1.to_csv("data/year_repo.csv", index=False)
    d2 = db.make_repo(month=True)
    d2.to_csv("data/month_repo.csv", index=False)

    sys.exit(0)
