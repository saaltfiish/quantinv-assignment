# Quantinv-assignment

## 环境和依赖

开发环境的系统是`Ubuntu 22.04.2 LTS`，`Python`版本是`3.10.12`。依赖的包都在 `requirements.txt`里，运行下面命令安装依赖：
```
pip install -r requirements.txt
```
在安装pysqlite3的时候如果遇到 `fatal error: sqlite3.h: No such file or directory`，需要运行如下命令安装`libsqlite3-dev`：
```
sudo apt-get install libsqlite3-dev
```
为了本地运行方便，选择了最轻量级的`sqlite`，只需要一个本地`.db`文件就可以运行。数据表的`DDL`文件是`EMFund.sql`，可以执行脚本创建表，也可以直接用创建好表的空数据库
```
cp EMFund_empty.db EMFund.db
```

## 运行方式和期望结果

程序分为本地模式和数据库模式：本地模式仅依靠本地文件读写模拟数据库读写，数据库模式则真实使用数据库。

程序的执行逻辑是：
1. 创建一个`DBFund`实例，尝试装载数据。
2. 如果初始状态数据内容为空，从[天天基金列表](https://fund.eastmoney.com/fund.html)爬取前20个基金产品的历史净值，存入数据库
3. 计算基金产品的年度评估指标，保存到默认路径`data/`下
4. 计算基金产品的月度评估指标，保存到默认路径`data/`下

建议先运行以下命令，以本地模式运行程序
```
python3 quantinv.py -l
```
然后运行以下命令，以数据库模式运行程序
```
python3 quantinv.py
```

## 解题思路

#### 爬虫
1. 单个基金的历史数据页面url模板是`http://fundf10.eastmoney.com/jjjz_{code}.html`，其中`code`是基金代码，可以从基金列表页爬到。
2. 历史数据接口的url模板是`http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery18305293200554312854_1705643555097`，参数如下：
- `fundCode`: 基金代码
- `pageIndex`: 当前页码
- `pageSize`: 每页大小
3. 发现这个接口有简单的反爬措施：有`Referer`检验，所以在请求的`header`里加入了`Referer`：

#### 数据处理和指标计算
1. 从接口拿到的单位净值、累计净值、净值增长率，都只精确到万分之一，累计运算误差很大。其中累计净值是最精准的状态量，用下面公式可以算出当日精确的收益率，首日的收益率记为0。
$$当日收益率 = (当日累计净值 - 昨日累计净值) / 昨日累计净值$$
2. 总收益率按产品从第一天起，复利（累乘）计算
3. 年化收益率按总收益率缩放至自然年计算
$$年化收益率 = 总收益率 / 产品存续期总自然日天数 * 一年总自然日天数$$
4. 夏普值按公式计算，最大回撤按最低累计净值减一算
5. 单年、单月指标也按照上面方法计算，作用在对应时间段的数据切片中

#### 数据库
1. 记`local`flag，标记是否本地模式
2. 记`pdata`，同步数据库内容，装载数据时存入`pdata`
3. 增加数据条目时，筛选出不存在于数据库的`(Code, TradingDay)`组合，避免冲突