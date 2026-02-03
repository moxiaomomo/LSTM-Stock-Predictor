import multiprocessing
import akshare as ak
import time
import json
import random

industrys = [{"name":"造纸印刷","code":"BK0470"},
{"name":"电池","code":"BK1033"},
{"name":"消费电子","code":"BK1037"},
{"name":"光伏设备","code":"BK1031"},
{"name":"化学制品","code":"BK0538"},
{"name":"化学原料","code":"BK1019"},
{"name":"美容护理","code":"BK1035"},
{"name":"装修装饰","code":"BK0725"},
{"name":"电子化学品","code":"BK1039"},
{"name":"公用事业","code":"BK0427"},
{"name":"化纤行业","code":"BK0471"},
{"name":"半导体","code":"BK1036"},
{"name":"光学光电子","code":"BK1038"},
{"name":"贵金属","code":"BK0732"},
{"name":"专用设备","code":"BK0910"},
{"name":"化肥行业","code":"BK0731"},
{"name":"橡胶制品","code":"BK1018"},
{"name":"农药兽药","code":"BK0730"},
{"name":"塑料制品","code":"BK0454"},
{"name":"交运设备","code":"BK0429"},
{"name":"石油行业","code":"BK0464"},
{"name":"家用轻工","code":"BK0440"},
{"name":"煤炭行业","code":"BK0437"},
{"name":"保险","code":"BK0474"},
{"name":"家电行业","code":"BK0456"},
{"name":"包装材料","code":"BK0733"},
{"name":"环保行业","code":"BK0728"},
{"name":"银行","code":"BK0475"},
{"name":"中药","code":"BK1040"},
{"name":"电机","code":"BK1030"},
{"name":"专业服务","code":"BK1043"},
{"name":"非金属材料","code":"BK1020"},
{"name":"通用设备","code":"BK0545"},
{"name":"旅游酒店","code":"BK0485"},
{"name":"电网设备","code":"BK0457"},
{"name":"电力行业","code":"BK0428"},
{"name":"通信设备","code":"BK0448"},
{"name":"仪器仪表","code":"BK0458"},
{"name":"汽车零部件","code":"BK0481"},
{"name":"燃气","code":"BK1028"},
{"name":"医疗器械","code":"BK1041"},
{"name":"风电设备","code":"BK1032"},
{"name":"电子元件","code":"BK0459"},
{"name":"纺织服装","code":"BK0436"},
{"name":"航天航空","code":"BK0480"},
{"name":"证券","code":"BK0473"},
{"name":"有色金属","code":"BK0478"},
{"name":"装修建材","code":"BK0476"},
{"name":"房地产开发","code":"BK0451"},
{"name":"物流行业","code":"BK0422"},
{"name":"电源设备","code":"BK1034"},
{"name":"农牧饲渔","code":"BK0433"},
{"name":"房地产服务","code":"BK1045"},
{"name":"汽车服务","code":"BK1016"},
{"name":"酿酒行业","code":"BK0477"},
{"name":"计算机设备","code":"BK0735"},
{"name":"生物制品","code":"BK1044"},
{"name":"钢铁行业","code":"BK0479"},
{"name":"食品饮料","code":"BK0438"},
{"name":"化学制药","code":"BK0465"},
{"name":"多元金融","code":"BK0738"},
{"name":"小金属","code":"BK1027"},
{"name":"铁路公路","code":"BK0421"},
{"name":"汽车整车","code":"BK1029"},
{"name":"贸易行业","code":"BK0484"},
{"name":"采掘行业","code":"BK1017"},
{"name":"能源金属","code":"BK1015"},
{"name":"医疗服务","code":"BK0727"},
{"name":"通信服务","code":"BK0736"},
{"name":"航空机场","code":"BK0420"},
{"name":"教育","code":"BK0740"},
{"name":"工程建设","code":"BK0425"},
{"name":"工程咨询服务","code":"BK0726"},
{"name":"工程机械","code":"BK0739"},
{"name":"玻璃玻纤","code":"BK0546"},
{"name":"船舶制造","code":"BK0729"},
{"name":"航运港口","code":"BK0450"},
{"name":"商业百货","code":"BK0482"},
{"name":"软件开发","code":"BK0737"},
{"name":"综合行业","code":"BK0539"},
{"name":"珠宝首饰","code":"BK0734"},
{"name":"游戏","code":"BK1046"},
{"name":"医药商业","code":"BK1042"},
{"name":"互联网服务","code":"BK0447"},
{"name":"水泥建材","code":"BK0424"},
{"name":"文化传媒","code":"BK0486"},
]

def query_all_stocks_daily():
    #单次获取所有A+H股实时行情
    stock_zh_ah_spot_em_df = ak.stock_zh_ah_spot_em()
    columns = stock_zh_ah_spot_em_df[["名称","A股代码","A股-涨跌幅"]].copy()

    results = []
    for idx, row in columns.iterrows():
        code = row["A股代码"]
        prename = row["名称"]
        change_rate = row["A股-涨跌幅"]
        results.append({
            "name": prename+"."+code,
            "size": abs(change_rate),
            "value": change_rate
        })
    print(len(results))

def query_stock_base_info():
    rows = []
    sectors = set()
    with open('data/stock_info_20260203.jsonl', 'r', encoding='utf-8') as fd:
        for line in fd:
            row = json.loads(line)
            rows.append(row)
            
    for row in rows:
        sectors.add(row["sector"])
    
    df_industry = ak.stock_board_industry_name_em()
    columns1 = df_industry[["板块名称","板块代码"]].copy()
    #print(df_industry.head())
    
    for idx, row in columns1.iterrows():
        #print(row["板块名称"], row["板块代码"])
        if row["板块名称"] in sectors:
            continue
        print(row["板块名称"])
        
        for i in range(10):
            try:
                print("bankuai:"+row["板块名称"])
                stock_board_industry_cons_em_df = ak.stock_board_industry_cons_em(symbol=row["板块名称"])
                # print(stock_board_industry_cons_em_df.head())
                columns2 = stock_board_industry_cons_em_df[["代码","名称","涨跌幅"]].copy()
                
                stocks = []
                for idx2, row2 in columns2.iterrows():
                    #print(row2["代码"], row2["名称"], row2["涨跌幅"])
                    stocks.append({
                        "name": row2["名称"],
                        "code": row2["代码"],
                        "fluctuation": row2["涨跌幅"]
                    })
                print(stocks)
                
                bankuai = {
                    "sector": row["板块名称"],
                    "stocks": stocks
                }
                with open('data/stock_info_20260203.jsonl', 'a', encoding='utf-8') as fd:
                    fd.write(json.dumps(bankuai, ensure_ascii=False)+"\n")
                
                time.sleep(180)
                break
            except Exception as e:
                time.sleep(300+i*30)
                print("板块个股重试查询:{}".format(row["板块名称"]))
                if i==9:
                    print("板块个股查询失败:{}".format(row["板块名称"]))
                    
                    
def worker(bankuai):
    time.sleep(random.randint(1, 8))
    try:
        print("bankuai:"+bankuai)
        stock_board_industry_cons_em_df = ak.stock_board_industry_cons_em(symbol=bankuai)
        columns2 = stock_board_industry_cons_em_df[["代码","名称","涨跌幅"]].copy()
        
        stocks = []
        for idx2, row2 in columns2.iterrows():
            stocks.append({
                "name": row2["名称"],
                "code": row2["代码"],
                "fluctuation": row2["涨跌幅"]
            })
        print(stocks)
        
        bankuai = {
            "sector": bankuai,
            "stocks": stocks
        }
        with open('data/stock_info_20260203.jsonl', 'a', encoding='utf-8') as fd:
            fd.write(json.dumps(bankuai, ensure_ascii=False)+"\n")

    except Exception as e:
        print("板块个股查询失败:{}".format(bankuai))    

# 查询某日各股涨跌幅
if __name__ == "__main__":
    #query_all_stocks_daily()
    #query_stock_base_info()
    rows = []
    sectors = set()
    with open('data/stock_info_20260203.jsonl', 'r', encoding='utf-8') as fd:
        for line in fd:
            row = json.loads(line)
            rows.append(row)
            
    for row in rows:
        sectors.add(row["sector"])
    
    df_industry = ak.stock_board_industry_name_em()
    columns1 = df_industry[["板块名称","板块代码"]].copy()
    
    bankuais = []
    for idx, row in columns1.iterrows():
        #print(row["板块名称"], row["板块代码"])
        if row["板块名称"] in sectors:
            continue
        bankuais.append(row["板块名称"])
        
    jobs = []
    for i in range(5):
        p = multiprocessing.Process(target=worker, args=(bankuais[i],))
        jobs.append(p)
        p.start()
    
    for job in jobs:
        job.join()