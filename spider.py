# -*- coding: utf-8 -*-
import traceback

import requests
import pymysql
import json
import time
from settings import db_infos


def get_conn():
    # 建立数据库连接
    conn = pymysql.connect(
        host = '127.0.0.1',
        user = db_infos['user'],
        password = db_infos['password'],
        db = db_infos['db']
    )
    # 创建游标
    cursor = conn.cursor()
    return conn, cursor


def close_conn(conn, cursor):
    # 关闭数据库连接
    if cursor:
        cursor.close()
    if conn:
        conn.close()


def get_tencent_data():
    """
    return:返回历史数据和当日的详细数据
    """
    url = "https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5"
    url_history = "https://view.inews.qq.com/g2/getOnsInfo?name=disease_other"
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50'
    }

    r = requests.get(url, headers)
    # print(r.status_code)
    res = json.loads(r.text)
    data_all = json.loads(res['data'])
    r_history = requests.get(url_history, headers)
    res_history = json.loads(r_history.text)
    history_data = json.loads(res_history['data'])

    # print(data_all)
    history = {}
    for i in history_data["chinaDayList"]:
        ds = "2020." + i["date"]
        # print(ds)
        tup = time.strptime(ds, "%Y.%m.%d")
        ds = time.strftime("%Y-%m-%d", tup)  # 改变时间格式
        confirm = i["confirm"]
        suspect = i["suspect"]
        heal = i["heal"]
        dead = i["dead"]
        history[ds] = {"confirm_add": confirm, "suspect_add": suspect, "heal_add": heal, "dead_all": dead}
    details = []  # 当日详细信息
    update_time = data_all["lastUpdateTime"]
    data_country = data_all["areaTree"]  # 25个国家
    data_province = data_country[0]["children"]
    for pro_infos in data_province:
        province = pro_infos["name"]  # 省名
        for city_infos in pro_infos["children"]:
            city = city_infos["name"]
            confirm = city_infos["total"]["confirm"]
            confirm_add = city_infos["total"]["confirm"]
            heal = city_infos["total"]["heal"]
            dead = city_infos["total"]["dead"]
            details.append([update_time, province, city, confirm, confirm_add, heal, dead])
    return history, details


def update_details():
    """更新detail表"""
    cursor = None
    conn = None
    try:
        li = get_tencent_data()[1] # 0是历史数据字典， 1是最新详细数据列表
        conn, cursor = get_conn()
        sql = "insert into details(update_time, province, city, confirm, confirm_add, heal, dead) values (%s,%s,%s,%s,%s,%s,%s)"
        sql_query = "select %s=(select update_time from details order by id desc limit 1)"
        cursor.execute(sql_query, li[0][0])
        if not cursor.fetchone()[0]:
            print(f"{time.asctime()}开始更新最新数据")
            for item in li:
                cursor.execute(sql, item)
            conn.commit()  # 提交事务
            print(f"{time.asctime()}更新最新数据完毕")
        else:
            print(f"{time.asctime()}已是最新数据")
    except:
        traceback.print_exc()
    finally:
        close_conn(conn, cursor)


def insert_history():
    """插入历史数据"""
    cursor = None
    conn = None
    try:
        dic = get_tencent_data()[0]  # 0是历史数据字典、1 最新详细数据列表
        print(f"{time.asctime()}开始插入历史数据")
        conn, cursor = get_conn()
        sql = "insert into history values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for k, v in dic.items():
            cursor.execute(sql, [k, v.get("confirm"), v.get("confirm_add"), v.get("suspect"),
                                 v.get("suspect_add"), v.get("heal"), v.get("heal_add"),
                                 v.get("dead"), v.get("dead_add")])
        conn.commit()
        print(f"{time.asctime()}插入历史数据完毕")
    except:
        traceback.print_exc()
    finally:
        close_conn(conn, cursor)


def update_history():
    """更新历史数据"""
    cursor = None
    conn = None
    try:
        dic = get_tencent_data()[0]   # 0是历史数据字典、1 最新详细数据列表
        print(f"{time.asctime()}开始更新历史数据")
        conn, cursor = get_conn()
        sql = "insert into history values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql_query = "select confirm from history where ds=%s"
        for k, v in dic.items():
            if not cursor.execute(sql_query, k):
                cursor.execute(sql, [k, v.get("confirm"), v.get("confirm_add"), v.get("suspect"),
                                 v.get("suspect_add"), v.get("heal"), v.get("heal_add"),
                                 v.get("dead"), v.get("dead_add")])
        conn.commit()
        print(f"{time.asctime()}历史数据更新完毕")
    except:
        traceback.print_exc()
    finally:
        close_conn(conn, cursor)


if __name__ == '__main__':
    insert_history()
    # history = get_tencent_data()[0]
    # print(history)
    # history = get_tencent_data()[0]  腾讯关闭了历史数据接口
    # print(history)
    # li = get_tencent_data()[1]
    # for item in li:
    #     print(item)
    # update_details()