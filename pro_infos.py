#! /usr/bin/env python
# -*-coding:utf-8 -*-
from flask import Flask, abort, render_template
from flask import jsonify
from flask import request
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import json

app = Flask(__name__)

client = Elasticsearch(hosts=["http://...:9200/", "http://....:9200/",
                              "http://...:9200/"])  # 连接原生的elasticsearch

create_date = datetime.now().strftime('%Y-%m-%d')


@app.route('/wish/pro_infos', methods=['GET', 'POST'])
def mer_infos():  # 商品信息
    error = None
    if request.method == 'POST':

        title = request.form['title']  # 商品标题
        pid = request.form['pid']  # 商品pid
        f_cid = request.form['f_cid']  # 商品一级类目
        cid = request.form['cid']  # 商品叶子类目
        totalprice_start = request.form['totalprice_start']  # 商品总价
        totalprice_end = request.form['totalprice_end']  # 商品总价

        gen_time_start = request.form['gen_time_start']  # 商品上架时间
        gen_time_end = request.form['gen_time_end']  # 商品上架时间
        gen_seven = request.form['gen_seven']  # 商品前7天上架
        gen_fourteen = request.form['gen_fourteen']  # 商品前14天上架
        gen_thirty = request.form['gen_thirty']  # 商品前30天上架

        approved_start = request.form['approved_start']  # 店铺开张时间开始值
        approved_end = request.form['approved_end']  # 店铺开张时间结束值
        salesweek1_start = request.form['salesweek1_start']  # 近一周销量起始值
        salesweek1_end = request.form['salesweek1_end']  # 近一周销量结束值

        salesweek2_start = request.form['salesweek2_start']  # 上周销量起始值
        salesweek2_end = request.form['salesweek2_end']  # 上周销量结束值

        two_weeks = request.form['two_weeks']  # 店铺前14天内开张
        last_week = request.form['last_week']  # 店铺前7天内开张
        last_month = request.form['last_month']  # 店铺前30天内开张

        is_promo = request.form['is_promo']  # 是否加钻 0为未加钻
        is_Hwc = request.form['is_Hwc']  # 是否海外仓　0为非海外仓

        sort = request.form['sort']  # 需要排序的字段
        order = request.form['order']  # 升序或降序的方式  排序字段和排序方式要一起传

        day = request.form['day']  # 指定多少天的数据

        merchant = request.form['merchant']
        mid = request.form['mid']

        create_date = datetime.now().strftime('%Y-%m-%d')

        sql = []
        now = datetime.now()

        if title:
            t = {"match": {"title": title}}
            sql.append(t)

        if pid:  # 标题和pid只能输一个
            p = {"match": {"pid": pid}}
            sql.append(p)

        if f_cid:  #

            f = {"match": {"cid": f_cid}}
            sql.append(f)
            # sql.append({"term": {"create_date": create_date}})

            if cid:
                cid = {"match": {"c_ids": cid}}

                sql.append(cid)

        if totalprice_start:  # 只有起始值，无结束值

            totalprice = {"gte": totalprice_start}

            if totalprice_end:
                totalprice["lte"] = totalprice_end

            sql.append({"range": {"totalprice": totalprice}})

        if gen_time_start:  # 商品上架时间

            gen_time = {"gte": gen_time_start}

            if gen_time_end:
                gen_time["lte"] = gen_time_end

            sql.append({"range": {"gen_time": gen_time}})

        if gen_seven:  # 商品前7天上架
            day7 = (now - timedelta(days=7)).strftime('%Y-%m-%d')

            sql.append({"range": {"gen_time": {"gte": day7, 'lte': create_date}}})

        if gen_fourteen:  # 商品前14天上架
            day14 = (now - timedelta(days=14)).strftime('%Y-%m-%d')
            sql.append({"range": {"gen_time": {"gte": day14, 'lte': create_date}}})

        if gen_thirty:  # 商品前30天上架
            day30 = (now - timedelta(days=30)).strftime('%Y-%m-%d')
            sql.append({"range": {"gen_time": {"gte": day30, 'lte': create_date}}})

        if approved_start:

            approved = {"gte": approved_start}

            if approved_start and approved_end:

                approved["lte"] = approved_end

            sql.append({"range": {"approved_date": approved}})

        if last_week:  # 店铺前7天内开张

            day7 = (now - timedelta(days=7)).strftime('%Y-%m-%d')

            sql.append({"range": {"approved_date": {"gte": day7, 'lte': create_date}}})

        if two_weeks:  # 店铺前14天内开张

            day14 = (now - timedelta(days=14)).strftime('%Y-%m-%d')
            sql.append({"range": {"approved_date": {"gte": day14, 'lte': create_date}}})

        if last_month:  # 店铺前30天内开张

            day30 = (now - timedelta(days=30)).strftime('%Y-%m-%d')
            sql.append({"range": {"approved_date": {"gte": day30, 'lte': create_date}}})

        if salesweek1_start:  # 只有起始值，无结束值 近一周销量起始值
            salesweek1 = {"gte": salesweek1_start}

            if salesweek1_start and salesweek1_end:
                salesweek1["lte"] = salesweek1_end

            sql.append({"range": {"salesweek1": salesweek1}})

        if salesweek2_start:  # 上周销量起始值

            salesweek2 = {"gte": salesweek2_start}

            if salesweek2_start and salesweek2_end:

                salesweek2["lte"] = salesweek2_end

            sql.append({"range": {"salesweek2": salesweek2}})

        if is_promo:  # 是否加钻 0为未加钻
            sql.append({"term": {"is_promo": is_promo}})

        if is_Hwc:  # 是否海外仓　0为非海外仓
            sql.append({"term": {"is_HWC": is_Hwc}})

        if merchant:
            sql.append({"match": {"merchant": merchant}})

        if mid:
            sql.append({"term": {"mid": mid}})

        if sql:

            if day:  # 如果传天数就返回指定天数内的数据，否则返回当天的数据
                day_district = (now - timedelta(days=(int(day) - 1))).strftime('%Y-%m-%d')  # 指定多少天的数据
                sql.append({"range": {"present": {"gte": day_district, 'lte': create_date}}})

            else:
                sql.append({"term": {"present": create_date}})

            if sort and order:  # 如果有排序字段

                search = {"_source": {"excludes": ["skus", "sale_his_data"]}, "query": {"bool": {"must": sql}},
                          'from': 0, 'size': 1000,
                          "sort": {sort: {"order": order}}}
                print(search)
                response = client.search(
                    index="wish_data",
                    # # doc_type=["Accessories", 'Makeup_Beauty', "Fashion", "Baby_Kids", "Gadgets", "Hobbies",
                    #           "Home_Decor",
                    #           "Phone_Upgrades", "Shoes", "Wallets_Bags", "Watches"],
                    doc_type="pro_datas",
                    body=search)

                total_size = response['hits']['total']
                if total_size > 0:  #

                    l = []
                    hits = response['hits']['hits']
                    for i in range(len(hits)):
                        l.append(hits[i])

                    if total_size % 50 == 0:
                        n = (total_size // 50)

                    else:
                        n = (total_size // 50) + 1

                    return jsonify({'status': 200, 'total_size': total_size, 'result': l, 'total_pages': n})

                else:  # 结果为空
                    return jsonify({'status': 200, 'total_size': total_size, 'result': []})

            elif not sort:  # 如果不进行排序，默认按照销量从大到小排序

                search = {"_source": {"excludes": ["skus", "sale_his_data"]}, "query": {"bool": {"must": sql}},
                          'from': 0, 'size': 1000,
                          "sort": {'salesweek1': {"order": "desc"}}}
                print(json.dumps(search))

                with open('pro.logs', 'a') as f:

                    f.write(':'.join([str(now), str(search)]))
                    f.write('\n' + '====' + '\n')

                response = client.search(
                    index="wish_data",
                    # # doc_type=["Accessories", 'Makeup_Beauty', "Fashion", "Baby_Kids", "Gadgets", "Hobbies",
                    #           "Home_Decor",
                    #           "Phone_Upgrades", "Shoes", "Wallets_Bags", "Watches"],
                    doc_type="pro_datas",
                    body=search)

                total_size = response['hits']['total']
                if total_size > 0:  #

                    l = []
                    hits = response['hits']['hits']
                    for i in range(len(hits)):
                        l.append(hits[i])

                    if total_size % 50 == 0:
                        n = (total_size // 50)

                    else:
                        n = (total_size // 50) + 1

                    return jsonify({'status': 200, 'total_size': total_size, 'result': l, 'total_pages': n})

                else:  # 结果为空
                    return jsonify({'status': 200, 'total_size': total_size, 'result': []})

        elif not sql:  # 传过来的参数为空
            if sort and order:  # 如果有排序字段

                search = {"_source": {"excludes": ["skus", "total_size", "ppid",]}, "query": {"bool": {"must":
                            [{"match_all": {}}, {"term": {"present": create_date}}]}},
                          "from": 0, "size": 5000, "sort": {sort: {"order": order}}}
                print(search)
                response = client.search(
                    index="wish_data",
                    # # doc_type=["Accessories", 'Makeup_Beauty', "Fashion", "Baby_Kids", "Gadgets", "Hobbies",
                    #           "Home_Decor",
                    #           "Phone_Upgrades", "Shoes", "Wallets_Bags", "Watches"],
                    doc_type="pro_datas",
                    body=search)

                total_size = response['hits']['total']
                if total_size > 0:  #

                    l = []
                    hits = response['hits']['hits']
                    for i in range(len(hits)):
                        l.append(hits[i])

                    if total_size % 50 == 0:
                        n = (total_size // 50)

                    else:
                        n = (total_size // 50) + 1

                    return jsonify({'status': 200, 'total_size': total_size, 'result': l, 'total_pages': n})

                else:  # 结果为空
                    return jsonify({'status': 200, 'total_size': total_size, 'result': []})

            elif not sort:  # 如果不进行排序，默认按照销量从大到小排序

                search = {"_source": {"excludes": ["skus", "sale_his_data"]},
                          "query": {"bool": {"must": [{"match_all": {}}, {"term": {"present": create_date}}]}},
                          'from': 0, 'size': 1000,
                          "sort": {'salesweek1': {"order": "desc"}}}
                print(search)

                response = client.search(
                    index="wish_data",
                    doc_type="pro_datas",
                    body=search)

                total_size = response['hits']['total']
                if total_size > 0:  #

                    l = []
                    hits = response['hits']['hits']
                    for i in range(len(hits)):
                        l.append(hits[i])

                    if total_size % 50 == 0:
                        n = (total_size // 50)

                    else:
                        n = (total_size // 50) + 1

                    return jsonify({'status': 200, 'total_size': total_size, 'result': l, 'total_pages': n})

                else:  # 结果为空
                    return jsonify({'status': 200, 'total_size': total_size, 'result': []})

    # return render_template('pro.html', error=error)


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=80, debug=True)
    # app.run(debug=True)

