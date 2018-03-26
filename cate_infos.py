#! /usr/bin/env python
# -*-coding:utf-8 -*-
from flask import Flask, abort, render_template
from flask import jsonify
from flask import request
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import json

app = Flask(__name__)

client = Elasticsearch(hosts=("http://...:9200/"))  # 连接原生的elasticsearch

create_date = datetime.now().strftime('%Y-%m-%d')
now = datetime.now()


@app.route('/wish/cate_infos', methods=['GET', 'POST'])
def mer_infos():  #
    error = None

    if request.method == 'POST':

        f_cid = request.form['f_cid']

        cid = request.form['cid']  #

        sort = request.form['sort']  # 需要排序的字段
        order = request.form['order']  # 升序或降序的方式  排序字段和排序方式要一起传

        cids = request.form['cids']  # 用叶子类目id获取类目表中查询所有商品的信息

        create_date = datetime.now().strftime('%Y-%m-%d')

        day = request.form['day']  # 指定多少天的数据

        sql = []
        sql_1 = []  # 在类目表中查询商品信息的语句

        if f_cid:  #

            f = {"match": {"f_cid": f_cid}}
            sql.append(f)

            if cid:
                cid = {"match": {"cid": cid}}

                sql.append(cid)
        if cids:
            sql_1.append({"match": {"c_ids": cids}})

        if sql:

            if day:  # 如果传天数就返回指定天数内的数据，否则返回当天的数据
                day_district = (now - timedelta(days=(int(day) - 1))).strftime('%Y-%m-%d')  # 指定多少天的数据
                sql.append({"range": {"present": {"gte": day_district, 'lte': create_date}}})

            else:
                sql.append({"term": {"present": create_date}})

            if sort and order:  # 如果有排序字段

                search = {"query": {"bool": {"must": sql}}, 'from': 0, 'size': 10000,
                          "sort": {sort: {"order": order}}}

                with open('cate.logs', 'a') as f:

                    f.write(':'.join([str(now), str(search)]))
                    f.write('\n' + '====' + '\n')

                response = client.search(
                    index="wish_cate",
                    doc_type="cate_infos",
                    body=search)

                total_size = response['hits']['total']
                if total_size > 0:  #

                    l = []
                    hits = response['hits']['hits']

                    for i in range(len(hits)):
                        l.append(hits[i]['_source'])

                    if total_size % 50 == 0:
                        n = (total_size // 50)

                    else:
                        n = (total_size // 50) + 1

                    return jsonify({'status': 200, 'total_size': total_size, 'result': l, 'total_pages': n})

                else:  # 结果为空
                    return jsonify({'status': 200, 'total_size': total_size, 'result': []})

            elif not sort:  # 如果不进行排序，默认按照销量从大到小排序

                search = {"query": {"bool": {"must": sql}}, "from": 0,
                          "size": 10000, "sort": {"salesweek1": {"order": "desc"}}}

                with open('cate.logs', 'a') as f:

                    f.write(':'.join([str(now), str(search)]))
                    f.write('\n' + '====' + '\n')

                response = client.search(
                    index="wish_cate",
                    doc_type="cate_infos",
                    body=search)

                total_size = response['hits']['total']

                if total_size > 0:  #

                    l = []
                    hits = response['hits']['hits']
                    for i in range(len(hits)):
                        l.append(hits[i]['_source'])

                    if total_size % 50 == 0:
                        n = (total_size // 50)

                    else:
                        n = (total_size // 50) + 1

                    return jsonify({'status': 200, 'total_size': total_size, 'result': l, 'total_pages': n})

                else:  # 结果为空
                    return jsonify({'status': 200, 'total_size': total_size, 'result': []})

        if sql_1:  # 用叶子类目id获取类目表中查询所有商品的信息

            if day:  # 如果传天数就返回指定天数内的数据，否则返回当天的数据
                day_district = (now - timedelta(days=(int(day) - 1))).strftime('%Y-%m-%d')  # 指定多少天的数据
                sql_1.append({"range": {"present": {"gte": day_district, 'lte': create_date}}})

            else:
                sql_1.append({"term": {"present": create_date}})

            if sort and order:  # 如果有排序字段

                search = {"query": {"bool": {"must": sql_1}}, 'from': '0', 'size': 5000,
                          "sort": {sort: {"order": order}}}

                with open('cate.logs', 'a') as f:

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
                        l.append(hits[i]['_source'])

                    if total_size % 50 == 0:
                        n = (total_size // 50)

                    else:
                        n = (total_size // 50) + 1

                    return jsonify({'status': 200, 'total_size': total_size, 'result': l, 'total_pages': n})

                else:  # 结果为空
                    return jsonify({'status': 200, 'total_size': total_size, 'result': []})

            elif not sort:  # 如果不进行排序，默认按照销量从大到小排序

                search = {"query": {"bool": {"must": sql_1}}, 'from': '0', 'size': 5000,
                          "sort": {'dailybought': {"order": "desc"}}}

                with open('cate.logs', 'a') as f:

                    f.write(':'.join([str(now), str(search)]))
                    f.write('\n' + '====' + '\n')

                response = client.search(
                    index="wish_pro",
                    # # doc_type=["Accessories", 'Makeup_Beauty', "Fashion", "Baby_Kids", "Gadgets", "Hobbies",
                    #           "Home_Decor",
                    #           "Phone_Upgrades", "Shoes", "Wallets_Bags", "Watches"],
                    doc_type="pro_infos",
                    body=search)

                total_size = response['hits']['total']
                if total_size > 0:  #

                    l = []
                    hits = response['hits']['hits']
                    for i in range(len(hits)):
                        l.append(hits[i]['_source'])

                    if total_size % 50 == 0:
                        n = (total_size // 50)

                    else:
                        n = (total_size // 50) + 1

                    return jsonify({'status': 200, 'total_size': total_size, 'result': l, 'total_pages': n})

                else:  # 结果为空
                    return jsonify({'status': 200, 'total_size': total_size, 'result': []})

        else:  # 传过来的参数为空
            return jsonify({'status': 200, 'total_size': 0, 'result': []})

    # # return render_template('cateinfo.html', error=error)


@app.route('/wish/category', methods=['GET', 'POST'])
def cate_infos():  # 根据一级类目id获取叶子类目
    error = None

    if request.method == 'POST':
        f_cid = request.form['f_cid']
        cid = request.form['cid']

        if f_cid:  # 根据一级类目查询信息 子类名和ｉｄ

            response = client.search(
                index="wish_cate",
                doc_type="cate_id",
                # 从商品的type中查找商品信息

                body={"query": {"bool": {"must": {"match": {"f_cid": f_cid}}}}, 'from': 0,
                      'size': 100})

            total_size = response['hits']['total']

            if total_size > 0:  #

                l = []
                hits = response['hits']['hits']
                for i in range(len(hits)):
                    l.append(hits[i]['_source'])

                return jsonify({'status': 200, 'total_size': total_size, 'result': l})

            else:  # 如果total_size=0,不存在返回空
                return jsonify({'status': 200, 'total_size': total_size, 'result': []})

        if cid:  # 根据叶子类目查询信息

            response = client.search(
                index="wish_cate",
                doc_type="cate_id",
                # 从商品的type中查找商品信息

                body={"query": {"bool": {"must": {"match": {"id.cid": cid}}}}, 'from': 0,
                      'size': 10})

            total_size = response['hits']['total']

            if total_size > 0:  #

                l = []
                hits = response['hits']['hits']

                for i in hits[0]['_source']['id']:

                    if i['cid'] == cid:
                        l.append(i)
                return jsonify({'status': 200, 'total_size': total_size, 'result': l})

            else:  # 如果total_size=0,不存在返回空
                return jsonify({'status': 200, 'total_size': total_size, 'result': []})

        else:
            return jsonify({'status': '401', 'result': [], 'total_size': 0})


@app.route('/joom/category', methods=['GET', 'POST'])
def joom_cate():  # 根据一级类目id获取叶子类目
    error = None

    if request.method == 'POST':

        f_cid = request.form['f_cid']
        cid = request.form['cid']

        if f_cid:  # 根据一级类目查询信息子类名和ｉｄ

            search = {"query": {"bool": {"must": {"match_phrase": {"categories_id": f_cid}}}},
                      'from': '0', 'size': 1000}

            response = client.search(
                index="joom_cate",
                doc_type="cate_id",
                body=search)

            total_size = response['hits']['total']

            if total_size > 0:  #

                l = []
                hits = response['hits']['hits']
                for i in range(len(hits)):
                    l.append(hits[i]['_source'])

                return jsonify({'status': 200, 'total_size': total_size, 'result': l})

            else:  # 如果total_size=0,不存在返回空
                return jsonify({'status': 200, 'total_size': total_size, 'result': []})

        if cid:  # 根据叶子类目查询信息

            response = client.search(
                index="joom_cate",
                doc_type="cate_id",
                # 从商品的type中查找商品信息

                body={"query": {"bool": {"must": {"match_phrase": {"leaf_categories_id": cid}}}}, 'from': 0,
                      'size': 1000})

            total_size = response['hits']['total']

            if total_size > 0:  #

                l = []
                hits = response['hits']['hits']
                for i in range(len(hits)):
                    l.append(hits[i]['_source'])

                return jsonify({'status': 200, 'total_size': total_size, 'result': l})

            else:  # 如果total_size=0,不存在返回空
                return jsonify({'status': 200, 'total_size': total_size, 'result': []})

        else:
            return jsonify({'status': '401', 'result': [], 'total_size': 0})

    return render_template('cate.html', error=error)


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=80, debug=True)
    # app.run(debug=True)

