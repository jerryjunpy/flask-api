#! /usr/bin/env python
# -*-coding:utf-8 -*-
from flask import Flask, abort, render_template
from flask import jsonify
from flask import request
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

app = Flask(__name__)

client = Elasticsearch(hosts=("http://....:9200/"))  # 连接原生的elasticsearch

create_date = datetime.now().strftime('%Y-%m-%d')


@app.route('/wish/mer_infos', methods=['GET', 'POST'])
def mer_infos():  # 店铺信息
    error = None

    if request.method == 'POST':

        merchant = request.form['merchant']
        salesweek1_start = request.form['salesweek1_start']  # 销量起始值
        salesweek1_end = request.form['salesweek1_end']  # 销量结束值
        approved_start = request.form['approved_start']  # 店铺开张起始值
        approved_end = request.form['approved_end']  # 店铺开张时间结束值
        salesweek2_start = request.form['salesweek2_start']  # 销量起始值
        salesweek2_end = request.form['salesweek2_end']  # 销量结束值
        paymentweek1_start = request.form['paymentweek1_start']  # 销售额起始值
        paymentweek1_end = request.form['paymentweek1_end']  # 销售额结束值
        two_weeks = request.form['two_weeks']
        last_week = request.form['last_week']
        last_month = request.form['last_month']

        mid = request.form['mid']  # 精确查找店铺信息

        sort = request.form['sort']  # 需要排序的字段
        order = request.form['order']  # 升序或降序的方式  排序字段和排序方式要一起传

        create_date = datetime.now().strftime('%Y-%m-%d')

        day = request.form['day']  # 指定多少天的数据

        sql = []
        now = datetime.now()

        if salesweek1_start:  # 只有起始值，无结束值
            salesweek1 = {"gte": salesweek1_start}

            if salesweek1_start and salesweek1_end:
                salesweek1["lte"] = salesweek1_end

            sql.append({"range": {"salesweek1": salesweek1}})

        if approved_start:  # 只有起始值，无结束值

            approved = {"gte": approved_start}

            if approved_start and approved_end:

                approved['lte'] = approved_end

            sql.append({"range": {"approved_date": approved}})

        if salesweek2_start:  # 只有起始值，无结束值

            salesweek2 = {"gte": salesweek2_start}

            if salesweek2_start and salesweek2_end:

                salesweek2["lte"] = salesweek2_end

            sql.append({"range": {"salesweek2": salesweek2}})

        if paymentweek1_start:  # 只有起始值，无结束值

            paymentweek1 = {"gte": paymentweek1_start}

            if paymentweek1_start and paymentweek1_end:
                paymentweek1["lte"] = paymentweek1_end

            sql.append({"range": {"paymentweek1": paymentweek1}})

        if last_week:

            day7 = (now - timedelta(days=7)).strftime('%Y-%m-%d')

            sql.append({"range": {"approved_date": {"gte": day7, 'lte': create_date}}})

        if two_weeks:

            day14 = (now - timedelta(days=14)).strftime('%Y-%m-%d')
            sql.append({"range": {"approved_date": {"gte": day14, 'lte': create_date}}})

        if last_month:

            day30 = (now - timedelta(days=30)).strftime('%Y-%m-%d')
            sql.append({"range": {"approved_date": {"gte": day30, 'lte': create_date}}})

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

                search = {"query": {"bool": {"must": sql}}, 'from': '0', 'size': 10000,
                          "sort": {sort: {"order": order}}}
                with open('mer.logs', 'a') as f:

                    f.write(':'.join([str(now), str(search)]))
                    f.write('\n' + '====' + '\n')

                response = client.search(
                    index="wish_mer",
                    doc_type="mer_infos",
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

                search = {"query": {"bool": {"must": sql}}, 'from': '0', 'size': 10000,
                          "sort": {'salesweek1': {"order": "desc"}}}

                with open('mer.logs', 'a') as f:

                    f.write(':'.join([str(now), str(search)]))
                    f.write('\n' + '====' + '\n')

                response = client.search(
                    index="wish_mer",
                    doc_type="mer_infos",
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

        elif not sql:  # 传过来的参数全部为空
            if sort and order:  # 如果有排序字段

                search = {"query": {"bool": {"must": [{"match_all": {}}, {"term": {"present": create_date}}]}},
                          "from": 0, "size": 1000, "sort": {sort: {"order": order}}}

                with open('mer.logs', 'a') as f:

                    f.write(':'.join([str(now), str(search)]))
                    f.write('\n' + '====' + '\n')

                response = client.search(
                    index="wish_mer",
                    doc_type="mer_infos",
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

                search = {"query": {"bool": {"must": [{"match_all": {}}, {"term": {"present": create_date}}]}},
                          'from': 0, 'size': 1000, "sort": {'salesweek1': {"order": "desc"}}}

                with open('mer.logs', 'a') as f:

                    f.write(':'.join([str(now), str(search)]))
                    f.write('\n' + '====' + '\n')

                response = client.search(
                    index="wish_mer",
                    doc_type="mer_infos",
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

    # return render_template('merchant.html', error=error)


if __name__ == '__main__':

    # app.run(host='0.0.0.0', port=80, debug=True)
    app.run(debug=True)

