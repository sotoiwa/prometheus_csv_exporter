import argparse
import collections
import csv
import datetime
import logging
import re
import subprocess

import pprint
import requests
import urllib3


formatter = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.WARNING, format=formatter)
logger = logging.getLogger(__name__)


# 警告を非表示にする
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# コマンド引数の処理
parser = argparse.ArgumentParser(description='PodのCPU使用率をcsvに出力します。')
parser.add_argument('-f', '--filename',
                    action='store',
                    type=str,
                    help='出力先のファイル名を指定します')
parser.add_argument('-n', '--namespace',
                    action='store',
                    type=str,
                    default='default',
                    help='Namespaceを指定します')
parser.add_argument('--interval',
                    action='store',
                    type=str,
                    default='5m',
                    help='CPU使用率計算に使用するデータの間隔を指定します（例）1h、5m')
parser.add_argument('--start',
                    action='store',
                    type=str,
                    help='データの開始時間を指定します（例）20190101-1000')
parser.add_argument('--end',
                    action='store',
                    type=str,
                    help='データの終了時間を指定します（例）20190102-1000')
parser.add_argument('--step',
                    action='store',
                    type=int,
                    help='データポイントの間隔（秒）を指定します')
args = parser.parse_args()

filepath = args.filename
namespace = args.namespace
interval = args.interval
step = args.step
logger.debug('filepath: {}'.format(filepath))
logger.debug('interval: {}'.format(interval))
logger.debug('step: {}'.format(step))

# 引数の開始時間と終了時間をUNIX時刻に変換
start_str = args.start
end_str = args.end
start_dt = datetime.datetime.strptime(start_str, '%Y%m%d-%H%M')
end_dt = datetime.datetime.strptime(end_str, '%Y%m%d-%H%M')
start_unix = start_dt.timestamp()
end_unix = end_dt.timestamp()
logger.debug('start_dt: {}'.format(start_dt))
logger.debug('end_dt: {}'.format(end_dt))
logger.debug('start_unix: {}'.format(start_unix))
logger.debug('end_unix: {}'.format(end_unix))

# サブプロセスでコマンドを実行し、結果からアクセストークンを抽出
completed_process = subprocess.run(['cloudctl', 'tokens'], stdout=subprocess.PIPE)
result_str = completed_process.stdout.decode('utf-8')
match = re.search(r'(.*)\s+Bearer\s+(.*)', result_str)
access_token = (match.group(2))
logger.debug('access_token: {}'.format(access_token))

# Prometheusクエリー
# 指定のNamespaceの、指定のintervalで算出したPod毎のCPU使用率を取得する
# sum(rate(container_cpu_usage_seconds_total{namespace="$namespace"}[$interval])) by (pod_name) * 100
query = 'sum(rate(container_cpu_usage_seconds_total{{namespace="{}"}}[{}])) ' \
        'by (pod_name) * 100'.format(namespace, interval)
logger.debug('query: {}'.format(query))

# リクエスト
url = 'https://mycluster.icp:8443/prometheus/api/v1/query_range'
headers = {'Authorization': 'Bearer {}'.format(access_token)}
params = {'query': query,
          'start': start_unix,
          'end': end_unix,
          'step': step}
logger.debug('url: {}'.format(url))
logger.debug('headers: {}'.format(headers))
logger.debug('params: {}'.format(params))


# リクエストを実行
response = requests.get(url, verify=False, headers=headers, params=params)
response.raise_for_status()
logger.debug('response: {}'.format(response))

# レスポンスは以下のようなデータ
# pprint.pprint(response.json())
# {'data': {'result': [{'metric': {'pod_name': 'infra-test-nodeport-cust-0'},
#                       'values': [[1547528400, '2.64939279124293'],
#                                  [1547532000, '2.5820633706497045'],
#                                  [1547535600, '2.562417181158173'],
#                                  [1547539200, '2.4563804665536724'],

# 意味のない部分を取り除いて中のリストを取り出す
results = response.json()['data']['result']

# 取り出したのは以下のようなデータ
# pprint.pprint(results)
# [{'metric': {'pod_name': 'infra-test-nodeport-cust-0'},
#   'values': [[1547528400, '2.64939279124293'],
#              [1547532000, '2.5820633706497045'],
#              [1547535600, '2.562417181158173'],
#              [1547539200, '2.4563804665536724'],
#
# このデータを時刻をキーにして以下のような辞書にまとめる
#
# {1547464889.632: {'infra-test-nodeport-cust-0': '3.1518179124293577',
#                   'infra-test-nodeport-cust-1': '1.530811175762711',
#                   'infra-test-nodeport2-cus-0': '3.0063879859887037',
#                   'infra-test-nodeport2-cus-1': '1.5241500936723127'},
#  1547468489.632: {'infra-test-nodeport-cust-0': '3.161739384943495',
#                   'infra-test-nodeport-cust-1': '1.5393470943785368',
#                   'infra-test-nodeport2-cus-0': '2.8831145322598943',
#                   'infra-test-nodeport2-cus-1': '1.578976048757047'},

# 時刻毎のデータの辞書を用意する
time_series = collections.defaultdict(dict)

# Pod名のSetを用意する
pod_names = set()

for result in results:
    # Pod名を取り出してSetに入れておく
    pod_name = result['metric']['pod_name']
    pod_names.add(pod_name)
    for value in result['values']:
        # timestampを辞書のキーにすることで同じtimestampのデータをまとめる
        # defaultdictを使うことでキーがなくてもKeyErrorにならない
        time_series[value[0]][pod_name] = value[1]

# pprint.pprint(time_series)

# csvのヘッダーは時刻とPod名にする
fieldnames = ['timestamp']
fieldnames.extend(pod_names)

# csvファイルに保存する
with open(filepath, 'w') as csv_file:

    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    # 辞書から時間毎のデータを取り出してループする
    for timestamp, values in time_series.items():
        # 行に時間の列を追加
        row = {'timestamp': datetime.datetime.fromtimestamp(timestamp)}
        # valuesは以下のような辞書
        # {'infra-test-nodeport-cust-0': '2.467225521553685',
        #  'infra-test-nodeport-cust-1': '1.5932590068361583',
        #  'infra-test-nodeport2-cus-0': '2.2811341803954917',
        #  'infra-test-nodeport2-cus-1': '1.6517850743220521'},
        # 事前に格納したPod名のリストの方でループする
        for pod_name in pod_names:
            try:
                row[pod_name] = values[pod_name]
            except KeyError:
                # valuesにこのPodのデータがないときはKeyErrorが発生するので空データを入れる
                row[pod_name] = ''
        writer.writerow(row)
