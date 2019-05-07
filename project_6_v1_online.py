import json
import requests
import threading
import re
import traceback
from tqdm import tqdm

MAX_THREADS = 5
HEADER_URL = 'http://34.74.243.55:8086/PO_Processing/header_process'

SELECTED_LABEL = 'http://bizlem.io/PurchaseOrderProcessing#'


def request_header_line(line_list, res, line_index):
    try:
        print(data)
        r = requests.post(HEADER_URL, data=data)
        res[line_index] = r.json()
    except Exception as e:
        traceback.print_exc()


def p6_process_json(path, verbose=True):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    
    table_data = data['table_data']
    if len(table_data) == 0: return []


    def process_item_data(item, current_line, count):
        data_arr = re.split('[\s\|\!]', item['data'])
        if 'Description' not in item['header'] and len(data_arr) > 1:
            sub_count = 0
            for sub_item in data_arr:
                current_line['C' + str(count)] = [
                    item['header'] + '_' + str(sub_count) ,
                    sub_item,
                ]
                count += 1
                sub_count += 1
        else:
            current_line['C' + str(count)] = [
                item['header'],
                item['data'],
            ]
            count += 1

        return count

    current_line_index = table_data[0]['line_index']
    current_line = {}
    count = 1
    count = process_item_data(table_data[0], current_line, count)
    line_list = []

    for item in table_data[1:]:
        if item['line_index'] == current_line_index:
            count = process_item_data(item, current_line, count)
        else:
            line_list.append(current_line)
            current_line = {}
            count = 1
            count = process_item_data(item, current_line, count)
            current_line_index = item['line_index']

    if count != 1:
        line_list.append(current_line)

    print(json.dumps(line_list, indent=2))
    return

    res = [ None ] * len(line_list)
    threads = [ threading.Thread(
        target=request_header_line,
        args=(line_list, res, line_index),
    ) for line_index in range(len(line_list)) ]

    for thread in threads: thread.start()
    for thread in threads: thread.join()

    return res

if __name__ == '__main__':
    print('hello world')

