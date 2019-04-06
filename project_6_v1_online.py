import json
import requests
import threading
import re
import traceback
from tqdm import tqdm

MAX_THREADS = 5
OVERLAP_URL = 'http://35.186.166.22:8082/portal/servlet/service/Poheader.poi'

SELECTED_LABEL = 'http://bizlem.io/PurchaseOrderProcessing#'

def to_float(s):
    s_format = ''.join(re.split('[^\d\.]', s))
    return float(s_format)

def is_float(s):
    s_format = ''.join(re.split(',', s))
    try:
        num = float(s_format)
        return True
    except ValueError:
        return False


def p6_process_json(path, header_input, verbose=True):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    

    slist = data['concatenation']
    start_line_index = data['header_info']['start_line_index']
    stop_line_index = data['header_info']['stop_line_index']

    header_pos_temp = []

    header_input = json.loads(header_input)['header_transform_step1']

    for header_data in header_input:
        item = {}
        for key, value in header_data.items():
            index = key.find('_')
            if index >= 0:
                main_key = key[index + 1:]
                item[main_key] = value 
        header_pos_temp.append(item)

    header_pos = []
    for i, header_data in enumerate(header_pos_temp):
        left_x = 0
        right_x = 1000000

        if i > 0:
            left_x = int(header_pos_temp[i - 1]['x2']) + 1

        if i < len(header_pos_temp) - 1:
            right_x = int(header_pos_temp[i + 1]['x1']) - 1

        header_pos.append({
            'line_index': header_data['line_index'],
            'url': '',
            'url_found': False,
            'word': '',
            'x1': left_x,
            'x2': right_x,
            'y': int(header_data['y']),
        })

        header_pos.append({
            'line_index': header_data['line_index'],
            'url': header_data['url'],
            'url_found': header_data['url_found'] == 'Y',
            'word': header_data['word'],
            'x1': int(header_data['x1']),
            'x2': int(header_data['x2']),
            'y': int(header_data['y']),
        })

    if verbose:
        print(json.dumps(header_pos, indent=2))

    items_all = []
            
    def overlap(word_pos, header_item_pos):
        try:
            data = json.dumps({
                'user_name': 'carrotrule_xyz.com',
                'project_name': 'DataExtractionPO-2',
                'Rule_Engine': 'RuleData-2',
                'RawJson': {
                    'wX1': word_pos[0],
                    'wX2': word_pos[1],
                    'hX1': header_item_pos[0],
                    'hX2': header_item_pos[1],
                }
            })

            r = requests.post(OVERLAP_URL, data=data)
            return r.json()['hw1o'] == 'OVERLAP'
        except Exception as e:
            traceback.print_exc()
            return word_pos[1] >= header_item_pos[0] and word_pos[0] <= header_item_pos[1]
       

    def overlap_async(word_pos, header_item_pos, res, index):
        r = overlap(word_pos, header_item_pos)
        res[index] = r


    def find_header_async(word_pos, header_pos, res, line_index, word_index):
        res_overlap = [ False ] * len(header_pos)
        unknown_cols = []

        threads = [ threading.Thread(
            target=overlap_async, 
            args=(word_pos, (header['x1'], header['x2']), res_overlap, i)
        ) for i, header in enumerate(header_pos) ]

        for thread in threads: thread.start()
        for thread in tqdm(threads): thread.join()

        r = None
        for i, header in enumerate(header_pos):
            if res_overlap[i]:
                if header['word'] != '':
                    r = i 
                    break
                else:
                    unknown_cols.append(i)

        if r is None and len(unknown_cols) > 0:
            r = unknown_cols[0]

        res[line_index][word_index] = r


    def create_table_item(type='Data no header'):
        return {
            'type': type,
            'header': '',
            'header_x1': '',
            'header_x2': '',
            'header_y': '',
            'data': '',
            'line_index': '',
            'data_type': '',
            'data_x1': '',
            'data_x2': '',
            'data_y': '',
        }
        

    table_data = []
    last_header_index = None
    last_line_index = None

    if verbose:
        print(start_line_index, stop_line_index)

    # requesting overlap
    res_overlap = []
    for line_index in range(stop_line_index):
        if line_index < start_line_index + 1:
            res_overlap.append(None)
        else:
            res_overlap.append([ False ] * len(slist[line_index]['words']))

    args = []
    for line_index, words in enumerate(slist[start_line_index + 1:stop_line_index]):
        for word_index, word in enumerate(words['words']):
            args.append((
                (word['x1'], word['x2']), 
                header_pos, 
                res_overlap, 
                line_index + (start_line_index + 1), 
                word_index,
            ))

    if verbose: print('requesting overlapping...')
    for i in range(0, len(args), MAX_THREADS):
        threads = [ 
            threading.Thread(target=find_header_async, args=args[j]) 
            for j in range(i, min(i + MAX_THREADS, len(args))) 
        ]

        for thread in threads: thread.start()
        for thread in threads: thread.join()

    if verbose: print('finish requesting overlapping')

    # processing
    for line_index, words in enumerate(slist[start_line_index + 1:stop_line_index]):
        for word_index, word in enumerate(words['words']):
            header_index = res_overlap[line_index + (start_line_index + 1)][word_index]

            table_item = create_table_item()
            table_item['data'] = word['word']
            table_item['line_index'] = words['line_index']
            table_item['data_type'] = 'number' if is_float(word['word']) else 'string'
            table_item['data_x1'] = word['x1']
            table_item['data_x2'] = word['x2']
            table_item['data_y'] = words['y']
            table_item['header_index'] = header_index
            

            if last_line_index is not None:
                stop_header_index = header_index + len(header_pos) * (words['line_index'] - last_line_index)

                # not consecutive header
                if last_header_index is not None and (stop_header_index - last_header_index) > 1: 
                    for i in range(last_header_index + 1, stop_header_index):
                        current_header = header_pos[i % len(header_pos)]
                        if current_header['word'] == '': continue

                        no_data_item = create_table_item(type='Header no data')
                        no_data_item['header'] = current_header['url'] if current_header['url_found'] else current_header['word']
                        no_data_item['header_y'] = current_header['y']
                        no_data_item['header_x1'] = current_header['x1']
                        no_data_item['header_x2'] = current_header['x2']
                        no_data_item['line_index'] = last_line_index + (i // len(header_pos)) 
                        no_data_item['header_index'] = i % len(header_pos)

                        table_data.append(no_data_item)
                    
            
            header = header_pos[header_index]
            last_header_index = header_index 
            last_line_index = words['line_index']
            
            if header['word'] != '':
                table_item['type'] = 'Overlap'
                
            table_item['header_x1'] = header['x1']
            table_item['header_x2'] = header['x2']
            table_item['header_y'] = header['y']
            table_item['header'] = header['url'] if header['url_found'] else header['word']
                
            table_data.append(table_item)

    # merging step
    if len(table_data) == 0:
        return {
            'table_data': [],
            'table_rows': [],
        }

    last_item = table_data[0]
    table_data_temp = [ ]
    for table_item in table_data[1:]:
        if table_item['header'] != '' \
            and table_item['header'] == last_item['header'] \
            and table_item['line_index'] == last_item['line_index']:

            last_item['data'] += ' ' + table_item['data']
            last_item['data_x2'] = table_item['data_x2']
            last_item['data_type'] = 'string'
        else:
            table_data_temp.append(last_item)
            last_item = table_item 

    table_data_temp.append(last_item)
    table_data = table_data_temp


    table_rows = []
    item = {}
    count = 0
    current_line_index = None

    for table_item in table_data:
        if current_line_index is not None  \
            and table_item['line_index'] != '' \
            and table_item['line_index'] != current_line_index:

            item['line_index'] = current_line_index 
            table_rows.append(item)
            item = {}

        header = table_item['header']

        if table_item['type'] != 'Data no header':
            item.setdefault(header, '')
            if len(item[header]) > 0:
                item[header] += ' '
            item[header] += table_item['data']
        else:
            item['unknown_' + str(count)] = table_item['data']


        if table_item['line_index'] != '':
            current_line_index = table_item['line_index']



    result = {
        'table_data': table_data,
        'table_rows': table_rows,
    }
            

    return result


if __name__ == '__main__':
    print('hello world')

