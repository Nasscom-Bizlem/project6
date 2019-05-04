import os
import requests
import json
import re
import copy
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = './input_imagejsons'
urlhash = '(\#[A-Za-z].*)'
URL = 'http://34.74.125.253:8082/portal/servlet/service/Poheader.poi'
FURL = 'http://34.80.26.185:8086/PO_Processing/GetDataSolr.flatteringTemp'
TEMURL = 'http://34.80.26.185:8086/PO_Processing/GetDataSolr.Temp'
MERGEDEMERGEURL = 'http://34.80.26.185:8086/PO_Processing_Flattening_Template/TemplateMatch'
HEADERS_JSON = {
    'http://bizlem.io/PurchaseOrderProcessing#DescriptionOfServices': 'descriptionofservices',
    'http://bizlem.io/PurchaseOrderProcessing#Quantity': 'quantity',
    'http://bizlem.io/PurchaseOrderProcessing#Rate': 'rate',
    'http://bizlem.io/PurchaseOrderProcessing#AmountNumbers': 'amountnumbers',
    'http://bizlem.io/PurchaseOrderProcessing#GrossTotal': 'grosstotal',
    'http://bizlem.io/PurchaseOrderProcessing#HSN/SAC': 'hsn/sac',
    }

FOOTERS_JSON = {
    'http://bizlem.io/PurchaseOrderProcessing#CGST': 'cgst',
    'http://bizlem.io/PurchaseOrderProcessing#SGST': 'sgst',
    'http://bizlem.io/PurchaseOrderProcessing#IGST': 'igst',
    'http://bizlem.io/PurchaseOrderProcessing#GrossTotal': 'grandtotal',
    }
FOOTERS_LABELS = ['total', 'round']


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)


def allowed_file(filename, extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() \
        in extensions


@app.route('/image_json', methods=['POST'])
def update_json2():
    files = request.files
    sdata = request.data
    final_json = []
    all_steps_json = {}
    if 'file' not in files:
        if sdata:
            image_json = json.loads(sdata)
        else:
            return (jsonify({'error': 'No files provided'}), 400)
    elif 'file' in files:
        file = request.files['file']
        if file and allowed_file(file.filename, ['json']):
            filename = secure_filename(file.filename)
            path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(path)
            with open(path, encoding='utf-8') as f:
                image_json = json.load(f)

    final_json = {}
    headers = []
    header_footer_json = {}
    merged_headers = {}

    # code to get header indexes with our logic

    header_footer_json = get_header_footer(image_json)


    all_steps_json.update({'1_headers_footers': copy.deepcopy(header_footer_json)})

    merged_headers = new_header_ruleengine(
    	header_footer_json['header'], 
    	all_steps_json,
    )

    all_steps_json.update({'3_merged': copy.deepcopy(merged_headers)})

    # 'header_transform_step1'
    # # call flatening & merge/demerge combined API

    merge_demerge_json = {}
    combined_final_json = {}


    combined_final_json.update({'header_transform_step1': copy.deepcopy(merged_headers)})


    return jsonify(combined_final_json)


def merge_demerge(merged_headers):
    merged = merged_headers
    all_steps_json = {}
    new_json = {}
    api_json = {}
    index = 0
    print('===============================================================================================================')

    # add column key to mergejson

    for (index, item) in enumerate(merged_headers):
        prefix = list(merged_headers[index].keys())[0].split('_')[0]
        item.update({'column' + str(index + 1): 'column' + str(index
                    + 1)})

        # make json with column and url to send template API....

        print('prefix---', prefix)
        if prefix + '_url' in item.keys():
            if isinstance(item[prefix + '_url'], str):
                urlpatt = re.findall(urlhash, item[prefix + '_url'])
                if urlpatt:
                    api_json.update({'column' + str(index
                                    + 1): str(item[prefix + '_url'])})
            else:
                if prefix + '_word' in item.keys():
                    api_json.update({'column' + str(index
                                    + 1): str(item[prefix + '_word'])})

    print('mergedheaders---', merged_headers)

    print('mergeinput---', api_json)
    returned_json = requests.post(MERGEDEMERGEURL,
                                  data=json.dumps(api_json)).json()
    print('mergeoutput---', returned_json)
    for (k, v) in api_json.items():
        if k in returned_json.keys():
            if returned_json[k] == v:
                print('same---', returned_json[k], v)
            else:
                print('not same---', returned_json[k], v)
                urlpatt = re.findall(urlhash, returned_json[k])
                urlpatt2 = re.findall(urlhash, v)
                if urlpatt and not urlpatt2:

                    # search same key in merged json and replace with url value

                    for (index, item) in enumerate(merged_headers):
                        if k in merged_headers[index].keys():
                            merged_headers[index][k] = returned_json[k]
                            print('mjson key----',
                                   merged_headers[index][k],
                                   returned_json[k])

    return merged_headers


def new_header_ruleengine(header_footer_json, all_steps_json):
    merged_json_lst = []
    pheader_footer_json = change_prefixes(header_footer_json)
    all_steps_json.update({'2_with_prefixes': copy.deepcopy(pheader_footer_json)})

    if len(pheader_footer_json) > 1:
        merged_json_lst = new_combine(0, 1, pheader_footer_json)

    return merged_json_lst


def change_prefixes(tmpheader_footer_json):

    print('enu---', tmpheader_footer_json)
    c = 0

    # for index in range(0,len(tmpheader_footer_json),1):

    tmpheader_footer_json = sorted(tmpheader_footer_json, key=lambda i: \
                                   i['x1'])
    for (index, item) in enumerate(tmpheader_footer_json):
        prefix = 'a' + str(index + 1)
        item.update({prefix: prefix})
        if 'url' in item.keys():
            if isinstance(item['url'], str):
                urlpatt = re.findall(urlhash, item['url'])
                if urlpatt:
                    item.update({'url_found': 'Y'})
            else:
                item.update({'url_found': 'N'})

    for (index, item) in enumerate(tmpheader_footer_json):
        print('item---', item)
        prefix = 'a' + str(index + 1)
        tmpheader_footer_json[index].update({prefix + '_column': index
                + 1})

        tmpheader_footer_json[index][prefix + '_line_index'] = \
            tmpheader_footer_json[index].pop('line_index',
                tmpheader_footer_json[index]['line_index'])


        tmpheader_footer_json[index][prefix + '_url'] = \
            tmpheader_footer_json[index].pop('url',
                tmpheader_footer_json[index]['url'])
        item[prefix + '_url_found'] = item.pop('url_found',
                item['url_found'])
        tmpheader_footer_json[index][prefix + '_word'] = item.pop('word'
                , tmpheader_footer_json[index]['word'])
        tmpheader_footer_json[index][prefix + '_x1'] = \
            tmpheader_footer_json[index].pop('x1',
                tmpheader_footer_json[index]['x1'])
        tmpheader_footer_json[index][prefix + '_x2'] = \
            tmpheader_footer_json[index].pop('x2',
                tmpheader_footer_json[index]['x2'])
        tmpheader_footer_json[index][prefix + '_y'] = \
            tmpheader_footer_json[index].pop('y',
                tmpheader_footer_json[index]['y'])


        tmpheader_footer_json[index][prefix + '_type'] = \
            tmpheader_footer_json[index].pop('type',
                tmpheader_footer_json[index]['type'])

    return tmpheader_footer_json


def new_combine(current, next, header_footer_json):

    # rename keys to a1 for current and a2 for next

    current_prefix = list(header_footer_json[current].keys())[0].split('_')[0]
    next_prefix = list(header_footer_json[next].keys())[0].split('_')[0]

    combined_json1 = header_footer_json[current].copy()
    combined_json2 = header_footer_json[next].copy()

    combined_json1['a1_x1'] = combined_json1.pop(current_prefix + '_x1'
            , combined_json1[current_prefix + '_x1'])
    combined_json1['a1_x2'] = combined_json1.pop(current_prefix + '_x2'
            , combined_json1[current_prefix + '_x2'])
    combined_json1['a1'] = combined_json1.pop(current_prefix + '_word',
            combined_json1[current_prefix + '_word'])


    combined_json1['a1_url'] = combined_json1.pop(current_prefix
            + '_url', combined_json1[current_prefix + '_url'])

   
    combined_json1['a1_lno'] = combined_json1.pop(current_prefix
            + '_line_index', combined_json1[current_prefix
            + '_line_index'])
    combined_json1['a1_datatype'] = combined_json1.pop(current_prefix
            + '_type', combined_json1[current_prefix + '_type'])
    combined_json1['a1_cno'] = combined_json1.pop(current_prefix
            + '_column', combined_json1[current_prefix + '_column'])

    # convert values to string

    combined_json1['a1_x1'] = str(combined_json1['a1_x1'])
    combined_json1['a1_x2'] = str(combined_json1['a1_x2'])
    combined_json1['a1'] = str(combined_json1['a1'])
    combined_json1['a1_url'] = str(combined_json1['a1_url'])
    combined_json1['a1_lno'] = str(combined_json1['a1_lno'])
    combined_json1['a1_datatype'] = str(combined_json1['a1_datatype'])
    combined_json1['a1_cno'] = str(combined_json1['a1_cno'])

    combined_json2['a2_x1'] = combined_json2.pop(next_prefix + '_x1',
            combined_json2[next_prefix + '_x1'])
    combined_json2['a2_x2'] = combined_json2.pop(next_prefix + '_x2',
            combined_json2[next_prefix + '_x2'])
    combined_json2['a2'] = combined_json2.pop(next_prefix + '_word',
            combined_json2[next_prefix + '_word'])

    combined_json2['a2_url'] = combined_json2.pop(next_prefix + '_url',
            combined_json2[next_prefix + '_url'])


    combined_json2['a2_lno'] = combined_json2.pop(next_prefix
            + '_line_index', combined_json2[next_prefix + '_line_index'
            ])
    combined_json2['a2_datatype'] = combined_json2.pop(next_prefix
            + '_type', combined_json2[next_prefix + '_type'])
    combined_json2['a2_cno'] = combined_json2.pop(next_prefix
            + '_column', combined_json2[next_prefix + '_column'])


    combined_json2['a2_x1'] = str(combined_json2['a2_x1'])
    combined_json2['a2_x2'] = str(combined_json2['a2_x2'])
    combined_json2['a2'] = str(combined_json2['a2'])
    combined_json2['a2_url'] = str(combined_json2['a2_url'])
    combined_json2['a2_lno'] = str(combined_json2['a2_lno'])
    combined_json2['a2_datatype'] = str(combined_json2['a2_datatype'])
    combined_json2['a2_cno'] = str(combined_json2['a2_cno'])

    combined_json1.update(combined_json2)

    strj = {
        'Rule_Engine': 'MergeRule17Apr',
        'project_name': 'MergeAPI17Apr',
        'user_name': 'carrotrule_xyz.com',
        'RawJson': combined_json1,
    }

    print('hello', json.dumps(strj))
    returned_json = requests.post(URL, data=json.dumps(strj))
    print('yeu nuoc', returned_json.text)
    return returned_json.text

    if 'Output' in returned_json.keys():
        if returned_json['Output'] == 'MERGE':

            # print('mjson--',returned_json['Output'])

            merged_json = new_mergejson(current, next,
                    header_footer_json)
            if current <= len(merged_json) - 2:

                # print('current3--',current)

                final_json = new_combine(current, current + 1,
                        merged_json)
            else:
                return merged_json
        else:

            if next <= len(header_footer_json) - 2:
                final_json = new_combine(next, next + 1,
                        header_footer_json)
            else:
                return header_footer_json
    else:
        if next <= len(header_footer_json) - 2:
            final_json = new_combine(next, next + 1, header_footer_json)
        else:
            return header_footer_json
    return header_footer_json


def new_mergejson(current, next, header_footer_json):
    current_prefix = list(header_footer_json[current].keys())[0].split('_')[0]
    next_prefix = list(header_footer_json[next].keys())[0].split('_')[0]

    # merge values of current into next

    if int(header_footer_json[current][current_prefix + '_x1']) \
        < int(header_footer_json[next][next_prefix + '_x1']):
        header_footer_json[next][next_prefix + '_x1'] = \
            header_footer_json[current][current_prefix + '_x1']

    # header_footer_json[next][next_prefix+'_word']=header_footer_json[current][current_prefix+'_word']+"@@@@@"+header_footer_json[next][next_prefix+'_word']

    header_footer_json[next][next_prefix + '_word'] = \
        header_footer_json[current][current_prefix + '_word'] \
        + header_footer_json[next][next_prefix + '_word']
    del header_footer_json[current]
    return header_footer_json


def get_header(image_json):
    our_header_indexes = []
    original_header_indexes = []
    our_footer_indexes = []
    tmp_header = 0

    # flag=0
    # headers_alldata=[]
    # headers_data=[]

    tmp_headers = []

    # footers_data=[]
    # header_end=0
    # footer_start=0

    all_tmp_headers = []
    tmp = []
    if image_json:
        if 'header_info' in image_json.keys():
            if 'header_line_index' in image_json['header_info']:
                tmp = sorted(image_json['header_info'
                             ]['header_line_index'])
                if tmp:
                    our_header_indexes = list(set(tmp))
                original_header_indexes = \
                    sorted(image_json['header_info']['header_line_index'
                           ])
        if our_header_indexes:
            if 'concatenation' in image_json.keys():
                for itm in image_json['concatenation']:
                    if itm['line_index'] in our_header_indexes \
                        or itm['line_index'] == min(our_header_indexes) \
                        - 1 or itm['line_index'] \
                        == max(our_header_indexes) + 1:
                        tmp_headers = []
                        if 'words' in itm.keys():
                            if itm['line_index'] \
                                == min(our_header_indexes) - 1 \
                                or itm['line_index'] \
                                == max(our_header_indexes) + 1:
                                allalpha = all(str(i['word'
                                        ].encode('utf-8'
                                        )).strip().isalpha() for i in
                                        itm['words'])
                                if allalpha:
                                    print('alpha---', allalpha,
        [i['word'] for i in itm['words']])
                                    for xy in itm['words']:
                                        if isinstance(xy['url'], str):
                                            urlpatt = re.findall(urlhash, xy['url'])
                                            if urlpatt:
                                                if HEADERS_JSON.get(xy['url'
        ], False) != False:
                                                    for i in itm['words'
        ]:
                                                        i.update({'line_index': itm['line_index'
        ]})
                                                        tmp_headers.append(i)
                                        if tmp_headers:
                                            all_tmp_headers.append(tmp_headers)
                                            if itm['line_index'] \
    == min(our_header_indexes) - 1:
                                                our_header_indexes.append(min(our_header_indexes)
        - 1)
                                            elif itm['line_index'] \
    == max(our_header_indexes) + 1:
                                                our_header_indexes.append(max(our_header_indexes)
        + 1)
                                            break
                            else:
                                for xy in itm['words']:
                                    if isinstance(xy['url'], str):
                                        urlpatt = re.findall(urlhash, xy['url'])
                                        if urlpatt:
                                            if HEADERS_JSON.get(xy['url'
        ], False) != False:
                                                for i in itm['words']:
                                                    i.update({'line_index': itm['line_index'
        ]})
                                                    tmp_headers.append(i)
                                    if tmp_headers:
                                        all_tmp_headers.append(tmp_headers)
                                        break

    print('all_tmp_headers---', all_tmp_headers)
    print('original_header_indexes--', original_header_indexes)
    print('our_header_indexes--', our_header_indexes)
    header_footer_json = {}
    header_footer_json = {'header': all_tmp_headers}

    # return all_tmp_headers

    return header_footer_json


def get_header_footer(image_json):
    our_header_indexes = []
    original_header_indexes = []
    our_footer_indexes = []
    tmp_header = 0
    flag = 0
    headers_alldata = []
    headers_data = []
    tmp_headers = []
    footers_data = []
    header_end = 0
    footer_start = 0
    all_tmp_headers = []
    if image_json:
        if 'header_info' in image_json.keys():
            if 'header_line_index' in image_json['header_info']:
                our_header_indexes = image_json['header_info'
                        ]['header_line_index']
                original_header_indexes = image_json['header_info'
                        ]['header_line_index']

            # if headers_index:
                # headers_index.append(min(headers_index)-1)............................

        if our_header_indexes:
            if 'concatenation' in image_json.keys():
                for itm in image_json['concatenation']:
                    if itm['line_index'] in our_header_indexes \
                        or itm['line_index'] == min(our_header_indexes) \
                        - 1 or itm['line_index'] \
                        == max(our_header_indexes) + 1:
                        tmp_headers = []
                        if 'words' in itm.keys():
                            if itm['line_index'] \
                                == min(our_header_indexes) - 1 \
                                or itm['line_index'] \
                                == max(our_header_indexes) + 1:
                                allalpha = all(str(i['word'
                                        ].encode('utf-8'
                                        )).strip().isalpha() for i in
                                        itm['words'])
                                if allalpha:

                                    # print('alpha---',allalpha,[i['word'] for i in itm['words']])

                                    for xy in itm['words']:
                                        if isinstance(xy['url'], str):
                                            urlpatt = re.findall(urlhash, xy['url'])
                                            if urlpatt:
                                                if HEADERS_JSON.get(xy['url'
        ], False) != False:
                                                    for i in itm['words'
        ]:
                                                        i.update({'y': itm['y'
        ], 'line_index': itm['line_index']})
                                                        tmp_headers.append(i)
                                        if tmp_headers:
                                            all_tmp_headers.append(tmp_headers)
                                            our_header_indexes.append(itm['line_index'
        ])
                                            break
                            else:
                                for xy in itm['words']:
                                    if isinstance(xy['url'], str):
                                        urlpatt = re.findall(urlhash, xy['url'])
                                        if urlpatt:
                                            if HEADERS_JSON.get(xy['url'
        ], False) != False:
                                                for i in itm['words']:
                                                    if i['word'].strip().isalpha():
                                                        i.update({'y': itm['y'
        ], 'line_index': itm['line_index']})
                                                        tmp_headers.append(i)

                                    if tmp_headers:
                                        all_tmp_headers.append(tmp_headers)
                                        our_header_indexes.append(itm['line_index'
        ])
                                        break

    print('all_tmp_headers---', all_tmp_headers)
    all_tmp_headers2 = []
    flat_list = []
    for sublist in all_tmp_headers:
        for item in sublist:
            all_tmp_headers2.append(item)
    print('original_header_indexes--',
           list(set(original_header_indexes)))
    print('our_header_indexes--', list(set(our_header_indexes)))
    amturl = 'no'
    max_lno = 0
    alpha = True
    headers_list = []
    tmp_headers_list = []
    for i in list(set(our_header_indexes)):
        for (index, itm) in enumerate(image_json['concatenation']):
            if 'line_index' in itm.keys():
                if itm['line_index'] == i:

                    if 'words' in itm.keys():
                        tmp_headers_list = []
                        for i in itm['words']:
                            i.update({'y': itm['y'],
                                    'line_index': itm['line_index']})
                            tmp_headers_list.append(i)
                        if tmp_headers_list:
                            headers_list.append(tmp_headers_list)

    headers_list2 = []
    for sublist in headers_list:
        for item in sublist:
            headers_list2.append(item)

   

    headers_data = sorted(headers_data, key=lambda i: i['x1'])
    max_lno = 0
    if headers_data:
        max_lno = max(list(set([i['line_index'] for i in
                      headers_data])))

    header_end = max_lno

    # footer process starts here=================================================================================================
    # first get sgst,cgst url index for footers

    flag = 0
    if 'concatenation' in image_json.keys():
        for (index, itm) in enumerate(image_json['concatenation']):
            if itm['line_index'] > max_lno:
                if 'words' in itm.keys():
                    flag = 0
                    for (ind, xy) in enumerate(itm['words']):
                        if 'url' in xy.keys():

                            # print(type(xy['url']))

                            if isinstance(xy['url'], str):
                                urlpatt = re.findall(urlhash, xy['url'])

                                # if urlpatt:
                                    # url=urlpatt[0].replace('#',"").strip().lower()
                                    # if url=='descriptionofservices':
                                        # print('url--',url,urlpatt)

                                if FOOTERS_JSON.get(xy['url'], False) \
                                    != False:
                                    our_footer_indexes.append(itm['line_index'
        ])
                                    flag = 1
                                    break
                    if flag == 1:
                        break
    if our_footer_indexes:
        footer_start = min(our_footer_indexes)
    footer_indexes = []

    # get footer data >= footer start

    if 'concatenation' in image_json.keys():
        for (index, itm) in enumerate(image_json['concatenation']):
            if 'line_index' in itm.keys():
                if itm['line_index'] >= footer_start:
                    if 'words' in itm.keys():
                        for (ind, xy) in enumerate(itm['words']):
                            xy.update({'line_index': itm['line_index']})
                            footers_data.append(xy)
                    footer_indexes.append(itm['line_index'])

    print('all footer indexes---', footer_indexes)

    # sort footer data line by line................................

    all_footer_rows = []
    for i in footer_indexes:
        footer_row = []
        for item in footers_data:
            if item['line_index'] == i:
                footer_row.append(item)
        if footer_row:
            sorted_row = sorted(footer_row, key=lambda i: i['x1'])
            all_footer_rows.append(sorted_row)

            # print('row---',sorted_row)
    # get right most column from rows

    key = ''
    val = ''
    lno = ''
    footer_data_lst = []
    footer_json = {}
    for (index, item) in enumerate(all_footer_rows):
        data = [i['word'] for i in item]
        key = ''
        val = ''
        lno = ''

        # go to last column

        for (ind, i) in enumerate(item):
            lno = i['line_index']
            lst = []
            footer_json = {}
            if ind == len(item) - 1:

                # print('word---',i['word'])

                val = re.findall('(\d{1,})', str(i['word']))
                if val:
                    val = i['word']
                else:
                    val = ''
            else:
                if isinstance(i['url'], str):
                    urlpatt = re.findall(urlhash, i['url'])
                    if urlpatt:
                        if FOOTERS_JSON.get(i['url'], False) != False:
                            key = i['url']
                    else:
                        if str(i['word']) in FOOTERS_LABELS:
                            key = str(i['word'])
                elif i['word'] in FOOTERS_LABELS:
                    key = '###_' + str(i['word'])

            # print('key--val---',key,val)............

            if key != '' and val != '':

                # footer_json.update({str(lno)+"_"+key:val,})

                footer_json.update({key: val, 'line_index': str(lno)})

            if footer_json:
                lst.append(footer_json)
        if lst:
            footer_data_lst.append(lst)


    # add next line of header in array

    if tmp_header > 0:
        our_header_indexes.append(tmp_header)


    print('all headers ----', headers_list2)


    header_footer_json = {'header': headers_list2,
                          'footer': footer_data_lst}
    return header_footer_json


def combine(current, next, headers_xsorted):

    # rename keys to a1 for current and a2 for next

    current_prefix = list(headers_xsorted[current].keys())[0].split('_')[0]
    next_prefix = list(headers_xsorted[next].keys())[0].split('_')[0]

    combined_json1 = headers_xsorted[current].copy()
    combined_json2 = headers_xsorted[next].copy()

    combined_json1['a1_x1'] = combined_json1.pop(current_prefix + '_x1'
            , combined_json1[current_prefix + '_x1'])
    combined_json1['a1_x2'] = combined_json1.pop(current_prefix + '_x2'
            , combined_json1[current_prefix + '_x2'])
    combined_json1['a1_word'] = combined_json1.pop(current_prefix
            + '_word', combined_json1[current_prefix + '_word'])
    combined_json1['a1_lo'] = combined_json1.pop(current_prefix + '_lo'
            , combined_json1[current_prefix + '_lo'])
    combined_json1['a1_url'] = combined_json1.pop(current_prefix
            + '_url', combined_json1[current_prefix + '_url'])
    combined_json1['a1_url_found'] = combined_json1.pop(current_prefix
            + '_url_found', combined_json1[current_prefix + '_url_found'
            ])
    combined_json1['a1_y'] = combined_json1.pop(current_prefix + '_y',
            combined_json1[current_prefix + '_y'])
    combined_json1['a1_x_offset'] = combined_json1.pop(current_prefix
            + '_x_offset', combined_json1[current_prefix + '_x_offset'])
    combined_json1['a1_line_index'] = combined_json1.pop(current_prefix
            + '_line_index', combined_json1[current_prefix
            + '_line_index'])
    combined_json1['a1_type'] = combined_json1.pop(current_prefix
            + '_type', combined_json1[current_prefix + '_type'])

    combined_json2['a2_x1'] = combined_json2.pop(next_prefix + '_x1',
            combined_json2[next_prefix + '_x1'])
    combined_json2['a2_x2'] = combined_json2.pop(next_prefix + '_x2',
            combined_json2[next_prefix + '_x2'])
    combined_json2['a2_word'] = combined_json2.pop(next_prefix + '_word'
            , combined_json2[next_prefix + '_word'])
    combined_json2['a2_lo'] = combined_json2.pop(next_prefix + '_lo',
            combined_json2[next_prefix + '_lo'])
    combined_json2['a2_url'] = combined_json2.pop(next_prefix + '_url',
            combined_json2[next_prefix + '_url'])
    combined_json2['a2_url_found'] = combined_json2.pop(next_prefix
            + '_url_found', combined_json2[next_prefix + '_url_found'])
    combined_json2['a2_y'] = combined_json2.pop(next_prefix + '_y',
            combined_json2[next_prefix + '_y'])
    combined_json2['a2_x_offset'] = combined_json2.pop(next_prefix
            + '_x_offset', combined_json2[next_prefix + '_x_offset'])
    combined_json2['a2_line_index'] = combined_json2.pop(next_prefix
            + '_line_index', combined_json2[next_prefix + '_line_index'
            ])
    combined_json2['a2_type'] = combined_json2.pop(next_prefix + '_type'
            , combined_json2[next_prefix + '_type'])

    combined_json1.update(combined_json2)

    # strj={"user_name":"carrotrule_xyz.com","project_name":"TableHeadingPO","Rule_Engine":"HeadingPORule","RawJson": combined_json1}

    strj = {
        'Rule_Engine': 'POMergeRule',
        'project_name': 'POMergeAPI',
        'user_name': 'carrotrule_xyz.com',
        'RawJson': combined_json1,
        }

    # print('input--',strj)

    returned_json = requests.post(URL, data=json.dumps(strj)).json()
    print('output--', returned_json)
    if 'Merged' in returned_json.keys():
        if returned_json['Merged'] == 'True':
            print('mjson--', returned_json['Merged'])
            merged_json = mergejson(current, next, headers_xsorted)
            if current <= len(merged_json) - 2:

                # print('current3--',current)

                final_json = combine(current, current + 1, merged_json)
            else:
                return merged_json
        else:

            if next <= len(headers_xsorted) - 2:
                final_json = combine(next, next + 1, headers_xsorted)
            else:
                return headers_xsorted

    return headers_xsorted


def mergejson(current, next, headers_xsorted):
    current_prefix = list(headers_xsorted[current].keys())[0].split('_')[0]
    next_prefix = list(headers_xsorted[next].keys())[0].split('_')[0]

    # print('cpx--',current_prefix)
    # print('nxt--',next_prefix)
    # print('curr--',headers_xsorted[current])
    # print('next--',headers_xsorted[next])
    # merge values of current into next

    if int(headers_xsorted[current][current_prefix + '_x1']) \
        < int(headers_xsorted[next][next_prefix + '_x1']):
        headers_xsorted[next][next_prefix + '_x1'] = \
            headers_xsorted[current][current_prefix + '_x1']

    # headers_xsorted[next][next_prefix+'_word']=headers_xsorted[current][current_prefix+'_word']+"@@@@@"+headers_xsorted[next][next_prefix+'_word']

    headers_xsorted[next][next_prefix + '_word'] = \
        headers_xsorted[current][current_prefix + '_word'] \
        + headers_xsorted[next][next_prefix + '_word']
    del headers_xsorted[current]
    return headers_xsorted


def flattening_API(final_json):
    linenos = []
    maxlno = 0
    for (index, item) in enumerate(final_json):
        prefix = list(final_json[index].keys())[0].split('_')[0]
        val = str(final_json[index][prefix + '_word'])
        result = requests.post(FURL, data=json.dumps({
            'H4': val,
            'H3': '',
            'H2': ' ',
            'H1': ' ',
            }))
        r_json = result.json()

        # print('rjson--',r_json)

        if r_json:
            if 'response' in r_json.keys():

                # print('r---',r_json['response'])

                if 'docs' in r_json['response'].keys():
                    for (ind, im) in enumerate(r_json['response']['docs'
                            ]):
                        if 'url' in im.keys():
                            if im['url']:
                                urlval = im['url'][0]

                                # print('uval--',val,urlval)

                                item.update({'header_URL': urlval,
                                        'header_word': val})
        if prefix + '_line_index' in item.keys():
            if item[prefix + '_line_index'] not in linenos:
                linenos.append(item[prefix + '_line_index'])

    # set max line no. of all elements

    if linenos:
        maxlno = max(linenos)

        # print('maxlno---',maxlno)

    for (index, item) in enumerate(final_json):
        prefix = list(final_json[index].keys())[0].split('_')[0]
        item[prefix + '_line_index'] = maxlno
        print('del----', final_json[index])

        # delete lo key

        del final_json[index][prefix + '_lo']

    return final_json



def template_API(final_json):
    column_json = {}
    for (index, item) in enumerate(final_json):
        if 'data' in item.keys():
            column_json.update({'column' + item['column']: item['data'
                               ]})
    returned_json = requests.post(TEMURL,
                                  data=json.dumps(column_json)).json()

    return final_json


def noheader_API(image_json):
    indexes = 0
    line_nos = []
    if 'table_data' in image_json.keys():
        for (index, item) in enumerate(image_json['table_data']):
            if 'line_number' in item.keys():
                if item['line_number'] not in line_nos:
                    line_nos.append(item['line_number'])
            if 'type' in item.keys():
                if item['type'] == 'Overlap':
                    if 'header_x1' in item.keys():
                        hx1 = item['header_x1']
                    if 'header_x2' in item.keys():
                        hx2 = item['header_x2']
                    if 'header_y' in item.keys():
                        hy = item['header_y']
                    indexes = 0
                    for (ind, itm) in enumerate(image_json['table_data'
                            ]):
                        if 'type' in itm.keys():
                            type = itm['type']
                        if 'header_x1' in itm.keys():
                            thx1 = itm['header_x1']
                        if 'header_x2' in itm.keys():
                            thx2 = itm['header_x2']
                        if 'header_y' in itm.keys():
                            thy = itm['header_y']
                        if thy == hy and thx1 == hx1 and thx2 == hx2 \
                            and type == 'Header no data':
                            if indexes > 0:

                                # delete extra item

                                del image_json['table_data'][ind]
                            indexes += 1

    min_line = 0
    first_line_list = []
    if line_nos:
        min_line = min(line_nos)

        # print('minline---',min_line)

    counter = 1
    for (index, item) in enumerate(image_json['table_data']):
        if 'line_number' in item.keys():
            if item['line_number'] == min_line:
                item.update({'column': str(counter)})
                first_line_list.append(item)
                counter += 1
    temp = {}
    for (index, item) in enumerate(first_line_list):
        flag = -1
        cno = str(item['column'])
        dtype = str(item['data_type'])
        dx1 = str(item['data_x1'])
        dx2 = str(item['data_x2'])
        hx1 = str(item['header_x1'])
        hx2 = str(item['header_x2'])
        data = str(item['data'])
        temp.update({
            'h1Clm No': cno,
            'no Header 1': '',
            'h1Data Type': dtype,
            'h1X1': hx1,
            'h1X2': hx2,
            'h1Y': '',
            'h1LO': '',
            'wClm No': cno,
            'no Data': '',
            'wData Type': dtype,
            'wX1': dx1,
            'wX2': dx2,
            'wY': '',
            'wLO': '',
            'h2Clm No': cno,
            'no Header 2': '',
            'h2Data Type': dtype,
            'h2X1': hx1,
            'h2X2': hx2,
            'h2Y': '',
            'h2LO': '',
            'data in Line': data,
            })
        strj = {
            'Rule_Engine': 'PoMergeR2',
            'project_name': 'PoMergeP2',
            'user_name': 'carrotrule_xyz.com',
            'RawJson': temp,
            }

        # print('strj--',json.dumps(strj))

        returned_json = requests.post(URL, data=json.dumps(strj)).json()

        # print('rj----',returned_json)

        if 'columnoffset' in returned_json.keys():
            returned_json['columnoffset'] == 0
            flag = 0
        elif 'overlap' in returned_json.keys():
            returned_json['overlap'] == 0
            flag = 0

        # merge data....

        if flag == 0:
            x1 = min(item['data_x1'], item['header_x1'])
            x2 = max(item['data_x2'], item['header_x2'])
            dty = item['data_type']
            item.update({'data_x1': x1, 'data_x2': x2,
                        'data': item['data'] + item['header']})

            # print('item---',item)

    return image_json


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5022)

			