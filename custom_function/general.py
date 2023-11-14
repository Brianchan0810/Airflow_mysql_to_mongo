import re
import json
import numpy as np
from rdb import get_row_number
from datetime import datetime
from airflow.models import Variable


class DateTimeDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        ret = {}
        for key, value in obj.items():
            if re.search('(date|datetime|time|contract_start|contract_end|month_year)', key, re.IGNORECASE):
                ret[key] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
            else:
                ret[key] = value
        return ret


def get_required_sub_loop_no(table_name, mysql_connection):
    total_account_no = get_row_number(table_name, mysql_connection)
    customer_per_batch = int(Variable.get('customer_per_batch'))
    main_loop_no = int(Variable.get('main_loop_no'))
    return np.ceil(total_account_no / (main_loop_no * customer_per_batch))


def format_df_to_json(df):
    for column in df.select_dtypes(include=[np.datetime64]).columns.tolist():
        df[column] = df[column].apply(lambda x: datetime.strftime(x, "%Y-%m-%dT%H:%M:%S"))
    return [{**x[i]} for i, x in df.stack().groupby(level=0)]


def resolve_json_data(task_ids, key, decoder, ti):
    pull_data = ti.xcom_pull(task_ids=task_ids, key=key)
    return json.loads(pull_data, cls=decoder)



def pop_key(data, key_name):
    for key, value in data.copy().items():
        if key == key_name:
            data.pop(key)
    return data


def groupby_1key_from_list_of_dict(data, key_name):
    temp_dict = dict()
    for row in data:
        if temp_dict.get(row[key_name]) is None:
            temp_dict[row[key_name]] = list()
        temp_dict.get(row[key_name]).append(pop_key(row, key_name))
    return temp_dict


def groupby_2key_from_list_of_dict(data, key_name1, key_name2):
    temp_dict = {}
    for key, value in groupby_1key_from_list_of_dict(data, key_name1).items():
        temp_dict[key] = groupby_1key_from_list_of_dict(value, key_name2)
    return temp_dict


def list_for_arrayfilter(data, key_name):
    temp_list = []
    for item in data:
        temp_list.append(item[key_name])
    return temp_list


def pick_value_as_key(data, key_name, get_key_list=False):
    new_dict = dict()
    key_list = []

    for item in data:
        new_key = item[key_name]

        new_dict[new_key] = item
        key_list.append(new_key)

    if get_key_list:
        return new_dict, key_list
    else:
        return new_dict


def json_to_1key_groupby_dict(data, decoder, groupby_key):
    data = json.loads(data, cls=decoder)
    return groupby_1key_from_list_of_dict(data, groupby_key)


def json_to_2key_groupby_dict(data, decoder, groupby_key1, groupby_key2):
    data = json.loads(data, cls=decoder)
    return groupby_2key_from_list_of_dict(data, groupby_key1, groupby_key2)


def insert_data_to_document(doc, doc_branch_key, past_info, new_info, new_data_search_key):
    past_data = past_info.get(doc_branch_key, [])
    new_data = new_info.get(new_data_search_key, [])
    overall_data = new_data + past_data
    if len(overall_data) > 0:
        doc[doc_branch_key] = overall_data
    else:
        return doc


def mask_hkid(hkid):
    return hkid[:4] + 'xxx(x)'

