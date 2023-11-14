from airflow.models import Variable
from pymongo.encryption import (Algorithm, ClientEncryption)
import uuid
from bson.binary import Binary, UUIDLegacy, UUID_SUBTYPE
from airflow.providers.mongo.hooks.mongo import MongoHook
from general import resolve_json_data, groupby_1key_from_list_of_dict, groupby_2key_from_list_of_dict
#from pymongo import UpdateMany

def initialise_database_connection(mongo_conn_id, database):
    mongo_hook = MongoHook(mongo_conn_id)
    mongo_connection = mongo_hook.get_conn()
    return mongo_connection[database]


def insert_to_layer1(data, main_key, collection):
    for item in data:
        collection.update_one({main_key: item[main_key]}, {"$set": item}, upsert=True)


def insert_to_layer2(data, main_key, new_key, collection):
    for key, value in data.items():
        collection.update_one({main_key: key}, {"$push": {new_key: {'$each': value}}})


def insert_to_layer3(data, main_key, branch_key, sub_key, new_key, collection):
    for key_1st, value in data.items():
        for key_2nd, sub_value in value.items():
            collection.update_one({main_key: key_1st, f'{branch_key}.{sub_key}': key_2nd},
                              {"$push": {f"{branch_key}.$.{new_key}": {'$each': sub_value}}})


def layer1_from_pull_to_insert(task_ids, main_key, decoder, ti, collection, value_key='return_value'):
    data = resolve_json_data(task_ids, value_key, decoder, ti)
    insert_to_layer1(data, main_key, collection)


def layer2_from_pull_to_insert(task_ids, main_key, branch_name, decoder, ti, collection, value_key='return_value'):
    data = resolve_json_data(task_ids, value_key, decoder, ti)
    processed_data = groupby_1key_from_list_of_dict(data, main_key)
    insert_to_layer2(processed_data, main_key, branch_name, collection)


def layer3_from_pull_to_insert(task_ids, main_key, branch_name, sub_key, sub_branch_name, decoder, ti, collection, value_key='return_value'):
    data = resolve_json_data(task_ids, value_key, decoder, ti)
    processed_data = groupby_2key_from_list_of_dict(data, main_key, sub_key)
    insert_to_layer3(processed_data, main_key, branch_name, sub_key, sub_branch_name, collection)


def explicit_encryption_setup(mongo_conn_id):
    mongo_hook = MongoHook(mongo_conn_id)
    mongo_connection = mongo_hook.get_conn()

    coll = mongo_connection['poc']['demo0']

    kms_providers = {"local": {"key": bytes.fromhex(Variable.get('master_key'))}}

    key_vault_namespace = "secret.key_vault"

    data_encryption_key = mongo_connection['secret']['key_vault'].find_one({})['_id']

    return ClientEncryption(kms_providers, key_vault_namespace, mongo_connection, coll.codec_options), Binary(data_encryption_key.bytes, 4)


def explicit_encryption(data, client_encryption, data_encryption_key):
    return client_encryption.encrypt(
        data,
        Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
        key_id=data_encryption_key)







