import json
import requests
import pandas as pd


def get_klazify_request(target_url, access_key):
    url = "https://www.klazify.com/api/categorize"

    payload = json.dumps({'url': target_url})
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        'Authorization': f"Bearer {access_key}",
        'cache-control': "no-cache"
    }

    response = requests.request("POST", url, data=payload, headers=headers)
    output = json.loads(response.text)
    print(output)

    return output


def output_to_mongodb(output, database):
    url = output['domain']['full_path_url']

    collection1 = database['klazify_respond_raw']
    collection1.insert_one({'url': url, 'response': output})

    document = dict()
    document['url'] = url
    document['domain'] = '/'.join(url.split('/', 3)[:3])

    category_list = []
    for category in output['domain']['categories']:
        temp_dict = dict()
        temp_dict['confidence'] = category['confidence']
        temp_dict['category'] = category['name'].split('/')[-1]
        category_list.append(temp_dict)

    document['content'] = category_list

    collection2 = database['klazify_respond']
    collection2.insert_one(document)


def klazify_response_to_mongodb(target_url, database, access_key):
    output = get_klazify_request(target_url, access_key)

    if output['success'] == False:
        collection = database['klazify_unsuccessful']
        collection.update_one({'url': target_url}, {'$set': {'url': target_url}}, upsert=True)
        return 'not successful'

    output_to_mongodb(output, database)


def get_interest_score_table(list_of_url, database, access_key):
    collection = database['klazify_respond']

    list_of_respond = []
    for url in set(list_of_url):
        if collection.find_one({'url': url}) is None:
            klazify_response_to_mongodb(url, database, access_key)
        try:
            respond = collection.find_one({'url': url})['content']
            for item in respond:
                item['url'] = url
            list_of_respond += respond
        except:
            pass

    return pd.DataFrame(list_of_respond)


def get_personal_monthly_interest_score(personal_monthly_url_list, interest_score_table):
    df = pd.merge(pd.DataFrame(personal_monthly_url_list, columns=['url']), interest_score_table, how='left',
                  on='url')

    dict_of_interest = df.groupby('category')['confidence'].sum().to_dict()

    list_of_interest = []
    for key, value in dict_of_interest.items():
        list_of_interest.append({'category': key, 'score': value})

    return list_of_interest


def output_restructure(list_of_item, key_name, value_name):
    temp_list = []
    for item in list_of_item:
        temp_list.append({key_name: item[0], value_name: item[1]})
    return temp_list