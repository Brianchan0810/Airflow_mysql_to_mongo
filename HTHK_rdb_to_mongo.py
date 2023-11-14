from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator


def branch():
    main_loop_no, main_loop_counter = int(Variable.get('main_loop_no')), int(Variable.get('main_loop_counter'))

    if main_loop_counter < main_loop_no:
        return 'transit'
    else:
        return 'reset'


def transit():
    print('currently inside the task')

    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    from custom_function.rdb import initialise_db_cursor
    from custom_function.general import get_required_sub_loop_no

    if int(Variable.get('main_loop_counter')) > 0:
        pass
    else:
        mysql_cursor = initialise_db_cursor(schema=Variable.get('database'), mysql_conn_id='mysql_connection')

        sub_loop_no = get_required_sub_loop_no('personal_info', mysql_cursor)

        event_start_date = datetime.strftime((datetime.now().replace(day=1) - relativedelta(months=2)), '%Y-%m-%d')
        event_end_date = datetime.strftime((datetime.now().replace(day=1) + relativedelta(months=1)), '%Y-%m-%d')

        Variable.set('sub_loop_no', sub_loop_no)
        Variable.set('event_start_date', event_start_date)
        Variable.set('event_end_date', event_end_date)


def personal_info():
    print('currently inside the task')

    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    import json
    from airflow.models import Variable
    from custom_function.general import format_df_to_json
    from custom_function.rdb import initialise_db_cursor, fetch_dataset

    mysql_cursor = initialise_db_cursor(schema=Variable.get('database'), mysql_conn_id='mysql_connection')

    main_loop_no, main_loop_counter = int(Variable.get('main_loop_no')), int(Variable.get('main_loop_counter'))
    sub_loop_no = float(Variable.get('sub_loop_no'))

    sub_loop_counter = 0
    list_of_output = []

    while sub_loop_no > sub_loop_counter:
        query = f"""
        SELECT account_no, last_name, first_name, hkid, contact_no, date_of_birth, gender, email, mail_district
               , home_address, work_address, industry
        FROM personal_info
        WHERE RIGHT(account_no, LENGTH(account_no) -2) % {main_loop_no * sub_loop_no} = {main_loop_counter * sub_loop_no + sub_loop_counter};
        """

        info_df = fetch_dataset(query, mysql_cursor)
        info_df.rename(columns={'industry': 'profession'}, inplace=True)
        info_df['hkid_masked'] = info_df['hkid'].apply(lambda x: x[:4] + 'xxx(x)')

        print(info_df.shape)

        list_of_output += format_df_to_json(info_df)

        if sub_loop_counter % 10 == 0:
            print(sub_loop_counter)

        sub_loop_counter += 1

        print(len(list_of_output))

    return json.dumps(list_of_output)


def contract():
    print('currently inside the task')

    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    import json
    from airflow.models import Variable
    from custom_function.general import format_df_to_json
    from custom_function.rdb import initialise_db_cursor, fetch_dataset

    mysql_cursor = initialise_db_cursor(schema=Variable.get('database'), mysql_conn_id='mysql_connection')

    main_loop_no, main_loop_counter = int(Variable.get('main_loop_no')), int(Variable.get('main_loop_counter'))
    sub_loop_no = float(Variable.get('sub_loop_no'))

    sub_loop_counter = 0
    list_of_output = []

    while sub_loop_no > sub_loop_counter:
        query = f"""
        SELECT contract_no, account_no, phone_no, plan_type, plan_id, handset_model, voucher_amount, duration
               , contract_start_date, contract_end_date, monthly_charge, voice_cap, data_cap, data_speed
        FROM contract
        WHERE RIGHT(account_no, LENGTH(account_no) -2) % {main_loop_no * sub_loop_no} = {main_loop_counter * sub_loop_no + sub_loop_counter}
        """

        contract_df = fetch_dataset(query, mysql_cursor)
        contract_df['status'] = 'active'

        list_of_output += format_df_to_json(contract_df)

        if sub_loop_counter % 10 == 0:
            print(sub_loop_counter)

        sub_loop_counter += 1

    return json.dumps(list_of_output)


def billing():
    print('currently inside the task')

    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    import json
    from airflow.models import Variable
    from custom_function.general import format_df_to_json
    from custom_function.rdb import initialise_db_cursor, fetch_dataset

    mysql_cursor = initialise_db_cursor(schema=Variable.get('database'), mysql_conn_id='mysql_connection')

    main_loop_no, main_loop_counter = int(Variable.get('main_loop_no')), int(Variable.get('main_loop_counter'))
    sub_loop_no = float(Variable.get('sub_loop_no'))

    sub_loop_counter = 0
    list_of_output = []

    while sub_loop_no > sub_loop_counter:
        query = f"""
        SELECT billing_date, data_usage, voice_usage, basic_charge, charge_extra, billing.contract_no
        , contract.account_no 
        FROM billing
        LEFT JOIN contract
        ON billing.contract_no = contract.contract_no
        WHERE RIGHT(account_no, LENGTH(account_no) -2) % {main_loop_no * sub_loop_no} = {main_loop_counter * sub_loop_no + sub_loop_counter}
        AND billing_date >= '{Variable.get('event_start_date')}' AND Date(billing_date) < '{Variable.get('event_end_date')}'
        """

        billing_df = fetch_dataset(query, mysql_cursor)

        list_of_output += format_df_to_json(billing_df)

        if sub_loop_counter % 10 == 0:
            print(sub_loop_counter)

        sub_loop_counter += 1

    return json.dumps(list_of_output)


def disconnection():
    print('currently inside the task')

    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    import json
    from airflow.models import Variable
    from custom_function.general import format_df_to_json
    from custom_function.rdb import initialise_db_cursor, fetch_dataset

    mysql_cursor = initialise_db_cursor(schema=Variable.get('database'), mysql_conn_id='mysql_connection')

    main_loop_no, main_loop_counter = int(Variable.get('main_loop_no')), int(Variable.get('main_loop_counter'))
    sub_loop_no = float(Variable.get('sub_loop_no'))

    sub_loop_counter = 0
    list_of_output = []

    while sub_loop_no > sub_loop_counter:
        query = f"""
        SELECT discon_id, disconnect_date, reason, disconnection.contract_no, contract.account_no 
        FROM disconnection
        LEFT JOIN contract
        ON disconnection.contract_no = contract.contract_no
        WHERE RIGHT(account_no, LENGTH(account_no) -2) % {main_loop_no * sub_loop_no} = {main_loop_counter * sub_loop_no + sub_loop_counter}
        AND disconnect_date >= '{Variable.get('event_start_date')}' AND Date(disconnect_date) < '{Variable.get('event_end_date')}'
        """

        discon_df = fetch_dataset(query, mysql_cursor)

        list_of_output += format_df_to_json(discon_df)

        if sub_loop_counter % 10 == 0:
            print(sub_loop_counter)

        sub_loop_counter += 1

    return json.dumps(list_of_output)


def easy_call():
    print('currently inside the task')

    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    import json
    from airflow.models import Variable
    from custom_function.general import format_df_to_json
    from custom_function.rdb import initialise_db_cursor, fetch_dataset

    mysql_cursor = initialise_db_cursor(schema=Variable.get('database'), mysql_conn_id='mysql_connection')

    main_loop_no, main_loop_counter = int(Variable.get('main_loop_no')), int(Variable.get('main_loop_counter'))
    sub_loop_no = float(Variable.get('sub_loop_no'))

    sub_loop_counter = 0
    list_of_output = []

    while sub_loop_no > sub_loop_counter:
        query = f"""
        SELECT call_date, nature, upsale_offer_id, respond, easy_call.contract_no, contract.account_no 
        FROM easy_call
        LEFT JOIN contract
        ON easy_call.contract_no = contract.contract_no
        WHERE RIGHT(account_no, LENGTH(account_no) -2) % {main_loop_no * sub_loop_no} = {main_loop_counter * sub_loop_no + sub_loop_counter}
        AND call_date >= '{Variable.get('event_start_date')}' AND Date(call_date) < '{Variable.get('event_end_date')}'
        """

        easy_call_df = fetch_dataset(query, mysql_cursor)

        list_of_output += format_df_to_json(easy_call_df)

        if sub_loop_counter % 10 == 0:
            print(sub_loop_counter)

        sub_loop_counter += 1

    return json.dumps(list_of_output)


def complain():
    print('currently inside the task')

    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    import json
    from airflow.models import Variable
    from custom_function.general import format_df_to_json
    from custom_function.rdb import initialise_db_cursor, fetch_dataset

    mysql_cursor = initialise_db_cursor(schema=Variable.get('database'), mysql_conn_id='mysql_connection')

    main_loop_no, main_loop_counter = int(Variable.get('main_loop_no')), int(Variable.get('main_loop_counter'))
    sub_loop_no = float(Variable.get('sub_loop_no'))

    sub_loop_counter = 0
    list_of_output = []

    while sub_loop_no > sub_loop_counter:
        query = f"""
        SELECT case_id, record_date, complain_category, customer_attitude, call_result, complain.contract_no
               , contract.account_no, contract.phone_no 
        FROM complain
        LEFT JOIN contract
        ON complain.contract_no = contract.contract_no
        WHERE RIGHT(account_no, LENGTH(account_no) -2) % {main_loop_no * sub_loop_no} = {main_loop_counter * sub_loop_no + sub_loop_counter}
        AND record_date >= '{Variable.get('event_start_date')}' AND Date(record_date) < '{Variable.get('event_end_date')}'
        """

        complain_df = fetch_dataset(query, mysql_cursor)

        list_of_output += format_df_to_json(complain_df)

        if sub_loop_counter % 10 == 0:
            print(sub_loop_counter)

        sub_loop_counter += 1

    return json.dumps(list_of_output)


def web_browsing():
    print('currently inside the task')

    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    import json
    from datetime import datetime
    from collections import Counter
    from airflow.models import Variable
    from custom_function.general import format_df_to_json
    from custom_function.rdb import initialise_db_cursor, fetch_dataset
    from custom_function.mdb import initialise_database_connection
    from custom_function.klazify import get_interest_score_table, get_personal_monthly_interest_score, output_restructure

    mysql_cursor = initialise_db_cursor(schema=Variable.get('database'), mysql_conn_id='mysql_connection')

    db = initialise_database_connection("mongo_atlas_connection", 'HTHK_test')

    main_loop_no, main_loop_counter = int(Variable.get('main_loop_no')), int(Variable.get('main_loop_counter'))
    sub_loop_no = float(Variable.get('sub_loop_no'))

    sub_loop_counter = 0
    list_of_output = []

    while sub_loop_no > sub_loop_counter:
        query = f"""
        SELECT start_visit_datetime, end_visit_datetime, url, web_browsing.contract_no, contract.account_no
        FROM web_browsing
        LEFT JOIN contract
        ON web_browsing.contract_no = contract.contract_no
        WHERE RIGHT(account_no, LENGTH(account_no) -2) % {main_loop_no * sub_loop_no} = {main_loop_counter * sub_loop_no + sub_loop_counter}
        AND start_visit_datetime >= '{Variable.get('event_start_date')}' AND Date(start_visit_datetime) < '{Variable.get('event_end_date')}'
        """

        web_df = fetch_dataset(query, mysql_cursor)
        web_df['year_month'] = web_df['start_visit_datetime'].apply(lambda x: datetime(x.year, x.month, 1))

        list_of_url = web_df['url'].to_list()
        access_key = Variable.get("klazify_access_key")
        score_table = get_interest_score_table(list_of_url, db, access_key)

        web_grpby = web_df.groupby(['account_no', 'year_month'])['url'].apply(list)
        web_grpby = web_grpby.reset_index()
        web_grpby['monthly_interest_score'] = web_grpby['url'] \
            .apply(lambda x: get_personal_monthly_interest_score(x, score_table))
        web_grpby['monthly_top5_visit'] = web_grpby['url'].apply(lambda x: Counter(x).most_common(5))
        web_grpby['monthly_top5_visit'] = web_grpby['monthly_top5_visit'].apply(lambda x: output_restructure(x, 'url', 'count'))
        web_grpby.drop(columns=['url'], inplace=True)

        list_of_output += format_df_to_json(web_grpby)

        if sub_loop_counter % 10 == 0:
            print(sub_loop_counter)

        sub_loop_counter += 1

    return json.dumps(list_of_output)


def load(info_data, contract_data, billing_data, disconnect_data, easy_call_data, complain_data, web_data):
    import sys
    sys.path.insert(0, '/opt/airflow/dags/custom_function')

    import json
    from pymongo import ReplaceOne
    from airflow.models import Variable
    from custom_function.general import DateTimeDecoder, pick_value_as_key, json_to_1key_groupby_dict, json_to_2key_groupby_dict, insert_data_to_document
    from custom_function.mdb import initialise_database_connection, explicit_encryption_setup, explicit_encryption

    print(1)



    print('A')
    info_data = json.loads(info_data, cls=DateTimeDecoder)
    info_data_in_dict, account_no_list = pick_value_as_key(info_data, 'account_no', get_key_list=True)
    print('B')
    contract_data_in_dict = json_to_1key_groupby_dict(contract_data, DateTimeDecoder, 'account_no')
    print('C')
    billing_data_in_dict = json_to_2key_groupby_dict(billing_data, DateTimeDecoder, 'account_no', 'contract_no')
    print('D')
    disconnect_data_in_dict = json_to_2key_groupby_dict(disconnect_data, DateTimeDecoder, 'account_no', 'contract_no')
    print('E')
    easy_call_data_in_dict = json_to_2key_groupby_dict(easy_call_data, DateTimeDecoder, 'account_no', 'contract_no')
    print('F')
    complain_data_in_dict = json_to_1key_groupby_dict(complain_data, DateTimeDecoder, 'account_no')
    print('G')
    web_data_in_dict = json_to_1key_groupby_dict(web_data, DateTimeDecoder, 'account_no')
    print('H')

    mongo_encryption, encryption_key = explicit_encryption_setup("mongo_vm_connection")
    coll = initialise_database_connection("mongo_vm_connection", 'scale_test')[Variable.get('database')]
    print(2)

    past_doc_from_mongo = coll.find({'account_no': {'$in': account_no_list}})
    past_doc_from_mongo_in_dict = pick_value_as_key(past_doc_from_mongo, 'account_no')
    print(3)

    list_for_replace_one = []
    for i, account in enumerate(account_no_list):
        past_account_data = past_doc_from_mongo_in_dict.get(account, {})

        past_contract_in_dict = pick_value_as_key(past_account_data.get('contract', {}), 'contract_no')

        new_account = info_data_in_dict[account]
        new_account['hkid_encrypted'] = explicit_encryption(new_account['hkid'], mongo_encryption, encryption_key)
        new_account.pop('hkid')

        new_contract_data_in_dict, contract_list = pick_value_as_key(contract_data_in_dict.get(account,{}), 'contract_no', get_key_list=True)

        processed_contract = []
        for contract in contract_list:

            new_contract = new_contract_data_in_dict[contract]

            insert_data_to_document(new_contract, 'billing', past_contract_in_dict.get(contract, {})
                                    , billing_data_in_dict.get(account, {}), contract)

            insert_data_to_document(new_contract, 'disconnection', past_contract_in_dict.get(contract, {})
                                    , disconnect_data_in_dict.get(account, {}), contract)

            if len(new_contract.get('disconnection', [])) > 0:
                new_contract['status'] = 'terminated'

            insert_data_to_document(new_contract, 'easy_call', past_contract_in_dict.get(contract, {})
                                    , easy_call_data_in_dict.get(account, {}), contract)

            processed_contract.append(new_contract)

        new_account['contract'] = processed_contract

        insert_data_to_document(new_account, 'complain', past_account_data, complain_data_in_dict, account)

        insert_data_to_document(new_account, 'interest', past_account_data, web_data_in_dict, account)

        coll.replace_one({'account_no': account}, new_account, upsert=True)

        list_for_replace_one.append(ReplaceOne({'account_no': account}, new_account, upsert=True))

        if i % 1000 == 0:
            print(i)

    print(4)
    coll.bulk_write(list_for_replace_one)
    print(5)

    Variable.set('main_loop_counter', int(Variable.get('main_loop_counter')) + 1)


def reset():
    Variable.set('main_loop_counter', 0)


with DAG(
    'HTHK_rdb_to_mongo',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        #'on_success_callback': cleanup_xcom,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='',
    schedule_interval=None,
    start_date=datetime(2022, 2, 22),
    catchup=False,
    tags=['POC'],
) as dag:

    branch = BranchPythonOperator(task_id='branch', python_callable=branch)

    transit = PythonOperator(task_id='transit', python_callable=transit)

    personal_info = PythonVirtualenvOperator(
        task_id='personal_info',
        python_callable=personal_info,
        requirements=['apache-airflow', 'pandas'],
        do_xcom_push=True
    )

    contract = PythonVirtualenvOperator(
        task_id='contract',
        python_callable=contract,
        requirements=['python-dateutil', 'apache-airflow', 'pandas'],
        do_xcom_push=True
    )

    billing = PythonVirtualenvOperator(
        task_id='billing',
        python_callable=billing,
        requirements=['python-dateutil', 'apache-airflow', 'pandas'],
        do_xcom_push=True
    )

    disconnection = PythonVirtualenvOperator(
        task_id='disconnection',
        python_callable=disconnection,
        requirements=['python-dateutil', 'apache-airflow', 'pandas'],
        do_xcom_push=True
    )

    easy_call = PythonVirtualenvOperator(
        task_id='easy_call',
        python_callable=easy_call,
        requirements=['python-dateutil', 'apache-airflow', 'pandas'],
        do_xcom_push=True
    )

    complain = PythonVirtualenvOperator(
        task_id='complain',
        python_callable=complain,
        requirements=['python-dateutil', 'apache-airflow', 'pandas'],
        do_xcom_push=True
    )

    web_browsing = PythonVirtualenvOperator(
        task_id='web_browsing',
        python_callable=web_browsing,
        requirements=['python-dateutil', 'apache-airflow', 'pandas', 'requests'],
        do_xcom_push=True
    )

    load = PythonVirtualenvOperator(
        task_id='load',
        python_callable=load,
        requirements=['python-dateutil', 'apache-airflow', 'pandas', 'pymongo[encryption]', 'pymongo'],
        op_kwargs={
            "info_data": "{{ti.xcom_pull(task_ids='personal_info', key='return_value')}}",
            "contract_data": "{{ ti.xcom_pull(task_ids='contract', key='return_value') }}",
            "billing_data": "{{ ti.xcom_pull(task_ids='billing', key='return_value') }}",
            "disconnect_data": "{{ ti.xcom_pull(task_ids='disconnection', key='return_value') }}",
            "easy_call_data": "{{ ti.xcom_pull(task_ids='easy_call', key='return_value') }}",
            "complain_data": "{{ ti.xcom_pull(task_ids='complain', key='return_value') }}",
            "web_data": "{{ ti.xcom_pull(task_ids='web_browsing', key='return_value') }}"
        }
    )

    reset = PythonOperator(task_id='reset', python_callable=reset)

    delete_xcom = PostgresOperator(
        task_id='delete_xcom',
        postgres_conn_id='postgre_connection',
        sql="delete from xcom where dag_id= '" + dag.dag_id + "'"
    )

    trigger_next = TriggerDagRunOperator(task_id="re_trigger_dag", trigger_dag_id="HTHK_rdb_to_mongo")

    branch >> transit
    branch >> reset

    transit >> [personal_info, contract, billing, disconnection, easy_call, complain, web_browsing] >> load >> delete_xcom >> trigger_next
