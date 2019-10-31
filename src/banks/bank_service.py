from uuid import uuid4
from database.connector import connect
import datetime as dt
import json

import pandas as pd
from sql import sql_service


def prepare_bank_meta_data(banks, country_code):
    metadata_list = []
    for key, data in banks.iterrows():
        banks_metadata = {}
        metadata = {'BankName': str(data['name']).encode('utf-8', 'ignore').decode('utf-8', 'ignore'),
                    'BankCode': str(data['bank_code']).encode('utf-8', 'ignore').decode('utf-8', 'ignore'),
                    'PayeeCode': 'TRG', 'SubPayeeAgencyName': 'Agency Name', 'pocID': '',
                    'DestinationCountryISOCode': str(country_code).encode('utf-8', 'ignore').decode('utf-8', 'ignore')}
        banks_metadata['data'] = json.dumps(metadata)
        banks_metadata['bank_id'] = data['bank_id']
        banks_metadata['company_id'] = data['company_id']
        metadata_list.append(banks_metadata)
    return metadata_list


def prepare_bank_branch(branch_info):
    bank_branches = pd.DataFrame()
    bank_branches['name'] = branch_info['name']
    bank_branches['bank_id'] = branch_info['bank_id']
    bank_branches['payment_type_code'] = 'D'
    bank_branches = bank_branches.fillna('Not Available')
    return bank_branches


def prepare_branch_metadata(branch_info):
    metadata_list = []
    columns = branch_info.columns.tolist()
    branch_info = branch_info.fillna('Not Available')
    for key, data in branch_info.iterrows():
        banks_metadata = {}
        metadata = {'BranchName': data['name']}
        if 'branch_code' in columns:
            metadata['BranchCode'] = str(data['branch_code']).encode('utf-8', 'ignore').decode('utf-8', 'ignore')
        if 'address' in columns:
            metadata['Address'] = data['address'].encode('utf-8', 'ignore').decode('utf-8', 'ignore')
        if 'city' in columns:
            metadata['City'] = data['city'].encode('utf-8', 'ignore').decode('utf-8', 'ignore')
        if 'state' in columns:
            metadata['State'] = data['state'].encode('utf-8', 'ignore').decode('utf-8', 'ignore')
        if 'district' in columns:
            metadata['District'] = data['district'].encode('utf-8', 'ignore').decode('utf-8', 'ignore')
        if 'routing_no' in columns:
            metadata['RoutingNumber'] = str(data['routing_no']).encode('utf-8', 'ignore').decode('utf-8', 'ignore')
        banks_metadata['data'] = json.dumps(metadata)
        banks_metadata['branch_id'] = data['branch_id']
        banks_metadata['company_id'] = 7
        metadata_list.append(banks_metadata)
    print("MetaDatalist=", len(metadata_list))
    return metadata_list


def insert_bank_metadata(bank_data, country_code, cur):
    banks = sql_service.fetch_data('bank', ['id', 'name', 'country_id'], cur)
    banks.columns = ['bank_id', 'name', 'country_id']
    existing_data = sql_service.fetch_data('bank_metadata', ['data', 'bank_id', 'company_id'], cur)
    bank_data['name'] = pd.Series(bank_data['name']).str.title()
    bank_data.drop_duplicates(subset=['name']).reset_index(drop=True)
    print('existing banks=', len(banks))
    print("bankData=", len(bank_data))
    print('existingBankMetadata=', len(existing_data))
    insert_data = pd.DataFrame(prepare_bank_meta_data(pd.merge(bank_data, banks), country_code))
    print('insertData=', len(insert_data))
    data_to_insert = sql_service.remove_duplicate_data(existing_data, insert_data)
    print('dataToInsert=', len(data_to_insert))
    sql_service.bulk_insert('bank_metadata', data_to_insert.columns.tolist(), data_to_insert.values.tolist(), cur)
    return


def enable_mto_bank(country_id, company_id):
    print("Enabling bank with country id: %s and for mto: %s" % (country_id, company_id))
    banks = sql_service.fetch_data('bank', ['id', 'country_id'])
    data_to_insert = banks[banks['country_id'] == country_id][['id']]
    data_to_insert.columns = ['bank_id']
    data_to_insert['id'] = data_to_insert.index.to_series().map(lambda x: str(uuid4()))
    data_to_insert['created_at'] = dt.datetime.now()
    data_to_insert['company_id'] = company_id
    data_to_insert = data_to_insert[['id', 'created_at', 'bank_id', 'company_id']]
    print('dataToInsert=', data_to_insert[:5])
    sql_service.bulk_insert('mto_bank', data_to_insert.columns.tolist(), data_to_insert.values.tolist())
    print("--------------------Insertion Complete--------------------")


def insert_banks(sheets):
    conn = connect()
    cur = conn.cursor()
    countries = sql_service.fetch_data('country', ['id', 'name', 'three_char_code'], cur)
    for countryCode in sheets.keys():
        try:
            country = sql_service.fetch_country_by_code(countries, countryCode)
        except IndexError as e:
            print("--------------------Country not found--------------------")
            print("--------------------Aborting insert for %s--------------------" % countryCode)
            continue
        print(
            "Inserting banks for %s.........................................................." % country['name'].values[
                0])
        banks = pd.DataFrame(sheets[countryCode])
        banks['country_id'] = country['id'].values[0]
        banks['name'] = pd.Series(banks['name']).str.title()
        banks.drop_duplicates(subset=['name']).reset_index(drop=True)
        existing_banks = sql_service.fetch_data('bank', ['name', 'country_id'], cur)
        print('banks=', len(banks))
        print('existingBanks=', len(existing_banks))
        data_to_insert = sql_service.remove_duplicate_data(existing_banks, banks[['name', 'country_id']])
        print('dataToInsert=', len(data_to_insert))
        sql_service.bulk_insert('bank', data_to_insert.columns.tolist(), data_to_insert.values.tolist(), cur)
        print("Inserting bank metadata for %s.........................................................." %
              country['name'].values[0])
        insert_bank_metadata(banks, countryCode, cur)
        print("--------------------Insertion Complete--------------------")
    cur.execute('END;')
    cur.close()
    conn.close()
    return


def insert_branch_metadata(branch_info, cur):
    branches = sql_service.fetch_data('bank_branch', ['id', 'name', 'bank_id'], cur)
    branches.columns = ['branch_id', 'name', 'bank_id']
    existing_data = sql_service.fetch_data('branch_metadata', ['data', 'branch_id', 'company_id'], cur)
    print("existingData=", len(existing_data))
    print("branchInfo=", len(branch_info))
    print("branches=", len(branches))
    print("afterMerge=", len(pd.merge(branch_info, branches)))
    print("anotherMerge=", len(branches.merge(branch_info)))
    insert_data = pd.DataFrame(prepare_branch_metadata(pd.merge(branch_info, branches)))
    print("insertData=", len(insert_data))
    data_to_insert = sql_service.remove_duplicate_data(existing_data, insert_data)
    print("dataToInsert=", len(data_to_insert))
    sql_service.bulk_insert('branch_metadata', data_to_insert.columns.tolist(), data_to_insert.values.tolist(), cur)
    return


def insert_bank_branch(sheets):
    conn = connect()
    cur = conn.cursor()
    banks = sql_service.fetch_data('bank', ['id', 'name', 'country_id'], cur)
    banks.columns = ['bank_id', 'bank_name', 'country_id']
    countries = sql_service.fetch_data('country', ['id', 'name', 'three_char_code'], cur)
    for countryCode in sheets.keys():
        try:
            country = sql_service.fetch_country_by_code(countries, countryCode)
        except IndexError as e:
            print("--------------------Country not found--------------------")
            print("--------------------Aborting insert for %s--------------------" % countryCode)
            continue
        print(
            "Inserting banks for %s.........................................................." % country['name'].values[
                0])
        branch_info = pd.DataFrame(sheets[countryCode])
        branch_info['name'] = pd.Series(branch_info['name']).str.title()
        branch_info['bank_name'] = pd.Series(branch_info['bank_name']).str.title()
        branch_info.drop_duplicates(subset=['name']).reset_index(drop=True)
        branch_info['country_id'] = country['id'].values[0]
        print("branchInfo=", len(branch_info))
        print(branch_info[:2], banks[:2])
        branch_info = banks.merge(branch_info)
        print("branchInfo=", len(branch_info))
        insert_data = prepare_bank_branch(branch_info)
        existing_data = sql_service.fetch_data('bank_branch', ['name', 'bank_id', 'payment_type_code'], cur)
        data_to_insert = sql_service.remove_duplicate_data(existing_data, insert_data)
        print("insertData=", len(insert_data))
        print('existingData=', len(existing_data))
        print('dataToInsert=', len(data_to_insert))
        print("Inserting branch metadata for %s.........................................................." %
              country['name'].values[0])
        sql_service.bulk_insert('bank_branch', data_to_insert.columns.tolist(), data_to_insert.values.tolist(), cur)
        insert_branch_metadata(branch_info, cur)
        print("--------------------Insertion Complete--------------------")
    cur.execute('END;')
    cur.close()
    conn.close()
    return
