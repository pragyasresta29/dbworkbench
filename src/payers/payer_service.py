from database.connector import connect
from moneytun import moneytun_service
import json

import pandas as pd
from sql import sql_service


def add_payers(msb, msb_id, country_id):

    try:
        conn = connect()
        cur = conn.cursor()
        country = sql_service.fetch_country_by_id(country_id, cur)
        conn.close()
        if msb == 'moneytun':
            payers_info = moneytun_service.get_payers(country['three_char_code'].values[0])
        else:
            print("Insert payers not available for %s" % msb)
            return
        print("Inserting payers for %s.........................................................." %
              country['name'].values[0])
        insert_payers(payers_info, msb_id, country)

    except IndexError as e:
        print("--------------------Country not found--------------------")
        print("--------------------Aborting insert for %s--------------------" % msb)
        return
    except Exception as e:
        print("-------------------%s----------------------" % e.args)


def insert_payers(payers_info, msb_id, country):
    conn = connect()
    cur = conn.cursor()
    payers = pd.DataFrame()
    payers['name'] = pd.Series(payers_info['PayeeName']).str.title()
    payers['country_id'] = country['id'].values[0]
    payers.drop_duplicates(subset=['name']).reset_index(drop=True)
    existing_payers = sql_service.fetch_data('payer', ['name', 'country_id'], cur)
    existing_payers['name'] = pd.Series(existing_payers['name']).str.title()
    print('payer=', len(payers))
    print('existingPayers=', len(existing_payers))
    data_to_insert = sql_service.remove_duplicate_data(existing_payers, payers[['name', 'country_id']])
    print('dataToInsert=', len(data_to_insert))
    sql_service.bulk_insert('payer', data_to_insert.columns.tolist(), data_to_insert.values.tolist(), cur)
    print("Inserting payer metadata for %s.........................................................." %
          country['name'].values[0])
    insert_payer_metadata(payers_info, payers, msb_id, country, cur)
    print("--------------------Insertion Complete--------------------")
    cur.execute('END;')
    cur.close()
    conn.close()
    return


def insert_payer_metadata(payers_info, payers_data, msb_id, country, cur):
    payers = sql_service.fetch_data('payer', ['id', 'name', 'country_id'], cur)
    payers.columns = ['payer_id', 'name', 'country_id']
    existing_data = sql_service.fetch_data('payer_metadata', ['data', 'payer_id', 'company_id'], cur)
    payers_data.drop_duplicates(subset=['name']).reset_index(drop=True)
    payers_info['name'] = pd.Series(payers_info['PayeeName']).str.title()
    payers['name'] = pd.Series(payers['name']).str.title()
    print('existing payers=', len(payers))
    print("payers_data=", len(payers_data))
    print('existingPayerMetadata=', len(existing_data))
    insert_data = pd.DataFrame(prepare_payer_metadata(pd.merge(pd.merge(payers_data, payers), payers_info), country, msb_id))
    print('insertData=', len(insert_data))
    data_to_insert = sql_service.remove_duplicate_data(existing_data, insert_data)
    print('dataToInsert=', len(data_to_insert))
    sql_service.bulk_insert('payer_metadata', data_to_insert.columns.tolist(), data_to_insert.values.tolist(), cur)
    return


def prepare_payer_metadata(payers, country, msb_id):
    metadata_list = []
    for key, data in payers.iterrows():
        payer_metadata = {}
        response = moneytun_service.get_poc_list(country['three_char_code'].values[0], data['PayeeCode'])
        if response['payload']['StatusCode'] is not 0:
            continue
        poc_list = pd.DataFrame(response['payload']['PointOfContactList'])
        poc_data = poc_list[poc_list['DeliveryMethod'] == 'CASH PICKUP'].to_dict('records')
        if len(poc_data) == 0:
            continue
        payer_metadata = {
            'company_id': msb_id,
            'payer_id': data['payer_id'],
            'code': data['PayeeCode'],
            'address': data['AddressLine1'],
            'phone_number': data['Phone1'],
            'receiving_currency': poc_data[0]['ReceipientCurrencyISOCode'],
            'data': json.dumps(poc_data[0]),
        }
        metadata_list.append(payer_metadata)
    return metadata_list


def enable_mto_payer(country_id, company_id):
    print("Enabling payer with country id: %s and for mto: %s" % (country_id, company_id))
    payers = sql_service.fetch_data('payer', ['id', 'country_id'])
    print('======payers==========')
    print(payers)
    payers_metadata = sql_service.fetch_data('payer_metadata', ['payer_id'])
    print(payers_metadata)
    data_to_insert = payers[payers['country_id'] == int(country_id)][['id']]
    data_to_insert.columns = ['payer_id']
    print('data=')
    print(data_to_insert)
    data_to_insert = pd.merge(data_to_insert, payers_metadata)
    print('merger')
    print(data_to_insert)
    data_to_insert['id'] = sql_service.generated_uuid_column(data_to_insert)
    data_to_insert['created_at'] = sql_service.get_current_time()
    data_to_insert['mto_id'] = company_id
    data_to_insert = data_to_insert[['id', 'created_at', 'payer_id', 'mto_id']]
    print('dataToInsert=', data_to_insert[:5])
    sql_service.bulk_insert('mto_payer', data_to_insert.columns.tolist(), data_to_insert.values.tolist())
    print("--------------------Insertion Complete--------------------")
