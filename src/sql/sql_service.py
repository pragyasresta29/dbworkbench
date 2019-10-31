import pandas as pd
from uuid import uuid4
import datetime as dt
from database.connector import connect

conn = connect()
conn.autocommit = True


def create_select_statement(table, columns):
    joined_columns = ",".join(columns)
    select_sql = """select %s from %s;""" % (joined_columns, table)
    return select_sql


def create_insert_statement(table, columns):
    joined_columns = ",".join(columns)
    values = ','.join(["%s"] * len(columns))
    insert_sql = """insert into %s(%s) values(%s);""" % (table, joined_columns, values)
    return insert_sql


def fetch_data(table, columns, cur=conn.cursor()):
    select_sql = create_select_statement(table, columns)
    cur.execute(select_sql)
    return pd.DataFrame(cur.fetchall(), columns=columns)


def remove_duplicate_data(existing_data, data_to_insert):
    merged_data = data_to_insert.merge(existing_data, how='outer', indicator=True).drop_duplicates().reset_index(drop=True)
    return merged_data[merged_data['_merge'] == 'left_only'].reset_index(drop=True).drop('_merge', axis=1)


def fetch_country_by_id(country_id, cur):
    countries = fetch_data('country', ['id', 'name', 'three_char_code'], cur)
    return countries[countries['id'] == int(country_id)][:1]


def fetch_country_by_code(countries, country_code):
    return countries[countries['three_char_code'] == country_code][:1]


def bulk_insert(table, columns, data, cur=conn.cursor()):
    sql = create_insert_statement(table, columns)
    cur.executemany(sql, data)
    conn.commit()
    return


def generated_uuid_column(df):
    return df.index.to_series().map(lambda x: str(uuid4()))


def get_current_time():
    return dt.datetime.now()
