import http_service
from config import MONEYTUN_CONFIG
import pandas as pd


def get_payers(country_code):
    response = http_service.get_response(prepare_payers_request(country_code))
    if response['status'] == 200:
        return pd.DataFrame(response['payload']['PayerList'])
    else:
        raise Exception("Something went wrong while fetching data from Moneytun")


def get_poc_list(country_code, payee_code):
    response = http_service.get_response(prepare_poc_request(country_code, payee_code))
    if response['status'] == 200:
        return response
    else:
        raise Exception(
            "Something went wrong while fetching poc list from Moneytun for payee [%s] with country [%s]"
            % payee_code, country_code)


def prepare_poc_request(country_code, payee_code):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authentication': get_auth_token()
    }
    data = {
        'CountryISOCode': country_code,
        'Payee': payee_code
    }
    request = {
        'headers': headers,
        'url': "%s/Inbound/API/poc" % MONEYTUN_CONFIG['BASE_URL'],
        'method': 'POST',
        'data': data,
        'params': {}
    }
    return request


def prepare_payers_request(country_code):
    headers = {
        'Content-Type': 'application/json',
        'Authentication': get_auth_token()
    }
    request = {
        'headers': headers,
        'params': get_payers_param(country_code),
        'url': "%s/Inbound/API/payerlist" % MONEYTUN_CONFIG['BASE_URL'],
        'method': 'GET'
    }
    return request


def get_auth_token():
    return "%s:%s" % (MONEYTUN_CONFIG['ACCESS_TOKEN'], MONEYTUN_CONFIG['SECRET_TOKEN'])


def get_payers_param(country_code):
    return {
        'isocode': country_code
    }
