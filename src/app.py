#!/usr/bin/env python
import sys
import argparse

sys.path.insert(0, '../src')

from banks import bank_service
from payers import payer_service
from sql import sql_service


def main():
    parser = argparse.ArgumentParser(description="Program to insert data into database.")
    parser.add_argument("-i", "--insert", nargs=4, type=str, metavar=('table', 'msb', 'msb_id', 'countryId'),
                        help="All the data for specified will be added.")
    parser.add_argument("-s", "--select", nargs='*', type=str, metavar=('table', 'columns'),
                        help="Get data of specified tables and columns")
    parser.add_argument("-l", "--limit", nargs=1, type=int, metavar='limit',
                        help="Limit to get data of specified tables and columns")
    parser.add_argument("-e", "--enable", nargs=3, type=str, metavar=('type', 'countryId', 'companyId'),
                        help="Enable payers/bank of specified country id for mto")

    args = parser.parse_args()

    if args.insert is not None:
        table = args.insert[0]
        if table == 'payer':
            payer_service.add_payers(args.insert[1], args.insert[2], args.insert[3])
        else:
            print("Insertion on table %s is not available" % table)
    elif args.select is not None:
        table = args.select[0]
        columns = args.select[1:]
        if args.limit is not None:
            limit = args.limit[0]
        else:
            limit = 10
        data = sql_service.fetch_data(table, columns)
        print("total data=", len(data))
        print(data[:limit])
    elif args.enable is not None:
        type = args.enable[0]
        country_id = args.enable[1]
        company_id = args.enable[2]
        if type == 'bank':
            bank_service.enable_mto_bank(country_id, company_id)
        elif type == 'payer':
            payer_service.enable_mto_payer(country_id, company_id)
        else:
            print("Type %s is not supported" % type)
    else:
        print("Please enter required arguments(See --help)")
    return


if __name__ == "__main__":
    # calling the main function
    main()
