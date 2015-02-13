# -*- coding: utf-8 -*-

''' Read a CSV and write to another CSV the replicated (date,code) lines.

Usage:
    ./import_revenue [FILE]
    ./import_revenue (-h | --help)

Options:
    -h --help   Show this message.
'''
import csv
from datetime import datetime, timedelta
import calendar
import pandas as pd
from docopt import docopt


def parse_money(money_string):
    if money_string[0] == '-':
        return -float(money_string[3:].replace('.', '').replace(',', '.'))
    else:
        return float(money_string[3:].replace('.', '').replace(',', '.'))


def parse_date(date_string):
    year_month = datetime.strptime(date_string, '%Y-%m')
    date = year_month + timedelta(
        days=calendar.monthrange(
            year_month.year, year_month.month
        )[1] - 1)
    return date


def parse_code(code_string):
    return [int(i) for i in code_string.split('.')]


def analise_all(csv_file='../data/receitas_min.csv', lines_per_insert=100):
    data = pd.read_csv(csv_file, encoding='utf8')

    cache = {}
    repetidos = {}

    for row_i, row in data.iterrows():

        r = {}

        # print(str(int(current_line/total_lines*100))+'%')
        code = row['codigo']
        date = row['data']
        r['original_code'] = row['codigo']
        r['description'] = unicode(row['descricao']).encode('utf8')
        r['date'] = parse_date(row['data'])
        r['monthly_outcome'] = parse_money(row['realizado_mensal'])
        r['monthly_predicted'] = parse_money(row['previsto_mensal'])

        if (date, code) not in cache:
            cache[(date, code)] = r
        else:
            lista = repetidos.get((date, code))
            if not lista:
                lista = [cache[(date, code)]]
            lista.append(r)
            repetidos[(date, code)] = lista

    w = csv.DictWriter(open("repetidos.csv", 'wb'),
                       ['original_code', 'description', 'date',
                        'monthly_predicted', 'monthly_outcome'])
    w.writeheader()
    for k, l in repetidos.items():
        print(k)
        for i in l:
            print(i)
            w.writerow(i)


if __name__ == '__main__':
    arguments = docopt(__doc__)
    args = {}
    csv_file = arguments['FILE']
    if csv_file:
        args['csv_file'] = csv_file
    analise_all(**args)
