# -*- coding: utf-8 -*-

''' Read a CSV with revenues and insert them in the DB.

Usage:
    ./import_revenue [FILE] [LINES_PER_INSERT]
    ./import_revenue (-h | --help)

Options:
    -h --help   Show this message.
'''

from datetime import datetime, timedelta
import calendar

from sqlalchemy.sql.expression import insert
import pandas as pd
from docopt import docopt

from gastosabertos.receita.models import Revenue, RevenueCode
from utils import ProgressCounter, get_db


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


def insert_rows(db, rows_data):
    ins = insert(Revenue.__table__, rows_data)
    db.session.execute(ins)
    db.session.commit()


def insert_all(db, csv_file='../data/receitas_min.csv', lines_per_insert=100):
    print("Importing Revenues from: " + csv_file)
    data = pd.read_csv(csv_file, encoding='utf8')

    cache = {}
    to_insert = []
    counter = ProgressCounter(len(data))

    for row_i, row in data.iterrows():

        r = {}

        if len(to_insert) == lines_per_insert:
            insert_rows(db, to_insert)
            to_insert = []
            # Progress counter
            counter.update(lines_per_insert)

        r['original_code'] = row['codigo']
        r['description'] = row['descricao']
        r['date'] = parse_date(row['data'])
        r['monthly_outcome'] = parse_money(row['realizado_mensal'])
        r['monthly_predicted'] = parse_money(row['previsto_mensal'])
        code_parsed = parse_code(row['codigo'])
        r['economical_category'] = code_parsed[0]

        # Insert code reference
        code_parts = map(int, r['original_code'].split('.'))
        len_cp = len(code_parts)

        for i in range(len_cp):
            code = '.'.join(map(str, code_parts[:len_cp - i]))
            if code not in cache:
                code_result = db.session.query(RevenueCode.id).filter(
                    RevenueCode.code == code).all()
                if code_result:
                    cache[code] = code_result[0][0]
                    r['code_id'] = code_result[0][0]
                    break
            else:
                r['code_id'] = cache[code]
                break
        else:
            r['code_id'] = None

        if len(code_parsed) >= 2:
            r['economical_subcategory'] = code_parsed[1]
        else:
            r['economical_subcategory'] = None

        if len(code_parsed) >= 3:
            r['source'] = code_parsed[2]
        else:
            r['source'] = None

        if len(code_parsed) >= 4:
            r['rubric'] = code_parsed[3]
        else:
            r['rubric'] = None

        if len(code_parsed) >= 5:
            r['paragraph'] = code_parsed[4]
        else:
            r['paragraph'] = None

        if len(code_parsed) == 6:
            r['subparagraph'] = code_parsed[5]
        else:
            r['subparagraph'] = None

        to_insert.append(r)

    if len(to_insert) > 0:
        insert_rows(db, to_insert)

    counter.end()


if __name__ == '__main__':
    arguments = docopt(__doc__)
    args = {}
    csv_file = arguments['FILE']
    if csv_file:
        args['csv_file'] = csv_file
    lines_per_insert = arguments['LINES_PER_INSERT']
    if lines_per_insert:
        args['lines_per_insert'] = int(lines_per_insert)
    db = get_db()
    insert_all(db, **args)
