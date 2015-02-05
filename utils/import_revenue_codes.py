# -*- coding: utf-8 -*-

''' Read a TXT, extract revenue codes and descriptions, and insert them in the
DB.

Usage:
    ./import_revenue_code TXT_FILE
    ./import_revenue_code (-h | --help)

Options:
    -h --help   Show this message.
'''
import re

from docopt import docopt
import codecs

from sqlalchemy.sql.expression import insert

from gastosabertos import create_app
from gastosabertos.extensions import db
from gastosabertos.receita.models import RevenueCode

app = create_app()
db.app = app


def format_code(s):
    # return '.'.join([str(int(i)) for i in s.split('.')])
    a, b, c = [str(int(i)) for i in s.split('.')]
    a = '.'.join(a)
    if int(c):
        formated = '.'.join([a, b, c])
    elif int(b):
        formated = '.'.join([a, b])
    else:
        formated = a
    return formated


def get_codes(file_in):
    codes = {}
    with codecs.open(file_in, encoding="utf-8") as txt:
        for line in txt.readlines():
            matched = re.match(
                "(?P<code>\d{2,20}(\.\d\d){2})\s*(?P<descr>(\S+\s{1,3})+)",
                line.strip())
            if matched:
                code = format_code(matched.group("code"))
                if code not in codes:
                    codes[code] = matched.group("descr").strip()
            else:
                print("Rejected line: ", line)
    return codes


def insert_codes(codes):
    rows = [{"code": code, "description": description}
            for code, description in codes.items()]
    ins = insert(RevenueCode.__table__, rows)
    db.session.execute(ins)
    db.session.commit()

if __name__ == '__main__':
    arguments = docopt(__doc__)
    codes = get_codes(arguments['TXT_FILE'])
    insert_codes(codes)
