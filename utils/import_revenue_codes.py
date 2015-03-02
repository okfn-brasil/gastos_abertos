# -*- coding: utf-8 -*-

''' Read a TXT, extract revenue codes and descriptions, and insert them in the
DB.

Usage:
    ./import_revenue_code [TXT_FILE]
    ./import_revenue_code (-h | --help)

Options:
    -h --help   Show this message.
'''
import re

from docopt import docopt
import codecs

from sqlalchemy.sql.expression import insert

from gastosabertos import create_app
from gastosabertos.receita.models import RevenueCode


TXT = '../gastos_abertos_dados/doc/Codificacao_de_Receitas_2013.txt'


def get_db():
    from gastosabertos.extensions import db
    app = create_app()
    db.app = app
    return db


def get_codes(file_in):
    codes = {}
    with codecs.open(file_in, encoding="utf-8") as txt:
        for line in txt.readlines():
            matched = re.match(
                "(?P<code>\d{2,20}(\.\d\d){2})\s*(?P<descr>(\S+\s{1,3})+)",
                line.strip())
            if matched:
                code = RevenueCode.format_code(matched.group("code"))
                if code not in codes:
                    codes[code] = matched.group("descr").strip()
            # else:
            #     print("Rejected line: ", line)
    return codes


def insert_codes(db, codes):
    rows = [{"code": code, "description": description}
            for code, description in codes.items()]
    ins = insert(RevenueCode.__table__, rows)
    db.session.execute(ins)
    db.session.commit()


def import_codes(db, txt_file=TXT):
    print("Importing Revenue Codes from: " + txt_file)
    codes = get_codes(txt_file)
    insert_codes(db, codes)
    print("Done.")


if __name__ == '__main__':
    arguments = docopt(__doc__)
    txt_file = arguments['TXT_FILE']
    db = get_db()
    if txt_file:
        import_codes(db, txt_file)
    else:
        import_codes(db)
