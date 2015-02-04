''' Read a TXT, extract revenue codes and descriptions, and insert them in the DB.

Usage:
    ./import_revenue_code TXT_FILE
    ./import_revenue_code (-h | --help)

Options:
    -h --help   Show this message.
'''
import re

from docopt import docopt

from sqlalchemy.sql.expression import insert

from gastosabertos import create_app
from gastosabertos.extensions import db
from gastosabertos.receita.models import Revenue

app = create_app()
db.app = app


def clear_zeros(s):
    return '.'.join([str(int(i)) for i in s.split('.')])


def codes_to_dict(file_in):
    with open(file_in) as txt:
        for line in txt.readlines():
            r = re.match(
                "(?P<code>\d{2,20}(\.\d\d){2})\s*(?P<descr>(\S+\s{1,3})+)",
                line.strip())
            if r:
                row = {
                    "code": clear_zeros(r.group("code")),
                    "description": r.group("descr").strip()
                }
                ins = insert(Revenue.__table__, row)
                db.session.execute(ins)
                db.session.commit()

if __name__ == '__main__':
    arguments = docopt(__doc__)
    codes_to_dict(arguments['TXT_FILE'])
