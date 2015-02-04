''' Read a TXT, extract revenue codes and descriptions, and insert them in the DB.

Usage:
    ./import_revenue_code TXT_FILE
    ./import_revenue_code (-h | --help)

Options:
    -h --help   Show this message.
'''
import re
from docopt import docopt


def clear_zeros(s):
    return '.'.join([str(int(i)) for i in s.split('.')])


def codes_to_dict(file_in):
    codes = {}
    with open(file_in) as txt:
        for line in txt.readlines():
            r = re.match(
                "(?P<code>\d{2,20}(\.\d\d){2})\s*(?P<descr>(\S+\s{1,3})+)",
                line.strip())
            if r:
                code = clear_zeros(r.group("code"))
                codes[code] = r.group("descr").strip()
    print(codes)

if __name__ == '__main__':
    arguments = docopt(__doc__)
    codes_to_dict(arguments['TXT_FILE'])
