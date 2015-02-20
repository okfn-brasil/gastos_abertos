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

from sqlalchemy import func, extract
from sqlalchemy.sql.expression import insert

from gastosabertos import create_app
from gastosabertos.extensions import db
from gastosabertos.receita.models import Revenue, RevenueCode

app = create_app()
db.app = app

revenue_levels = {}
revenue_levels[0] = Revenue.economical_category
revenue_levels[1] = Revenue.economical_subcategory
revenue_levels[2] = Revenue.source
revenue_levels[3] = Revenue.rubric
revenue_levels[4] = Revenue.paragraph
revenue_levels[5] = Revenue.subparagraph

db.session.query(RevenueCode.code).all()

# q = db.session.query(Revenue.economical_category, Revenue.economical_subcategory, Revenue.source, Revenue.rubric, Revenue.paragraph, Revenue.subparagraph, func.sum(Revenue.monthly_outcome)) \
# .filter(extract('year', Revenue.date) == 2013) \
# .group_by(Revenue.economical_category, Revenue.economical_subcategory, Revenue.source, Revenue.rubric, Revenue.paragraph, Revenue.subparagraph)

def calculate_year(year):
    codes_values = {}
    for i in range(len(revenue_levels)):
        levels = revenue_levels.values()[0:i+1]
        args = levels + \
            [func.sum(Revenue.monthly_outcome).label('total_outcome')]

        q = db.session.query(*args)\
            .filter(extract('year', Revenue.date) == year) \
            .group_by(*levels)

        for element in q.all():
            code = element[0:-1]
            value = element[-1]
            codes_values[code] = value

    return codes_values


def calculate_all():
    for year in range(2008, 2016):
        year = str(year)
        year_data = calculate_year(year)
        

        # years = args['years']
        # drilldown = args['drilldown']

        # total = {'name': 'Total Outcome',
        #          'data': []}

        # for code in codes:
        #     #try:
        #     #    formated_code = RevenueCode.format_code(code)
        #     #except:
        #     #    formated_code = code

        #     code_levels = code.split('.')
        #     args = [revenue_levels[l] == v for l, v in enumerate(code_levels)]
        #     for year in years:
        #         args += [extract('year', Revenue.date) == year]

        #     levels = [revenue_levels[l] for l in range(len(code_levels) + 1)]
        #     levels += [Revenue.description, func.sum(Revenue.monthly_outcome).label('total_outcome')]

        #     q = db.session.query(*levels)\
        #             .join(RevenueCode)\
        #             .filter(and_(*args))\

        #     if drilldown and drilldown == 'true':
        #         q = q.group_by(revenue_levels[len(code_levels)])
        #     else:
        #         revenue_name_query = db.session.query(RevenueCode.description)\
        #                                 .filter(RevenueCode.code == code).one()
        #         revenue_name = revenue_name_query[0]

        #     q = q.order_by('total_outcome')
        #     revenues_results = q.all()

        #     for r in revenues_results:
        #         if drilldown and drilldown == 'true':
        #             revenue_name = unicode(r[1])
        #             #code = '.'.join([str(c) for c in r[:-2]])
        #             code = '.'.join([str(c) for c in r[:len(code_levels)+1]])

        #             try:
        #                 revenue_name_query = db.session.query(RevenueCode.description)\
        #                                         .filter(RevenueCode.code == code).one()
        #                 revenue_name = revenue_name_query[0]
        #             except:
        #                 revenue_name = r[-2]

        #         total['data'] += [{'code': code,
        #                            'name': revenue_name,
        #                            'value': float(str(r[-1]))
        #                             }]

        # return total

if __name__ == '__main__':
    arguments = docopt(__doc__)
    calculate_all()
