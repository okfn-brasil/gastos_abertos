# -*- coding: utf-8 -*-

''' Generate a JSON by year with totalizations for each code.

Usage: generate_total_json [options] [<year>...]

Options:
-h, --help                        Show this message.
-o, --outfolder <outfolder>       Folder where to place files.
                                  Maybe relative.
                                  [default: current folder]
'''
import os
import json

from docopt import docopt

from sqlalchemy import func, extract

from gastosabertos import create_app
from gastosabertos.extensions import db
from gastosabertos.receita.models import Revenue

app = create_app()
db.app = app

revenue_levels = {}
revenue_levels[0] = Revenue.economical_category
revenue_levels[1] = Revenue.economical_subcategory
revenue_levels[2] = Revenue.source
revenue_levels[3] = Revenue.rubric
revenue_levels[4] = Revenue.paragraph
revenue_levels[5] = Revenue.subparagraph


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
            code_str = '.'.join([str(i) for i in code if i])
            value = element[-1]
            codes_values[code_str] = float(value)

    return codes_values


def calculate_all(outfolder, years):
    # if years is an empty list, calculate for all years in the DB
    if not years:
        ext = extract('year', Revenue.date)
        dbyears = db.session.query(ext).group_by(ext).all()
        # years = range(2008, 2016)
        years = [str(i[0]) for i in dbyears]
    for year in years:
        # year = str(year)
        filepath = os.path.join(outfolder, year + ".json")
        with open(filepath, 'w') as outfile:
            year_data = calculate_year(year)
            json.dump(year_data, outfile)


if __name__ == '__main__':
    arguments = docopt(__doc__)
    outfolder = arguments['--outfolder']
    if outfolder == "current folder":
        outfolder = os.getcwd()
    calculate_all(outfolder, arguments['<year>'])
