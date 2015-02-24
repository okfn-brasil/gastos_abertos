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

from sqlalchemy import func, extract, and_
from sqlalchemy.orm.exc import NoResultFound

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


def get_description(code, parent_description):
    """Searches for the description of the code.
    If can't find, returns parent_description."""
    code_str = '.'.join([str(i) for i in code if i])
    try:
        description = db.session.query(RevenueCode.description)\
            .filter(RevenueCode.code == code_str).one()
    except NoResultFound:
        if parent_description:
            description = parent_description
        else:
            raise
    return description


def create_treemap(code_levels, year, parent_name):
    """Calculate a tree below a code"""
    treemap = {}
    element_name = get_description(code_levels, parent_name)
    treemap['name'] = element_name
    treemap['children'] = []

    args = [revenue_levels[l] == v for l, v in enumerate(code_levels)]
    args += [extract('year', Revenue.date) == year]

    levels = [revenue_levels[l] for l in range(len(code_levels)+1)]
    levels += [func.sum(Revenue.monthly_outcome)]

    children = db.session.query(*levels)\
        .filter(and_(*args))\
        .group_by(revenue_levels[len(code_levels)])\
        .all()

    for child in children:
        child_value = float(child[-1])
        # Ignore 0 paths
        if child_value != 0:
            child_code_levels = child[:len(code_levels)+1]

            is_leaf = len(child_code_levels) == 6 or\
                (child_code_levels[-1] is None)

            if is_leaf:
                child_name = get_description(child_code_levels, element_name)
                leaf = [{'name': child_name, 'value': child_value}]
                treemap['children'].append(leaf)
            else:
                # recursive drilldown
                child_tree = create_treemap(child_code_levels,
                                            year, element_name)
                treemap['children'].append(child_tree)

    return treemap


def calculate_year(year):
    """Calculate the tree for a year"""

    # get highest level numbers (ie.: 1, 2 and 9)
    highest_level = revenue_levels[0]
    highest_level_numbers = db.session.query(highest_level)\
        .filter(extract('year', Revenue.date) == year)\
        .group_by(highest_level).all()

    treemap = {}
    treemap['name'] = "Revenue %s" % year
    treemap['children'] = []

    for code in highest_level_numbers:
        treemap['children'].append(create_treemap(code, year, None))

    return treemap


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
