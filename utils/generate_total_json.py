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
# from sqlalchemy.orm.exc import NoResultFound

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


def get_code_str(code):
    return '.'.join([str(i) for i in code if i])


def get_description(code, parent_description):
    """Searches for the description of the code.
    If can't find, returns parent_description."""
    code_str = get_code_str(code)
    founds = db.session.query(RevenueCode.description)\
               .filter(RevenueCode.code == code_str).first()
    if founds:
        description = founds[0]
    else:
        if parent_description:
            description = parent_description
            # print(code, description)
        else:
            description = "ERRO: SEM DESCRIÇÃO"
            print("Descrição não encontra e código é superior!:",
                  code, parent_description)
    return description


# def create_child_map(child_code_levels, child_value, element_name):
#     child_name = get_description(child_code_levels, element_name)
#     child_map = {
#         'name': child_name,
#         'y': child_value,
#         'drilldown': get_code_str(child_code_levels)
#     }
#     return child_map


def create_treemap(code_levels, value, year, parent_name):
    """Calculate a tree below a code"""
    element_map = {}
    element_name = get_description(code_levels, parent_name)
    element_map['name'] = element_name
    element_map['code'] = get_code_str(code_levels)
    # element_map['y'] = value
    element_map['children'] = []

    args = [revenue_levels[l] == v for l, v in enumerate(code_levels)]
    args += [extract('year', Revenue.date) == year]

    levels = [revenue_levels[l] for l in range(len(code_levels)+1)]
    levels += [func.sum(Revenue.monthly_outcome)]

    children = db.session.query(*levels)\
        .filter(and_(*args))\
        .group_by(revenue_levels[len(code_levels)])\
        .all()

    children_map_list = []
    for child in children:
        child_value = float(child[-1])
        # Ignore 0 paths
        if child_value != 0:
            child_code_levels = child[:len(code_levels)+1]
            # child_map = create_child_map(child_code_levels,
            #                              child_value,
            #                              element_name)
            child_name = get_description(child_code_levels, element_name)
            child_map = {
                'name': child_name,
                'code': get_code_str(child_code_levels),
                'data': [child_value],
                # 'drilldown': get_code_str(child_code_levels)
            }
            element_map['children'].append(child_map)

            is_leaf = len(child_code_levels) == 6 or\
                (child_code_levels[-1] is None)

            if is_leaf:
                children_map_list.append(child_map)
            else:
                # recursive drilldown
                child_map_list = create_treemap(
                    child_code_levels,
                    child_value,
                    year,
                    element_name)
                children_map_list += child_map_list

    return [element_map] + children_map_list


def calculate_year(year):
    """Calculate the tree for a year"""

    # get highest level numbers (ie.: 1, 2 and 9)
    highest_level = revenue_levels[0]
    levels = [highest_level, func.sum(Revenue.monthly_outcome)]
    highest_level_numbers = db.session.query(*levels)\
        .filter(extract('year', Revenue.date) == year)\
        .group_by(highest_level).all()

    element_map = {}
    element_map['name'] = "Receita %s" % year
    element_map['code'] = 'BASE'
    element_map['children'] = []

    children_map_list = []
    for code, value in highest_level_numbers:
        code = (code,)
        value = float(value)
        child_map_list = create_treemap(code, value, year, None)
        children_map_list += child_map_list

        child_name = get_description(code, None)
        child_map = {
            'name': child_name,
            'code': get_code_str(code),
            'data': [value],
            # 'drilldown': get_code_str(code)
        }
        element_map['children'].append(child_map)

    family = {}
    for element in [element_map] + children_map_list:
        try:
            family[element['code']] = element
        except:
            pass
    return family


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
