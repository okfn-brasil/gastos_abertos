# -*- coding: utf-8 -*-

import os
import pandas as pd
from sqlalchemy import and_, extract, func
from datetime import datetime

from flask import (Blueprint, render_template)
from flask.ext import restful
from flask.ext.restful import fields
from flask.ext.restful.reqparse import RequestParser

from .models import Revenue, RevenueCode
from gastosabertos.extensions import db

# Blueprint for Receita
receita = Blueprint('receita', __name__,
                    template_folder='templates',
                    static_folder='static',
                    static_url_path='/receita/static')

# Create the restful API
receita_api = restful.Api(receita, prefix="/api/v1")

class Date(fields.Raw):
    def format(self, value):
        return str(value)

# Parser for RevenueAPI arguments
revenue_list_parser = RequestParser()
revenue_list_parser.add_argument('page', type=int, default=0)
revenue_list_parser.add_argument('per_page_num', type=int, default=100)
revenue_list_parser.add_argument('years', type=int, action='append')

# Fields for RevenueAPI data marshal
revenue_fields = { 'id': fields.Integer()
                 , 'date': Date
                 , 'description': fields.String()
                 , 'code': fields.String()
                 , 'monthly_predicted': fields.Float()
                 , 'monthly_outcome': fields.Float() }

class RevenueApi(restful.Resource):

    @restful.marshal_with(revenue_fields)
    def get(self):
        # Extract the argumnets in GET request
        args = revenue_list_parser.parse_args()
        page = args['page']
        per_page_num = args['per_page_num']
        years = args['years']

        # Create the query
        revenue_data = db.session.query(Revenue)

        if years:
            # Filter years
            if len(years) == 2:
                year_min = "{}-1-1".format(years[0])
                year_max = "{}-12-31".format(years[1])
                revenue_data = revenue_data.filter(and_(Revenue.date >= datetime.strptime(year_min, '%Y-%m-%d')),
                                                        Revenue.date <= datetime.strptime(year_max, '%Y-%m-%d'))
            elif len(years) == 1:
                year = str(years[0])
                revenue_data = revenue_data.filter(extract('year', Revenue.date) == year)

        # Limit que number of results per page
        revenue_data = revenue_data.offset(page*per_page_num)\
                            .limit(per_page_num)\

        return revenue_data.all()

# Revenues levels Dict for GroupedRevenueApi
revenue_levels = {}
revenue_levels[0] = Revenue.economical_category
revenue_levels[1] = Revenue.economical_subcategory
revenue_levels[2] = Revenue.source
revenue_levels[3] = Revenue.rubric
revenue_levels[4] = Revenue.paragraph
revenue_levels[5] = Revenue.subparagraph

# Parser for GroupedRevenueApi
grouped_revenue_parser = RequestParser()
grouped_revenue_parser.add_argument('levels', type=int, action='append')
grouped_revenue_parser.add_argument('years', type=int, action='append')

# Fields for GroupedRevenueApi data marshal
grouped_fields = { 'category_code': fields.String
                 , 'Total Predicted': fields.Float()
                 , 'Total Outcome': fields.Float() }

class GroupedRevenueApi(restful.Resource):

    def get(self):
        args = grouped_revenue_parser.parse_args()
        years = args['years']
        levels = args['levels']

        # Create the query
        levels_columns = [ revenue_levels[l] for l in levels ]
        # Sum Outcome and predicted values
        complete_query = levels_columns + [ func.sum(Revenue.monthly_predicted).label('Total predicted'),
                                            func.sum(Revenue.monthly_outcome).label('Total outcome') ]

        # Create the query
        revenue_query_base = db.session.query(*complete_query)

        if not years:
            years = ['all']

        revenue_grouped = {}
        for year in years:
            year = str(year)
            if year != 'all':
                revenue_data = revenue_query_base.filter(extract('year', Revenue.date) == year)\
                                           .group_by(*levels_columns)
            else:
                revenue_data = revenue_query_base.group_by(*levels_columns)

            revenue_grouped[year] = [{'category_code': rev[:len(levels)],
                                      'total_predicted': str(rev[len(levels)]),
                                      'total_outcome': str(rev[len(levels) + 1])} for rev in revenue_data.all()]

        return revenue_grouped


# Parser for RevenueCodeAPI arguments
revenue_code_parser = RequestParser()
# type of argument defaults to unicode in python2 and str in python3
revenue_code_parser.add_argument('code', action='append')

# Fields for RevenueCodeAPI data marshal
# revenue_code_fields = {'code': fields.String()}
revenue_code_fields = {'description': fields.String()}


class RevenueCodeApi(restful.Resource):

    @restful.marshal_with(revenue_code_fields)
    def get(self):
        # Extract the argumnets in GET request
        args = revenue_code_parser.parse_args()
        code = args['code']

        # Create the query
        descriptions = db.session.query(RevenueCode)
        descriptions = descriptions.filter(RevenueCode.code == code)
        return descriptions.all()


receita_api.add_resource(RevenueApi, '/receita/list')
receita_api.add_resource(GroupedRevenueApi, '/receita/grouped')
receita_api.add_resource(RevenueCodeApi, '/receita/code')

# csv_receita = os.path.join(receita.root_path, 'static', 'receita-2008-01.csv')
# df_receita = pd.read_csv(csv_receita, encoding='utf8')

years = range(2008, 2015)

def get_year_data(year):
    # TODO correct real year, an not month 01
    csv_receita = os.path.join(
        receita.root_path,
        'static',
        'receita-%s-01.csv' %
        year)
    return pd.read_csv(csv_receita, encoding='utf8').iterrows()


@receita.route('/receita/<int:year>')
@receita.route('/receita/<int:year>/<int:page>')
def receita_table(year, page=0):
    receita_data = get_year_data(year)
    return render_template(
        'fulltable.html',
        receita_data=receita_data,
        years=years)


@receita.route('/sankey/<path:filename>')
def sankey(filename):
    return send_from_directory('sankey', filename)
