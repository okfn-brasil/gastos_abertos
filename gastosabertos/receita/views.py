# -*- coding: utf-8 -*-

import os
import pandas as pd
from sqlalchemy import and_, extract
from datetime import datetime

from flask import (Blueprint, render_template)
from flask.ext import restful
from flask.ext.restful import fields
from flask.ext.restful.reqparse import RequestParser

from .models import Revenue
from gastosabertos.extensions import db

parser = RequestParser()
parser.add_argument('page', type=int, default=0)
parser.add_argument('per_page_num', type=int, default=100)
parser.add_argument('years', type=int, action='append')

receita = Blueprint('receita', __name__,
                    template_folder='templates',
                    static_folder='static',
                    static_url_path='/receita/static')

receita_api = restful.Api(receita, prefix="/api/v1")

class Date(fields.Raw):
    def format(self, value):
        return str(value)

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
        args = parser.parse_args()
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

receita_api.add_resource(RevenueApi, '/receita/hello')

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
