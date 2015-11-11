# -*- coding: utf-8 -*-

# from sqlalchemy import and_, extract, func
# from datetime import datetime

from __future__ import unicode_literals  # unicode by default
import json

from flask import Blueprint
# from flask.ext import restful
# from flask.ext.restful import fields
# from flask.ext.restful.utils import cors
# from flask.ext.restful.reqparse import RequestParser
from flask.ext.restplus import Resource
from sqlalchemy import desc
# from flask.ext.restplus import Resource, marshal_with, fields
# from sqlalchemy import Integer

from .models import Execucao, History, ExecucaoYearInfo
from gastosabertos.extensions import db, api

# Blueprint for Execucao
execucao = Blueprint('execucao', __name__,
                     template_folder='templates',
                     static_folder='static',
                     static_url_path='/execucao/static')


ns = api.namespace('api/v1/execucao', 'Dados sobre execução')

arguments = {
    'code': {
        'type': str,
        'help': 'Code.',
    },
    'codes': {
        'type': list,
        'location': 'json',
        'default': None,
        'help': 'List of codes.',
    },
    'year': {
        'type': int,
        'help': 'Year.',
    },
    'page': {
        'type': int,
        'default': 0,
        'help': 'Page.',
    },
    'per_page_num': {
        'type': int,
        'default': 100,
        'help': 'Number of elements per page.',
    },
    'has_key': {
        'type': str,
        'help': 'Field that must have been modified.',
    },
}

def create_parser(*args):
    '''Create a parser for the passed arguments.'''
    parser = api.parser()
    for arg in args:
        parser.add_argument(arg, **arguments[arg])
    return parser

general_parser = create_parser(*arguments)

@ns.route('/info')
class ExecucaoInfoApi(Resource):

    def get(self):
        '''Information about all the database (currently only years).'''
        dbyears = db.session.query(Execucao.get_year()).distinct().all()
        years = sorted([str(i[0]) for i in dbyears])

        return {
            'data': {
                'years': years,
            }
        }


@ns.route('/info/<int:year>')
class ExecucaoInfoMappedApi(Resource):

    def get(self, year):
        '''Information about a year.'''
        return db.session.query(ExecucaoYearInfo).get(year).data


@ns.route('/minlist/<int:year>')
class ExecucaoMinListApi(Resource):

    parser = api.parser()
    parser.add_argument('state', type=bool, help='State or not state')
    parser.add_argument('capcor', type=bool, help='Capital or Corrente')

    def get(self, year):
        '''Basic information about all geolocated values in a year.
        This endpoint is usefull to plot all the points in a map and use the
        codes to get more information about specific points. Using parameters
        it is possible to get more information about all the points. Only codes
        and latlons are returned by default.'''

        args = self.parser.parse_args()
        return_state = args['state']
        return_cap_cor = args['capcor']

        fields = filter(lambda i: i is not None, [
            Execucao.code,
            Execucao.point.ST_AsGeoJSON(3),
            Execucao.state if return_state else None,
            Execucao.cap_cor if return_cap_cor else None,
        ])

        items = (
            db.session.query(*fields)
            .filter(Execucao.get_year() == year)
            .filter(Execucao.point_found())
            .all())

        # import IPython; IPython.embed()

        return {
            'FeatureColletion': [
                {'type': 'Feature',
                 'properties': dict(filter(lambda i: i[1], (
                     # Add required properties
                     ('uid', v.code),
                     ('state', v.state if return_state else None),
                     ('cap_cor', v.cap_cor if return_cap_cor else None),
                 ))),
                 'geometry': json.loads(v[1])}
                for v in items
            ]
        }


# execucao_fields = api.model('Todo', {
    # 'sld_orcado_ano': fields.Float(required=True,
    #                                description='The task details')
# })


@ns.route('/list')
# @api.doc(params={'id': 'An ID'})
class ExecucaoAPI(Resource):

    @api.doc(parser=create_parser('code', 'year', 'page', 'per_page_num'))
    def get(self):
        '''List execução data in pages.'''
        args = general_parser.parse_args()
        code = args['code']
        page = args['page']
        per_page_num = args['per_page_num']
        year = args['year']

        execucao_data = query_execucao()

        # Get only row of 'code'
        if code:
            execucao_data = execucao_data.filter(Execucao.code == code)
        # Get all rows of 'year'
        elif year:
            execucao_data = execucao_data.filter(Execucao.get_year() == year)

        total = execucao_data.count()

        # Limit que number of results per page
        execucao_data = (execucao_data.offset(page*per_page_num)
                         ).limit(per_page_num)

        return data2json(execucao_data.all()), 200, headers_with_counter(total)

    @api.doc(parser=create_parser('codes'))
    def post(self):
        '''Return information about a given list of codes.'''
        args = general_parser.parse_args()
        codes = args['codes']
        print('UHU', codes)
        if codes:
            execucao_data = (query_execucao()
                             .filter(Execucao.code.in_(codes))
                             .all())
        else:
            execucao_data = []
        return data2json(execucao_data)



@ns.route('/updates')
class ExecucaoUpdates(Resource):

    @api.doc(parser=create_parser('page', 'per_page_num', 'has_key'))
    def get(self):
        '''Rows updates.'''
        args = general_parser.parse_args()
        page = args['page']
        per_page_num = args['per_page_num']
        has_key = args['has_key']

        fields = (History, Execucao.data['ds_projeto_atividade'])
        updates_data = (db.session.query(*fields)
                        .order_by(desc(History.date))
                        .filter(Execucao.code == History.code))

        if has_key:
            updates_data = updates_data.filter(History.data.has_key(has_key))

        total = updates_data.count()

        # Limit que number of results per page
        updates_data = (updates_data.offset(page*per_page_num)
                        ).limit(per_page_num)

        return {
            'data': [{
                'date': hist.date.strftime('%Y-%m-%d'),
                'event': hist.event,
                'code': hist.code,
                'description': descr,
                'data': hist.data
            } for hist, descr in updates_data.all()]
        }, 200, headers_with_counter(total)


def headers_with_counter(total):
    return {
        # Add 'Access-Control-Expose-Headers' header here is a workaround
        # until Flask-Restful adds support to it.
        'Access-Control-Expose-Headers': 'X-Total-Count',
        'X-Total-Count': total
    }


def query_execucao():
    return db.session.query(
        Execucao.point.ST_AsGeoJSON(3),
        Execucao.code,
        Execucao.data)


def data2json(rows):
    return {'data': [
        dict({
            'code': i.code,
            'geometry': json.loads(i[0]) if i[0] else None,
        }, **i.data)
        for i in rows
    ]}
