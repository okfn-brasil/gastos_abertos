# -*- coding: utf-8 -*-

# from sqlalchemy import and_, extract, func
# from datetime import datetime

import json

from flask import Blueprint
# from flask.ext import restful
# from flask.ext.restful import fields
# from flask.ext.restful.utils import cors
# from flask.ext.restful.reqparse import RequestParser
from flask.ext.restplus import Resource
from sqlalchemy import func
# from flask.ext.restplus import Resource, marshal_with, fields
# from sqlalchemy import Integer

from .models import Execucao
from gastosabertos.extensions import db, api

# Blueprint for Execucao
execucao = Blueprint('execucao', __name__,
                     template_folder='templates',
                     static_folder='static',
                     static_url_path='/execucao/static')


ns = api.namespace('execucao', 'Dados sobre execução')


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


@ns.route('/info/<year>')
class ExecucaoInfoMappedApi(Resource):

    def get(self, year):
        '''Information about a year.'''
        # TODO: Passar tudo isso para uma tabela e evitar esse monte de
        # queries?

        q_total = db.session.query(Execucao).filter(
            Execucao.get_year() == year)
        num_total = q_total.count()
        q_mapped = q_total.filter(Execucao.point_found())
        num_mapped = q_mapped.count()

        rows = {
            'total': num_total,
            'mapped': num_mapped,
            # TODO: calcular regionalizados...
            'region': num_mapped,
        }

        values = []
        fields = [
            ('orcado', 'sld_orcado_ano'),
            ('atualizado', 'vl_atualizado'),
            ('empenhado', 'vl_empenhadoliquido'),
            ('liquidado', 'vl_liquidado')
        ]
        for name, db_field in fields:
            q = (db.session.query(
                func.sum(Execucao.data[db_field].cast(db.Float)))
                .filter(Execucao.get_year() == year))

            total = q.scalar()
            mapped = q.filter(Execucao.point_found()).scalar()
            if mapped is None:
                mapped = 0
            values.append({
                'name': name,
                'total': total,
                'mapped': mapped,
                # TODO: calcular regionalizados...
                'region': mapped,
            })

        return {
            'data': {
                'rows': rows,
                'values': values
            }
        }


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

parser = api.parser()
parser.add_argument('code', type=str, help='Code doc!!!')
parser.add_argument('year', type=int, help='Years doc!!!')
parser.add_argument('page', type=int, default=0, help='Page doc!!!')
parser.add_argument('per_page_num', type=int, default=100, help='PPN doc!!!')


@ns.route('/list')
# @api.doc(params={'id': 'An ID'})
class ExecucaoAPI(Resource):

    @api.doc(parser=parser)
    # @marshal_with(execucao_fields, envelope='data')
    def get(self):
        # Extract the argumnets in GET request
        args = parser.parse_args()
        page = args['page']
        per_page_num = args['per_page_num']
        year = args['year']
        code = args['code']

        execucao_data = db.session.query(Execucao.point.ST_AsGeoJSON(3),
                                         Execucao.code,
                                         Execucao.data)

        # Get only row of 'code'
        if code:
            execucao_data = execucao_data.filter(Execucao.code == code)
        # Get all rows of 'year'
        elif year:
            execucao_data = execucao_data.filter(Execucao.get_year() == year)

        headers = {
            # Add 'Access-Control-Expose-Headers' header here is a workaround
            # until Flask-Restful adds support to it.
            'Access-Control-Expose-Headers': 'X-Total-Count',
            'X-Total-Count': execucao_data.count()
        }

        # Limit que number of results per page
        execucao_data = (execucao_data.offset(page*per_page_num)
                         ).limit(per_page_num)

        return {'data': [
            dict({
                'code': i.code,
                'geometry': json.loads(i[0]) if i[0] else None,
            }, **i.data)
            for i in execucao_data.all()
        ]}, 200, headers


# Create the restful API
# execucao_api = restful.Api(execucao, prefix='/api/v1/execucao')
# execucao_api.add_resource(ExecucaoInfoApi, '/info')
