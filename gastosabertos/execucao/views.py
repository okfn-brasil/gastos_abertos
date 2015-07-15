# -*- coding: utf-8 -*-

# from sqlalchemy import and_, extract, func
# from datetime import datetime

from flask import Blueprint
# from flask.ext import restful
# from flask.ext.restful import fields
# from flask.ext.restful.utils import cors
# from flask.ext.restful.reqparse import RequestParser
from flask.ext.restplus import Resource
# from sqlalchemy import Integer

from .models import Execucao
from gastosabertos.extensions import db, api

# Blueprint for Execucao
execucao = Blueprint('execucao', __name__,
                     template_folder='templates',
                     static_folder='static',
                     static_url_path='/execucao/static')


# Create the restful API
# execucao_api = restful.Api(execucao, prefix="/api/v1/execucao")


ns = api.namespace('execucao', 'Dados sobre execução')


@ns.route('/info')
class ExecucaoInfoApi(Resource):

    def get(self):
        dbyears = db.session.query(
            Execucao.data['cd_anoexecucao']).distinct().all()
        years = sorted([str(i[0]) for i in dbyears])

        return {
            "data": {
                "years": years,
            }
        }


@ns.route('/minlist/<int:year>')
class ExecucaoMinListApi(Resource):

    def get(self, year):
        """Codes and latlons of all geolocated values in a year."""
        items = (
            db.session.query(Execucao.data['code'],
                             Execucao.point.ST_X(),
                             Execucao.point.ST_Y())
            .filter(Execucao.data['cd_anoexecucao'].cast(db.Integer) == year)
            .filter(Execucao.point != None)
            .all())

        return {"data": items}


# execucao_api.add_resource(ExecucaoInfoApi, '/info')
