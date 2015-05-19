# -*- coding: utf-8 -*-

import os
import json
from sqlalchemy import and_, extract, func
from datetime import datetime
from jinja2 import TemplateNotFound

from flask import Blueprint, render_template, send_from_directory, abort
from flask.ext import restful
from flask.ext.restful import fields
from flask.ext.restful.reqparse import RequestParser

from .models import Contrato
from gastosabertos.extensions import db

# Blueprint for Contrato
contratos = Blueprint('contratos', __name__,
                    template_folder='templates',
                    static_folder='static',
                    static_url_path='/contrato/static')


# Create the restful API
contratos_api = restful.Api(contratos, prefix="/api/v1")
# receita_api.decorators = [cors.crossdomain(origin='*')]

# class Date(fields.Raw):
#     def format(self, value):
#         return str(value)

# Parser for RevenueAPI arguments
contratos_list_parser = RequestParser()
contratos_list_parser.add_argument('cnpj')
contratos_list_parser.add_argument('orgao')
contratos_list_parser.add_argument('modalidade')
contratos_list_parser.add_argument('evento')
contratos_list_parser.add_argument('objeto')
contratos_list_parser.add_argument('processo_administrativo')
contratos_list_parser.add_argument('nome_fornecedor')
contratos_list_parser.add_argument('licitacao')
contratos_list_parser.add_argument('page', type=int, default=0)
contratos_list_parser.add_argument('per_page_num', type=int, default=100)

# Fields for ContratoAPI data marshal
contratos_fields = { 'id': fields.Integer()
                   , 'orgao': fields.String()
                   , 'data_assinatura': fields.DateTime(dt_format='iso8601')
                   , 'vigencia': fields.Integer()
                   , 'objeto': fields.String()
                   , 'modalidade': fields.String()
                   , 'evento': fields.String()
                   , 'processo_administrativo': fields.String()
                   , 'cnpj': fields.String()
                   , 'nome_fornecedor': fields.String()
                   , 'valor': fields.Float()
                   , 'licitacao': fields.String()
                   , 'data_publicacao': fields.DateTime(dt_format='iso8601') }

class ContratoApi(restful.Resource):

    @restful.marshal_with(contratos_fields)
    def get(self):
        # Extract the arguments in GET request
        args = contratos_list_parser.parse_args()
        page = args['page']
        per_page_num = args['per_page_num']
        cnpj = args['cnpj']
        nome_fornecedor = args['nome_fornecedor']
        orgao = args['orgao']
        modalidade = args['modalidade']
        evento = args['evento']
        objeto = args['objeto']
        processo_administrativo = args['processo_administrativo']
        licitacao = args['licitacao']
  
        contratos_data = db.session.query(Contrato)

        if cnpj:
            contratos_data = contratos_data.filter(Contrato.cnpj == cnpj) 

        if nome_fornecedor:
            nome_query = '%{}%'.format(nome_fornecedor)
            contratos_data = contratos_data.filter(Contrato.nome_fornecedor.ilike(nome_query)) 

        if orgao:
            orgao_query = '%{}%'.format(orgao)
            contratos_data = contratos_data.filter(Contrato.orgao.ilike(orgao_query)) 

        if modalidade:
            modalidade_query = '%{}%'.format(modalidade)
            contratos_data = contratos_data.filter(Contrato.modalidade.ilike(modalidade_query)) 

        if evento:
            evento_query = '%{}%'.format(evento)
            contratos_data = contratos_data.filter(Contrato.evento.ilike(evento_query)) 

        if objeto:
            objeto_query = '%{}%'.format(objeto)
            contratos_data = contratos_data.filter(Contrato.objeto.ilike(objeto_query)) 

        if processo_administrativo:
            processo_administrativo_query = '%{}%'.format(processo_administrativo)
            contratos_data = contratos_data.filter(Contrato.processo_administrativo.ilike(processo_administrativo_query)) 

        if licitacao:
            licitacao_query = '%{}%'.format(licitacao)
            contratos_data = contratos_data.filter(Contrato.licitacao.ilike(licitacao_query)) 

        headers = {
            # Add 'Access-Control-Expose-Headers' header here is a workaround
            # until Flask-Restful adds support to it.
            'Access-Control-Expose-Headers': 'X-Total-Count',
            'X-Total-Count': contratos_data.count()
        }

        # Limit que number of results per page
        contratos_data = contratos_data.offset(page*per_page_num).limit(per_page_num)

        return contratos_data.all(), 200, headers

contratos_api.add_resource(ContratoApi, '/contrato/list')

@contratos.route('/contrato/<contract_id>')
def show_contract(contract_id):
    try:
        contrato = db.session.query(Contrato).filter(Contrato.numero == contract_id).one()

        return render_template('contrato.html', contrato=contrato)
    except TemplateNotFound:
        abort(404)


@contratos.route('/contrato/cnpj/<cnpj>')
def contracts_for_cnpj(cnpj):
    cnpj = "{}.{}.{}/{}-{}".format( cnpj[0:2], cnpj[2:5], cnpj[5:8], cnpj[8:12], cnpj[12:14])
    print cnpj
    try:
        contratos = db.session.query(Contrato).filter(Contrato.cnpj == cnpj).all()

        return render_template('contratos-cnpj.html', contratos=contratos)
    except TemplateNotFound:
        abort(404)

