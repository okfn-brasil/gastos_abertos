# -*- coding: utf-8 -*-

from sqlalchemy import func, desc
from jinja2 import TemplateNotFound

from flask import Blueprint, render_template, abort, request
from flask.ext.paginate import Pagination
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
contratos_list_parser.add_argument('group_by', default='')
contratos_list_parser.add_argument('order_by', 'id')
contratos_list_parser.add_argument('page', type=int, default=0)
contratos_list_parser.add_argument('per_page_num', type=int, default=100)

contratos_search_parser = RequestParser()
contratos_search_parser.add_argument('query')

# Fields for ContratoAPI data marshal
contratos_fields = {'id': fields.Integer(),
                    'orgao': fields.String(),
                    'data_assinatura': fields.DateTime(dt_format='iso8601'),
                    'vigencia': fields.Integer(),
                    'objeto': fields.String(),
                    'modalidade': fields.String(),
                    'evento': fields.String(),
                    'processo_administrativo': fields.String(),
                    'cnpj': fields.String(),
                    'nome_fornecedor': fields.String(),
                    'valor': fields.Float(),
                    'licitacao': fields.String(),
                    'data_publicacao': fields.DateTime(dt_format='iso8601'),
                    }


class ContratoApi(restful.Resource):

    def filter(self, contratos_data):
        # Extract the arguments in GET request
        args = contratos_list_parser.parse_args()
        cnpj = args['cnpj']
        nome_fornecedor = args['nome_fornecedor']
        orgao = args['orgao']
        modalidade = args['modalidade']
        evento = args['evento']
        objeto = args['objeto']
        processo_administrativo = args['processo_administrativo']
        licitacao = args['licitacao']

        if cnpj:
            contratos_data = contratos_data.filter(Contrato.cnpj == cnpj)

        if nome_fornecedor:
            nome_query = '%{}%'.format(nome_fornecedor)
            contratos_data = contratos_data.filter(
                Contrato.nome_fornecedor.ilike(nome_query))

        if orgao:
            orgao_query = '%{}%'.format(orgao)
            contratos_data = contratos_data.filter(
                Contrato.orgao.ilike(orgao_query))

        if modalidade:
            modalidade_query = '%{}%'.format(modalidade)
            contratos_data = contratos_data.filter(
                Contrato.modalidade.ilike(modalidade_query))

        if evento:
            evento_query = '%{}%'.format(evento)
            contratos_data = contratos_data.filter(
                Contrato.evento.ilike(evento_query))

        if objeto:
            objeto_query = '%{}%'.format(objeto)
            contratos_data = contratos_data.filter(
                Contrato.objeto.ilike(objeto_query))

        if processo_administrativo:
            processo_administrativo_query = '%{}%'.format(
                rocesso_administrativo)
            contratos_data = contratos_data.filter(
                Contrato.processo_administrativo.ilike(
                    processo_administrativo_query))

        if licitacao:
            licitacao_query = '%{}%'.format(licitacao)
            contratos_data = contratos_data.filter(
                Contrato.licitacao.ilike(licitacao_query))

        return contratos_data

    def order(self, contratos_data):
        args = contratos_list_parser.parse_args()
        order_by = args['order_by'].split(',')

        if order_by:
            order_by_args = []
            for field_name in order_by:
                desc_ = False
                if field_name.startswith('-'):
                    field_name = field_name[1:]
                    desc_ = True
                if field_name in contratos_fields or field_name == 'count':
                    order_by_arg = field_name
                    if desc_:
                        order_by_arg = desc(order_by_arg)
                    order_by_args.append(order_by_arg)
            contratos_data = contratos_data.order_by(*order_by_args)

        return contratos_data

    def paginate(self, contratos_data):
        args = contratos_list_parser.parse_args()
        page = args['page']
        per_page_num = args['per_page_num']

        # Limit que number of results per page
        contratos_data = contratos_data.offset(
            page*per_page_num).limit(per_page_num)

        return contratos_data


class ContratoListApi(ContratoApi):

    @restful.marshal_with(contratos_fields)
    def get(self):
        contratos_data = Contrato.filter()

        contratos_data = self.order(contratos_data)
        contratos_data = self.filter(contratos_data)

        total_count = contratos_data.count()

        contratos_data = self.paginate(contratos_data)

        headers = {
            # Add 'Access-Control-Expose-Headers' header here is a workaround
            # until Flask-Restful adds support to it.
            'Access-Control-Expose-Headers': 'X-Total-Count',
            'X-Total-Count': total_count
        }

        return contratos_data.all(), 200, headers

contratos_api.add_resource(ContratoListApi, '/contrato/list')


class ContratoSearchApi(ContratoApi):

    @restful.marshal_with(contratos_fields)
    def get(self):
        args = contratos_search_parser.parse_args()
        query = args['query']

        contratos_data = Contrato.search(query)

        #contratos_data = self.order(contratos_data)
        #contratos_data = self.filter(contratos_data)

        total_count = contratos_data.count()

        contratos_data = self.paginate(contratos_data)
        print contratos_data

        headers = {
            # Add 'Access-Control-Expose-Headers' header here is a workaround
            # until Flask-Restful adds support to it.
            'Access-Control-Expose-Headers': 'X-Total-Count',
            'X-Total-Count': total_count
        }

        return contratos_data.all(), 200, headers

contratos_api.add_resource(ContratoSearchApi, '/contrato/search')


class ContratoAggregateApi(ContratoApi):

    def get(self):
        args = contratos_list_parser.parse_args()
        group_by = args['group_by'].split(',')
        group_by_fields = []

        # Always return a count
        query_args = [func.count(Contrato.id).label('count')]
        keys = []
        temporary_keys = []
        partial_fields = []
        # Tuples with SQLAlchemy function and args to get parts of values.
        # This allows to group by years or months for example.
        parts = {
            'year': (
                lambda field: [func.extract('year', field)],
                lambda values: list(values)[0]
                ),
            'month': (
                lambda field: [
                    func.extract('year', field),
                    func.extract('month', field),
                    ],
                lambda values: '-'.join([format(v, '02') for v in values])
                ),
            'day': (
                lambda field: [
                    func.extract('year', field),
                    func.extract('month', field),
                    func.extract('day', field),
                    ],
                lambda values: '-'.join([format(v, '02') for v in values])
                ),
        }

        for field_name in group_by:
            part = None
            if field_name.endswith(
                    tuple(map(lambda a: '__{}'.format(a), parts.keys()))):
                # User asked to group using only part of value.
                # Get the original field name and which part we should use.
                # "?group_by=data_publicacao__year" results in
                # field_name = 'data_publicacao'
                # part = 'year'
                field_name, part = field_name.split('__', 1)
            if field_name in contratos_fields:
                group_by_field = [getattr(Contrato, field_name)]
                if part:
                    # Apply the "part" function
                    group_by_field = parts[part][0](group_by_field[0])
                    temporary_keys.extend(['{}__{}'.format(field_name, i)
                                           for i in range(len(group_by_field))
                                           ])
                    partial_fields.append({
                        'field_name': field_name,
                        'count': len(group_by_field),
                        'part_name': part,
                    })
                else:
                    keys.append(field_name)
                    temporary_keys.append(field_name)
                group_by_fields.extend(group_by_field)
                query_args.extend(group_by_field)

        query_args.append(func.sum(Contrato.valor).label('valor'))
        keys.append('valor')
        temporary_keys.append('valor')

        contratos_data = db.session.query(*query_args)
        if group_by_fields:
            contratos_data = contratos_data.group_by(*group_by_fields)

        contratos_data = self.order(contratos_data)
        contratos_data = self.filter(contratos_data)

        total_count = contratos_data.count()

        contratos_data = self.paginate(contratos_data)

        headers = {
            # Add 'Access-Control-Expose-Headers' header here is a workaround
            # until Flask-Restful adds support to it.
            'Access-Control-Expose-Headers': 'X-Total-Count',
            'X-Total-Count': total_count
        }

        # Create the dictionary used to marshal
        fields_ = {'count': fields.Integer()}
        fields_.update({key: contratos_fields.get(key, fields.String())
                        for key in keys})

        # Create a list of dictionaries
        result = map(lambda a: dict(zip(['count'] + temporary_keys, a)),
                     contratos_data.all())

        # Set partial dates type to string
        for f in partial_fields:
            fields_[f['field_name']] = fields.String()
            for item in result:
                item[f['field_name']] = parts[f['part_name']][1](
                    (item.pop('{}__{}'.format(f['field_name'], i))
                     for i in range(f['count'])))

        return restful.marshal(result, fields_), 200, headers

contratos_api.add_resource(ContratoAggregateApi, '/contrato/aggregate')


@contratos.route('/contrato/<contract_id>')
def show_contract(contract_id):
    try:
        contrato = db.session.query(Contrato).filter(
            Contrato.numero == contract_id).one()

        return render_template('contrato.html', contrato=contrato)
    except TemplateNotFound:
        abort(404)


@contratos.route('/contrato/cnpj/<cnpj>')
def contracts_for_cnpj(cnpj):
    cnpj = "{}.{}.{}/{}-{}".format(cnpj[0:2],
                                   cnpj[2:5],
                                   cnpj[5:8],
                                   cnpj[8:12],
                                   cnpj[12:14])

    page = int(request.args.get('page', 1))
    per_page_num = 10

    try:
        contratos_query = db.session.query(Contrato).filter(
            Contrato.cnpj == cnpj)
        contratos_ = contratos_query.offset(
            (page-1)*per_page_num).limit(per_page_num).all()
        count = contratos_query.count()
        pagination = Pagination(page=page, per_page=per_page_num, total=count,
                                found=count, bs_version=3, search=True,
                                record_name='contratos')

        return render_template('contratos-cnpj.html',
                               contratos=contratos_,
                               pagination=pagination,
                               count=count,
                               filter_info=u"Fornecedor",
                               filter_value=cnpj)

    except TemplateNotFound:
        abort(404)


@contratos.route('/contrato/orgao/<orgao>')
def contracts_for_orgao(orgao):
    page = int(request.args.get('page', 1))
    per_page_num = 10

    try:
        contratos_query = db.session.query(Contrato).filter(
            Contrato.orgao == orgao)
        contratos_ = contratos_query.offset(
            (page-1)*per_page_num).limit(per_page_num).all()
        count = contratos_query.count()
        pagination = Pagination(page=page, per_page=per_page_num, total=count,
                                found=count, bs_version=3, search=True,
                                record_name='contratos')

        return render_template('contratos-orgao.html',
                               contratos=contratos_,
                               pagination=pagination,
                               count=count,
                               filter_info=u"Orgão",
                               filter_value=orgao)
    except TemplateNotFound:
        abort(404)


@contratos.route('/contrato/modalidade/<modalidade>')
def contracts_for_modalidade(modalidade):
    page = int(request.args.get('page', 1))
    per_page_num = 10

    try:
        contratos_query = db.session.query(Contrato).filter(
            Contrato.modalidade == modalidade)
        contratos_ = contratos_query.offset(
            (page-1)*per_page_num).limit(per_page_num).all()
        count = contratos_query.count()
        pagination = Pagination(page=page, per_page=per_page_num, total=count,
                                found=count, bs_version=3, search=True,
                                record_name='contratos')

        return render_template('contratos-orgao.html',
                               contratos=contratos_,
                               pagination=pagination,
                               count=count,
                               filter_info="Modalidade",
                               filter_value=modalidade)

    except TemplateNotFound:
        abort(404)


@contratos.route('/contrato/evento/<evento>')
def contracts_for_evento(evento):
    page = int(request.args.get('page', 1))
    per_page_num = 10

    try:
        contratos_query = db.session.query(Contrato).filter(
            Contrato.evento == evento)
        contratos_ = contratos_query.offset(
            (page-1)*per_page_num).limit(per_page_num).all()
        count = contratos_query.count()
        pagination = Pagination(page=page, per_page=per_page_num, total=count,
                                found=count, bs_version=3, search=True,
                                record_name='contratos')

        return render_template('contratos-orgao.html',
                               contratos=contratos_,
                               pagination=pagination,
                               count=count,
                               filter_info="Evento",
                               filter_value=evento)

    except TemplateNotFound:
        abort(404)


@contratos.route('/contratos')
def all_contracts():
    page = int(request.args.get('page', 1))
    per_page_num = 40
    try:
        contratos_ = db.session.query(Contrato).offset(
            (page-1)*per_page_num).limit(per_page_num).all()
        count = db.session.query(Contrato).count()
        pagination = Pagination(page=page, per_page=per_page_num, total=count,
                                found=count, bs_version=3, search=True,
                                record_name='contratos')

        return render_template('todos-contratos.html',
                               contratos=contratos_,
                               pagination=pagination,
                               count=count)
    except TemplateNotFound:
        abort(404)
