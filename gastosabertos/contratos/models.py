# -*- coding: utf-8 -*-

from sqlalchemy import Column, types

from ..extensions import db

import locale

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    locale.setlocale(locale.LC_ALL, '')

class Contrato(db.Model):

    __tablename__ = 'contratos'

    id = Column(db.Integer, primary_key=True)
    numero = Column(db.String(60), nullable=False)
    orgao = Column(db.Text())
    data_assinatura = Column(db.DateTime())
    vigencia = Column(db.Integer)
    objeto = Column(db.Text())
    modalidade = Column(db.Text())
    evento = Column(db.Text())
    processo_administrativo = Column(db.Text())  
    cnpj = Column(db.Text())
    nome_fornecedor = Column(db.Text())
    valor = Column(db.DECIMAL(19, 2))
    licitacao = Column(db.String(60), nullable=False)
    data_publicacao = Column(db.DateTime())
    file_url = Column(db.Text())
    txt_file_url = Column(db.Text())

    @property
    def fvalor(self):
        return locale.currency(self.valor, grouping=True)

    @property
    def clean_cnpj(self):
        return ''.join([c for c in self.cnpj if c not in ['.', '/', '-']])
