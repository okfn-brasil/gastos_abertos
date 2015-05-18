# -*- coding: utf-8 -*-

from sqlalchemy import Column, types

from ..extensions import db


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
