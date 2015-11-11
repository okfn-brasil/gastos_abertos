# -*- coding: utf-8 -*-

#import locale
import os

from ..models import db, Column, Model, searchable

#try:
#    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
#except:
#    pass


class Contrato(Model):

    __tablename__ = 'contratos'

    id = Column(db.Integer, primary_key=True)
    numero = Column(db.String(60), nullable=False)
    orgao = Column(db.Text(), searchable=True, boost=2)
    data_assinatura = Column(db.DateTime())
    vigencia = Column(db.Integer)
    objeto = Column(db.Text(), searchable=True, boost=3)
    modalidade = Column(db.Text(), searchable=True)
    evento = Column(db.Text(), searchable=True)
    processo_administrativo = Column(db.Text(), searchable=True)
    cnpj = Column(db.Text(), searchable=True, search_config={'index': 'not_analyzed'})
    nome_fornecedor = Column(db.Text(), searchable=True, boost=2)
    valor = Column(db.DECIMAL(19, 2), searchable=True)
    licitacao = Column(db.String(60), nullable=False, searchable=True)
    data_publicacao = Column(db.DateTime())
    file_url = Column(db.Text())
    txt_file_url = Column(db.Text())

    @property
    def fvalor(self):
        #return locale.currency(self.valor, grouping=True)
        return '{:,.2f}'.format(self.valor
                                ).replace('.', 't'
                                          ).replace(',', '.'
                                                    ).replace('t', ',')

    @property
    def fdata_assinatura(self):
        return self.data_assinatura.strftime("%d/%m/%Y")

    @property
    def fdata_publicacao(self):
        return self.data_publicacao.strftime("%d/%m/%Y")

    @property
    def clean_cnpj(self):
        return ''.join([c for c in self.cnpj if c not in ['.', '/', '-']])

    @property
    @searchable(db.Text())
    def content(self):
        if not hasattr(self, '__content_cache'):
            content = ''
            url = self.txt_file_url
            if url and isinstance(url, basestring) and url.startswith('http'):
                filename = url.split('/')[-1]
                filepath = os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    '..', '..', 'data', 'contratos', filename)
                if os.path.isfile(filepath):
                    # try to get the content from a local file
                    with open(filepath, 'r') as file:
                        content = file.read()
                else:
                    # could not get the local file, lets download it
                    import urllib2
                    try:
                        file = urllib2.urlopen(url)
                        content = file.read()
                    except urllib2.HTTPError:
                        content = ''
                    finally:
                        file.close()
            self.__content_cache = content.decode('utf-8', 'ignore').strip()
        return self.__content_cache
