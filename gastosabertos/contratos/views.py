# -*- coding: utf-8 -*-

import os
import json
from sqlalchemy import and_, extract, func
from datetime import datetime

from flask import Blueprint, render_template, send_from_directory
from flask.ext import restful
from flask.ext.restful import fields
from flask.ext.restful.reqparse import RequestParser

from .models import Contrato
from gastosabertos.extensions import db

# Blueprint for Receita
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


