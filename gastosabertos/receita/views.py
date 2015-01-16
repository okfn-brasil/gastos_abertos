# -*- coding: utf-8 -*-

import os
import pandas as pd

from flask import (Blueprint, render_template)

receita = Blueprint('receita', __name__,
                    template_folder='templates',
                    static_folder='static',
                    static_url_path='/receita/static')

# csv_receita = os.path.join(receita.root_path, 'static', 'receita-2008-01.csv')
# df_receita = pd.read_csv(csv_receita, encoding='utf8')

years = range(2008, 2015)


def get_year_data(year):
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
