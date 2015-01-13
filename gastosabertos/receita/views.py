# -*- coding: utf-8 -*-

import os
import pandas as pd

from flask import (Blueprint, render_template)

receita = Blueprint('receita', __name__,
                    template_folder='templates',
                    static_folder='static',
                    static_url_path='/receita/static')

csv_receita = os.path.join(receita.root_path, 'static', 'receita-2008-01.csv')
df_receita = pd.read_csv(csv_receita, encoding='utf8')

@receita.route('/receita')
@receita.route('/receita/<int:page>')
def receita_table(page=0):
    receita_data = df_receita.iterrows()

    return render_template('fulltable.html', receita_data=receita_data)


@receita.route('/sankey/<path:filename>')
def sankey(filename):
    return send_from_directory('sankey', filename)
