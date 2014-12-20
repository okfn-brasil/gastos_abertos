#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pandas as pd
from flask import Flask
app = Flask(__name__)


csv_receita = os.path.join("..","data","receita-2008-01.csv")
df_receita = pd.read_csv(csv_receita)

page_receita = """
<!-- DataTables CSS -->
<link rel="stylesheet" type="text/css"
href="//cdn.datatables.net/1.10.4/css/jquery.dataTables.css">
  
<!-- jQuery -->
<script type="text/javascript" charset="utf8"
src="//code.jquery.com/jquery-1.10.2.min.js"></script>

<!-- DataTables -->
<script type="text/javascript" charset="utf8"
src="//cdn.datatables.net/1.10.4/js/jquery.dataTables.js"></script>
<script>
$(document).ready( function () {
    $('#table_id').DataTable();
} );
</script>
<table id="table_id" class="display">
    <thead>
        <tr>
            <th>ID</th>
            <th>Data</th>
            <th>Código</th>
            <th>Descrição</th>
            <th>Previsto</th>
            <th>Realizado</th>
        </tr>
    </thead>
    <tbody>
"""


@app.route("/receita")
def receita():
    global page_receita
    global df_receita
    count = 0
    for row in df_receita.iterrows():
        page_receita += "<tr>\n"
        for i in range(6):
            page_receita += "<td>%s</td>\n" % row[1][i]
        page_receita += "</tr>\n"
        # Avoids too much data...
        count += 1
        if count == 1000:
            break
    page_receita += "    </tbody> </table>"
    return page_receita

if __name__ == "__main__":
    app.run(debug=True)
