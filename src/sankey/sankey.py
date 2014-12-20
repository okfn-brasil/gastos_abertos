#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json

df = pd.read_excel("basedadosexecucao2014.xls")

dic = {"nodes": [],
       "links": []}

nodes = []
links = {}

for row in df.iterrows():
    # Extrac data from row
    source = row[1]['Ds_Fonte'].strip()
    target = row[1]['Ds_Funcao'].partition('-')[2].strip()
    value = float(row[1]['Vl_Pago'])
    # Add to nodes, if not already there
    if source not in nodes:
        nodes.append(source)
    if target not in nodes:
        nodes.append(target)
    # Calculate values
    index_source = nodes.index(source)
    index_target = nodes.index(target)
    current_value = links.get((index_source, index_target), 0)
    links[(index_source, index_target)] = current_value + value

# Prepare dict for json exportation in the required format
for node in nodes:
    dic["nodes"].append({"name": node})
for link, value in links.items():
    dic["links"].append({"source": link[0],
                         "target": link[1],
                         "value": value})

file = open("budget.json", 'w')
json.dump(dic, file)
