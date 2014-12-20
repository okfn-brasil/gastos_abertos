import pandas as pd

receitas = pd.read_csv('receitas_min.csv')

for year_month, df in receitas.groupby('data'):
    df.to_csv("receita-{}".format(year_month))


