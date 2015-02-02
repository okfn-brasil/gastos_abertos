import pandas as pd
from datetime import datetime, timedelta
import calendar

from gastosabertos import create_app
from gastosabertos.extensions import db
from gastosabertos.receita.models import Revenue

app = create_app()
db.app = app

def parse_money(money_string):
    return float(money_string[3:].replace('.', '').replace(',', '.'))

def parse_date(date_string):
    year_month = datetime.strptime(date_string, '%Y-%m')
    date = year_month + timedelta(days = calendar.monthrange(year_month.year, year_month.month)[1] - 1)
    return date

def parse_code(code_string):
    return [int(i) for i in code_string.split('.')]

data = pd.read_csv('../data/receitas_min.csv')

for row_i, row in data.iterrows():
    pass

