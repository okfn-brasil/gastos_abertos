[![Stories in Ready](https://badge.waffle.io/okfn-brasil/gastos_abertos.png?label=ready&title=Ready)](https://waffle.io/okfn-brasil/gastos_abertos)

Gastos Abertos
==============

Main code of the Gastos Abertos project.

We are using the Flask micro framework.


## Installation (Debian like systems)

Install virtualenv and git:

    sudo apt-get install python-virtualenv git

Initiate a virtual environment you'll work with:

    virtualenv venv
    . venc/bin/activate

Clone this project repository and the data repository:

    git clone https://github.com/okfn-brasil/gastos_abertos.git
    git clone https://github.com/okfn-brasil/gastos_abertos_dados.git

Enter the project folder:

    cd gastos_abertos

Install python's dependencies:

    python setup.py install

Prepare DB and other files:

    fab reset initdb importdata generate_jsons

Start the server:

    python manage.py run



## Troubleshooting

`fab importdata` fails with:

    sqlalchemy.exc.OperationalError: (OperationalError) too many SQL variables u'INSERT INTO revenue (original_code, code_id, description, date, monthly_predicted, monthly_outcome, economical_category, economical_subcategory, source, rubric, paragraph, subparagraph)

Seems to be an [sqlite limitation](https://stackoverflow.com/questions/7106016/too-many-sql-variables-error-in-django-witih-sqlite3).
Use `fab importdata:local,80`.

