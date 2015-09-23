# -*- coding: utf-8 -*-

# http://docs.fabfile.org/en/1.5/tutorial.html

from fabric.api import *
from fabric.network import ssh
from flask.ext.script import Manager

project = "gastosabertos"

remote_project_dir = '/home/gastosabertos/gastos_abertos'
remote_prefix = "source /home/gastosabertos/.virtualenvs/ga/bin/activate"

env.user = 'gastosabertos'
env.hosts = ['gastosabertos.org']
# env.key_filename = '~/.ssh/ga_id_rsa'

env.place = "remote"


def smart_run(command, inside_env=False):
    if env.place == "local":
        local(command)
    elif env.place == "remote":
        if inside_env:
            with cd(remote_project_dir):
                with prefix(remote_prefix):
                    run(command)
        else:
            run(command)

env.run_in = smart_run


@task
def run_local():
    env.place = "local"


@task
def reset():
    """
    Reset local debug env.
    """

    env.run_in("""
    rm -rf /tmp/instance
    mkdir /tmp/instance
    """)


@task
def setup():
    """
    Setup virtual env.
    """

    env.run_in("virtualenv env")
    activate_this = "env/bin/activate_this.py"
    execfile(activate_this, dict(__file__=activate_this))
    env.run_in("python setup.py install")
    reset()


@task
def deploy():
    """
    Deploy project to Gastos Abertos server
    """

    env.run_in("""
    git pull
    python setup.py develop
    touch wsgi.py
    """, inside_env=True)


@task
def initdb():
    """
    Init or reset database
    """

    env.run_in("python manage.py initdb", inside_env=True)


@task
def importdata(lines_per_insert=100):
    """
    Import data to the local DB
    """

    # env.run_in("""
    # python utils/import_revenue_codes.py
    # python utils/import_revenue.py data/receitas_min.csv {lines_per_insert}
    # python utils/import_contrato.py
    # """.format(lines_per_insert=lines_per_insert), inside_env=True)
    env.run_in("python manage.py importdata", inside_env=True)


@task
def generate_jsons(year=''):
    """
    Generate Jsons for Highcharts
    """
    env.run_in("""
    python utils/generate_total_json.py -o data/total_by_year_by_code {year}
    """.format(year=year), inside_env=True)


@task
def d():
    """
    Debug.
    """

    reset()
    local("python manage.py run")


@task
def babel():
    """
    Babel compile.
    """

    env.run_in("python setup.py compile_catalog --directory `find -name translations` -f")
