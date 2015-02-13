# -*- coding: utf-8 -*-

# http://docs.fabfile.org/en/1.5/tutorial.html

from fabric.api import *
from fabric.network import ssh
from flask.ext.script import Manager

from gastosabertos.extensions import db
from gastosabertos import create_app

app = create_app()

project = "gastosabertos"

env.user = 'gastosabertos'
env.hosts = ['gastosabertos.org']
#env.key_filename = '~/.ssh/ga_id_rsa'

env.run_in = run

@task
def run_local():
    env.run_in = local

@task
def reset():
    """
    Reset local debug env.
    """

    env.run_in("rm -rf /tmp/instance")
    env.run_in("mkdir /tmp/instance")

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

    project_dir = '/home/gastosabertos/gastos_abertos'
    with cd(project_dir):
        run("git pull")
        with prefix("source /home/gastosabertos/.virtualenvs/ga/bin/activate"):
            run("python setup.py install")
        run("touch wsgi.py")

@task
def initdb():
    """
    Init or reset database
    """

    with app.app_context():
        db.drop_all()
        db.create_all()

@task
def importdata(lines_per_insert=100):
    """
    Import data to the local DB
    """

    import_commands = """
    python utils/import_revenue_codes.py
    python utils/import_revenue.py data/receitas_min.csv {lines_per_insert}
    """.format(lines_per_insert=lines_per_insert)

    env.run_in(import_commands)


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
