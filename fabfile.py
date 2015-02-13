# -*- coding: utf-8 -*-

# http://docs.fabfile.org/en/1.5/tutorial.html

from fabric.api import *
from fabric.network import ssh
from flask.ext.script import Manager

project = "gastosabertos"
project_dir = '/home/gastosabertos/gastos_abertos'

env.user = 'gastosabertos'
env.hosts = ['gastosabertos.org']
# env.key_filename = '~/.ssh/ga_id_rsa'


def smart_run(command, place, inside_env=True):
    if place == "local":
        local(command)
    elif place == "remote":
        if inside_env:
            with cd(project_dir):
                with prefix(
                        "source /home/gastosabertos/.virtualenvs/ga/bin/activate"):
                    run(command)
        else:
            run(command)
    else:
        print("Where to import? 'local' or 'remote'?")


def reset(place="local"):
    """
    Reset local debug env.
    """

    command = """
    rm -rf /tmp/instance
    mkdir /tmp/instance
    """
    smart_run(command, place)


def setup():
    """
    Setup virtual env.
    """

    local("virtualenv env")
    activate_this = "env/bin/activate_this.py"
    execfile(activate_this, dict(__file__=activate_this))
    local("python setup.py install")
    reset()


def deploy():
    """
    Deploy project to Gastos Abertos server
    """

    with cd(project_dir):
        run("git pull")
        with prefix("source /home/gastosabertos/.virtualenvs/ga/bin/activate"):
            run("python setup.py install")
        run("touch wsgi.py")


def initdb(place="local"):
    """
    Init or reset database
    """

    command = "python manage.py initdb"
    smart_run(command, place)


def importdata(place="local", lines_per_insert=100):
    """
    Import data to the local DB
    """

    command = """
    python utils/import_revenue_codes.py
    python utils/import_revenue.py data/receitas_min.csv {lines_per_insert}
    """.format(lines_per_insert=lines_per_insert)
    smart_run(command, place)


def d():
    """
    Debug.
    """

    reset()
    local("python manage.py run")


def babel():
    """
    Babel compile.
    """

    local("python setup.py compile_catalog --directory `find -name translations` --locale zh -f")
