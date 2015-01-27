# -*- coding: utf-8 -*-

# http://docs.fabfile.org/en/1.5/tutorial.html

from fabric.api import *
from fabric.network import ssh

project = "gastosabertos"

env.user = 'gastosabertos'
env.hosts = ['gastosabertos.org']
#env.key_filename = '~/.ssh/ga_id_rsa'

def reset():
    """
    Reset local debug env.
    """

    local("rm -rf /tmp/instance")
    local("mkdir /tmp/instance")


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

    project_dir = '/home/gastosabertos/gastosabertos'
    with cd(project_dir):
        run("git pull")
#        run("touch app.wsgi")

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
