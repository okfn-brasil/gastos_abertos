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

Clone this project repository:

    git clone https://github.com/okfn-brasil/gastos_abertos.git

Enter the project folder:

    cd gastos_abertos

Install python's dependencies:

    python setup.py install

Start the server:

    fab reset
    python manage.py run
