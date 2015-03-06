# -*- coding: utf-8 -*-

from setuptools import setup

project = "gastosabertos"

setup(
    name=project,
    version='0.0.1',
    url='https://github.com/okfn-brasil/gastos_abertos',
    description='Visualization of public spending in Sao Paulo city for Gastos Abertos project',
    author='Edgar Zanella Alvarenga',
    author_email='e@vaz.io',
    packages=["gastosabertos"],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'Flask>=0.10.1',
        'Flask-SQLAlchemy',
        'Flask-WTF',
        'Flask-Script',
        'Flask-Babel',
        'Flask-Testing',
        'Flask-Restful',
        'Flask-CORS',
        'fabric',
        'docopt',
        'pandas'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries'
    ]
)
