#! /usr/bin/env python3
# -*- coding: utf8 -*-


import os
import sys
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name = "DTE",
    version = "0.1",
    author = "Florian Kunneman, Rik van Noord",
    author_email = "f.kunneman@let.ru.nl",
    description = ("Framework for extracting Dutch Twitter events"),
    license = "GPL",
    keywords = "nlp computational_linguistics Twitter",
    url = "https://github.com/fkunneman/DTE.git",
    packages=['dte', 'dte.functions', 'dte.modules', 'dte.classes'],
    long_description=read('README.rst'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Text Processing :: Social Media :: Information Extraction :: Signal Processing",
        "Programming Language :: Python :: 3",
        "Operating System :: POSIX",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
    zip_safe=False,
    #include_package_data=True,
    #package_data = {'': ['*.wsgi','*.js','*.xsl','*.gif','*.png','*.xml','*.html','*.jpg','*.svg','*.rng'] },
    install_requires=['colibricore','ucto','openpyxl','xlrd'],
    #entry_points = {    'console_scripts': [
    #        'luiginlp = luiginlp.luiginlp:main',
    #]
    #}
)
