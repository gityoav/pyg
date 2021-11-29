import os
import sys
import platform
from setuptools import setup, find_packages
from distutils.core import Extension

setup(name = 'pyg', version = '1.0.1', packages = find_packages(), python_requires = '>=3.7.')