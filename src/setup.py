import os
import sys
import platform
from setuptools import setup, find_packages
from distutils.core import Extension

setup(name = 'pygpyg', version = '1.0.20230210', packages = find_packages(), python_requires = '>=3.6.')