#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from setuptools import setup


setup(name="geo-playground",
      version="0.0.1",
      install_requires=[
          'pandas>0.22.0'
      ],
      setup_requires=[
          'wheel'
      ],
      packages=[
          'geo_playground'
      ],
      scripts=[
          'bin/merger'
      ],
      zip_safe=False)
