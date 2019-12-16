#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os
import re

with open("README.md", "r", encoding="utf8") as fh:
    readme = fh.read()

package = "clickhousepy"


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    init_py = open(os.path.join(package, "__init__.py")).read()
    return re.search("^__version__ = ['\"]([^'\"]+)['\"]", init_py, re.MULTILINE).group(
        1
    )


setup(
    name="clickhousepy",
    version=get_version(package),
    description="Python обертка для запросов в БД Clickhouse",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Pavel Maksimov",
    author_email="vur21@ya.ru",
    url="https://github.com/pavelmaksimov/clickhousepy",
    install_requires=["clickhouse_driver", "pandas"],
    packages=[package],
    license="MIT",
    zip_safe=False,
    keywords="clickhouse,python,wrapper",
    test_suite="tests",
)
