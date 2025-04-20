#!/bin/sh

echo upgrade
python3.13 -m pip install --upgrade pip
python3.13 -m pip install --upgrade mypy ruff

echo ruff
python3.13 -m ruff check --config=testing\ruff.toml || exit 1

echo doctest
python3.13 -m doctest vintagebackup.py testing\test.py || exit 1

echo mypy
python3.13 -m mypy --strict vintagebackup.py testing\test.py || exit 1

echo unittest
python3.13 -m unittest testing\test.py || exit 1
