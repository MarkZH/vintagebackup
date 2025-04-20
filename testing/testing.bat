@echo off

echo upgrade
py -3.13 -m pip install --upgrade pip
py -3.13 -m pip install --upgrade mypy ruff

echo ruff
py -3.13 -m ruff check --config=testing\ruff.toml || exit /b

echo doctest
py -3.13 -m doctest vintagebackup.py testing\test.py || exit /b

echo mypy
py -3.13 -m mypy --strict vintagebackup.py testing\test.py || exit /b

echo unittest
py -3.13 -m unittest testing\test.py || exit /b
