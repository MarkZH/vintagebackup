@echo off

echo install testing dependencies
py -3.13 -m pip install pip
py -3.13 -m pip install mypy ruff

echo ruff
py -3.13 -m ruff check --config=testing\ruff.toml || exit /b

echo mypy
py -3.13 -m mypy --strict . || exit /b

echo unittest
py -3.13 -m unittest testing\test.py || exit /b
