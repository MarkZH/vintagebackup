echo upgrade
python3.13 -m pip install --upgrade pip
python3.13 -m pip install --upgrade mypy ruff

echo ruff
python3.13 -m ruff check --config=testing/ruff.toml --config "target-version = \"py313\"" || exit 1

echo mypy
python3.13 -m mypy --strict vintagebackup.py testing/test.py --python-version 3.13 || exit 1

echo unittest
python3.13 -m unittest testing/test.py || exit 1
