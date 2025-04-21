# Testing

There are two scripts in the `testing` folder that run local tests on the code: `testing.bat` for Windows systems and `testing.sh` for Linux systems.
These test the code for:
 - Code style with [`ruff`](https://docs.astral.sh/ruff/),
 - Type correctness with [`mypy`](https://www.mypy-lang.org/), and
 - Functionality with [`doctest`](https://docs.python.org/3/library/doctest.html) and [`unittest`](https://docs.python.org/3/library/unittest.html).

 There is also a GitHub workflow that runs the above tests for pull requests and commits to the `main` branch.
