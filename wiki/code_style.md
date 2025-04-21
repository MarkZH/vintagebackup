# Code Style

## General

Most codestyle is enforced by [`ruff`](https://docs.astral.sh/ruff/) with the ruff.toml configuration in the testing directory.

## Make alignment safe from renaming and reindenting

The purpose of this formatting is to allow for variables and functions to be renamed without needing to fix the alignment of the arguments and container items on the following lines.
Either everything is on one line, or lines are broken with one indentation for the continuation of the line.
The new lines are not aligned to parentheses, brackets, or braces on previous lines.
Renaming should not leave the first argument/item misaligned with the others.
This also makes it easier to change the indentation level of a code block when adding or removing loops, conditional, and try-except blocks.

This was inspired by the Kevlin Henney talk [Seven Ineffective Coding Habits of Many Programmers](https://www.youtube.com/watch?v=SUIUZ09mnwM&t=1214s).
It is similar to the formating produced by the program [black](https://black.readthedocs.io/en/stable/), but there are too many aspects of that style that annoy me--most prominently, closing braces/brackets/parenetheses on separate lines by themselves.

### Rules

If an argument list makes a function declaration or call too long, put all of the arguments on the next line indented, leaving the opening parentheses on the same line as the function name. Function arguments declarations are indented by 8 spaces (to distinguish them from the following code), call arguments by 4. The closing parenthesis (and colon for declarations) are on the same line as the last argument.
```python
def function_with_few_arguments(a: int, s: str) -> bool:
    """Docstring"""
    pass

def function_with_too_many_arguments(
        a: int,
        b: int,
        c: int,
        d: int,
        e: str) -> bool:
    """Docstring"""
    pass

def main() -> None:
    """Docstring"""
    function_with_too_many_arguments(
        1,
        2,
        3,
        4,
        "string argument that is too long to fit on one line.")
```

Container literals are similar. The opening parenthesis, brace, or bracket is left on the same line as the variable declaration, and all items are moved to the next line, indented by 4 spaces. The closing parenthesis, brace, or bracket is on the same line as the last item.
```python
the_raven = [
    "Once upon a midnight dreary, ",
    "while I pondered, weak and weary,",
    "over many a quaint and curious volume ",
    "of forgotten lore--"]
```

If a `with`-block contains multiple context managers that are too long for one line, put each context manager one it's own line starting one the same line as the with block.
Each subsequent line is indented four spaces.
Leave a blank line after the last one to indicate where the code block starts.
```python
with (context_manager_A() as A,
    context_manager_B() as B,
    context_manager_C() as C):

    # do the things
```
