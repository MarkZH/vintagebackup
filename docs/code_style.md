# Code Style

## General

Most codestyle is enforce by [ruff](https://docs.astral.sh/ruff/) with the ruff.toml configuration in the testing directory.

## Make alignment safe from renaming

The purpose of this formatting is to allow for variables and functions to be renamed without needing to fix the alignment of the arguments and container items on the following lines.
Either everything is on one line, or the changing names and arguments/items are on separate lines.
Either way, renaming no longer leaves the first argument/item misaligned with the others.
This also makes it easier to change the indentation level of a code block when adding or removing loops, conditional, and try-except blocks.

This was inspired by the Kevlin Henney talk [Seven Ineffective Coding Habits of Many Programmers](https://www.youtube.com/watch?v=SUIUZ09mnwM&t=1214s)

### Rules

If an argument list makes a function declaration or call too long, put all of the arguments on the next line indented, leaving the opening parentheses on the same line as the function name. Function arguments declarations are indented by 8 spaces (to distinguish them from the following code), call arguments by 4. The closing parenthesis (and colon for declarations) are on the same line as the last argument.

Container literals are similar. The opening parenthesis, brace, or bracket is left on the same line as the variable declaration, and all items are moved to the next line, indented by 4 spaces. The closing parenthesis, brace, or bracket is on the same line as the last item.
