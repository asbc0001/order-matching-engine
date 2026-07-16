# tools/

Small offline tools for creating saved command files.

`text_to_saved_commands` converts hand-written text commands into the binary
format consumed by trace replay.

`generate_saved_commands` creates a seeded synthetic command file. It runs the
matcher synchronously while generating commands so later cancel commands can use
real handles assigned by earlier accepted orders.
