# pgn-to-sqlite

This script converts pgn game files from [Lichess](https://database.lichess.org/#standard_games) to SQLite databases for easier management.

## Usage

You need to have [SQLite](https://sqlite.org/index.html) installed. It also helps to have some way of browsing the datbase, for example [SQLiteBrowser](https://sqlitebrowser.org/).

Use `g++ pgn-to-sqlite.cpp -o pgn-to-sqlite.bin -lsqlite3 -O3` to compile and `./pgn-to-sqlite.bin` to run.
