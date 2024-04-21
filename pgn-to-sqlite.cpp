#include <iostream>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>
#include <memory>
#include <map>
#include <stdexcept>
#include <sqlite3.h>
#include "chess.hpp"

using namespace std;
using namespace chess;

string vector_to_json_array(vector<string> vec)
{
	stringstream ss;

	ss << "[";
	for (auto it = vec.begin(); it != vec.end(); it++)
	{
		if (it != vec.begin())
		{
			ss << ", ";
		}
		ss << '"' << *it << '"';
	}
	ss << "]";

	return ss.str();
}

// Each piece and color is coded to 64 boards with 1s
// Then coded are castling rights and side to move
bitset<773> board_to_binary(Board board)
{
	Color::underlying piece_colors_list[2] = {Color::WHITE, Color::BLACK};
	PieceType::underlying piece_types_list[6] = {
		PieceType::PAWN,
		PieceType::ROOK,
		PieceType::KNIGHT,
		PieceType::BISHOP,
		PieceType::QUEEN,
		PieceType::KING};

	bitset<773> bits(0);
	int position = 0;
	for (auto piece_color : piece_colors_list)
	{
		for (auto piece_type : piece_types_list)
		{
			// Append position bits
			bitset<773> temp_bits = board.pieces(piece_type, piece_color).getBits();
			temp_bits <<= position;
			bits |= temp_bits;

			position += 64;
		}

		// Append castling rights bits
		bitset<773> temp_bits = board.castlingRights().has(piece_color, Board::CastlingRights::Side::KING_SIDE);
		temp_bits <<= position;
		bits |= temp_bits;
		position += 1;
		temp_bits.reset();
		temp_bits = board.castlingRights().has(piece_color, Board::CastlingRights::Side::QUEEN_SIDE);
		temp_bits <<= position;
		bits |= temp_bits;
		position += 1;
	}

	// Append turn bit
	bitset<773> temp_bits = board.sideToMove() == Color::WHITE;
	temp_bits <<= position;
	bits |= temp_bits;

	return bits;
}

map<string, int> SqlPgnGamesIndexMap = {
	{"Event", 1},
	{"Site", 2},
	{"White", 3},
	{"Black", 4},
	{"Result", 5},
	{"UTCDate", 6},
	{"UTCTime", 7},
	{"WhiteElo", 8},
	{"BlackElo", 9},
	{"WhiteRatingDiff", 10},
	{"BlackRatingDiff", 11},
	{"ECO", 12},
	{"Opening", 13},
	{"TimeControl", 14},
	{"Termination", 15},
	{"Moves", 16},
};
map<string, int> SqlEloFenOutcomesIndexMap = {
	{"Elo", 1},
	{"PositionFen", 2},
	{"EndPositionFen", 3},
};

class PgnVisitor : public pgn::Visitor
{
public:
	PgnVisitor(string db_name)
	{
		const auto sql_create_table_pgn_games = R"(
			CREATE TABLE IF NOT EXISTS pgn_games (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				Event TEXT,
				Site TEXT,
				White TEXT,
				Black TEXT,
				Result TEXT,
				UTCDate TEXT,
				UTCTime TEXT,
				WhiteElo TEXT,
				BlackElo TEXT,
				WhiteRatingDiff TEXT,
				BlackRatingDiff TEXT,
				ECO TEXT,
				Opening TEXT,
				TimeControl TEXT,
				Termination TEXT,
				Moves TEXT);
		)";
		const auto sql_create_table_elo_fen_outcomes = R"(
			CREATE TABLE IF NOT EXISTS elo_fen_outcomes(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				Site TEXT,
				PositionFen TEXT,
				PositionBinary BLOB,
				Elo INTEGER,
				WhiteWon BOOLEAN);
		)";
		const auto sql_count_pgn_games = R"(
			SELECT COUNT(1) FROM pgn_games
		)";

		int db_exit_code = SQLITE_OK;
		char *db_error_message;

		try
		{
			// Create to the SQLite database
			db_exit_code = sqlite3_open(db_name.c_str(), &db);
			if (db_exit_code != SQLITE_OK)
			{
				throw "Error creating DB";
			}

			// Create the pgn_games table
			db_exit_code = sqlite3_exec(db, sql_create_table_pgn_games, NULL, NULL, &db_error_message);
			if (db_exit_code != SQLITE_OK)
			{
				throw "Error creating pgn_games table";
			}

			// Create the elo_fen_outcomes table
			db_exit_code = sqlite3_exec(db, sql_create_table_elo_fen_outcomes, NULL, NULL, &db_error_message);
			if (db_exit_code != SQLITE_OK)
			{
				throw "Error creating elo_fen_outcomes table";
			}

			// Speeds up the DB I/O
			sqlite3_exec(db, "PRAGMA synchronous=OFF", NULL, NULL, &db_error_message);
			sqlite3_exec(db, "PRAGMA count_changes=OFF", NULL, NULL, &db_error_message);
			sqlite3_exec(db, "PRAGMA journal_mode=MEMORY", NULL, NULL, &db_error_message);
			sqlite3_exec(db, "PRAGMA temp_store=MEMORY", NULL, NULL, &db_error_message);
		}
		catch (const char *err_message)
		{
			cerr << err_message << endl;
			cerr << sqlite3_errmsg(db) << endl;
			sqlite3_free(db_error_message);
			abort();
		}

		db_exit_code = sqlite3_exec(
			db,
			sql_count_pgn_games,
			save_count_table_pgn_games,
			&pgn_games_table_count,
			&db_error_message);
		if (db_exit_code != SQLITE_OK)
		{
			throw "Error counting pgn_games table";
		}
	}
	~PgnVisitor()
	{
		sqlite3_close_v2(db);
	}

private:
	sqlite3 *db;
	sqlite3_stmt *stmt;
	map<string, string> headersMap;
	vector<string> movesList;
	int index = 0;
	// Used to skip already added indexes
	int pgn_games_table_count = 0;

	void startPgn()
	{
		headersMap.clear();
		movesList.clear();
	}
	void header(string_view key, string_view value)
	{
		// Skip already added games
		if (index < pgn_games_table_count)
		{
			return;
		}

		// Those fields aren't used and can be empty
		if (key == "WhiteTitle" || key == "BlackTitle")
		{
			return;
		}

		headersMap[string(key)] = string(value);
	}
	void startMoves() {}
	void move(string_view move, string_view comment)
	{
		// Skip already added games
		if (index < pgn_games_table_count)
		{
			return;
		}

		movesList.push_back(string(move));
	}
	void endPgn()
	{
		// Skip already added games
		if (index < pgn_games_table_count)
		{
			index += 1;

			return;
		}

		int db_exit_code = SQLITE_OK;
		char *db_error_message;

		// Update pgn_games table
		try
		{
			stmt = NULL;
			const auto sql_insert_to_pgn_games = R"(
				INSERT INTO pgn_games (
					Event,
					Site,
					White,
					Black,
					Result,
					UTCDate,
					UTCTime,
					WhiteElo,
					BlackElo,
					WhiteRatingDiff,
					BlackRatingDiff,
					ECO,
					Opening,
					TimeControl,
					Termination,
					Moves
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
			)";

			db_exit_code = sqlite3_prepare_v2(db, sql_insert_to_pgn_games, -1, &stmt, NULL);
			for (auto [key, value] : headersMap)
			{
				db_exit_code = sqlite3_bind_text(
					stmt,
					SqlPgnGamesIndexMap.at(string(key)),
					string(value).c_str(),
					-1,
					SQLITE_TRANSIENT);
			}
			db_exit_code = sqlite3_bind_text(stmt, 16, vector_to_json_array(movesList).c_str(), -1, SQLITE_TRANSIENT);
			db_exit_code = sqlite3_step(stmt);
			db_exit_code = sqlite3_finalize(stmt);
		}
		catch (const char *err_message)
		{

			cerr << err_message << endl;
			cerr << sqlite3_errmsg(db) << endl;
			sqlite3_free(db_error_message);
			abort();
		}

		// Update elo_fen_outcomes table
		try
		{
			stmt = NULL;
			const auto sql_insert_to_elo_fen_outcomes = R"(
				INSERT INTO elo_fen_outcomes (
					PositionFen,
					PositionBinary,
					Elo,
					WhiteWon
				) VALUES (?, ?, ?, ?);
			)";

			Board board;
			for (auto moveString : movesList)
			{
				Move moveObj = uci::parseSan(board, string(moveString));
				board.makeMove(moveObj);

				string PositionFen = board.getFen();
				bitset<773> PositionBinary = board_to_binary(board);
				int Elo = (atoi(headersMap["WhiteElo"].c_str()) + atoi(headersMap["WhiteElo"].c_str())) / 2;
				bool WhiteWon = headersMap["Result"] == "1-0";

				try
				{
					db_exit_code = sqlite3_prepare_v2(db, sql_insert_to_elo_fen_outcomes, -1, &stmt, NULL);
					db_exit_code = sqlite3_bind_text(stmt, 1, PositionFen.c_str(), -1, SQLITE_STATIC);
					db_exit_code = sqlite3_bind_blob(stmt, 2, PositionBinary.to_string().c_str(), -1, SQLITE_STATIC);
					db_exit_code = sqlite3_bind_int(stmt, 3, Elo);
					db_exit_code = sqlite3_bind_int(stmt, 4, WhiteWon);
					db_exit_code = sqlite3_step(stmt);
					db_exit_code = sqlite3_finalize(stmt);
				}
				catch (const char *err_message)
				{
					cerr << err_message << endl;
					cerr << sqlite3_errmsg(db) << endl;
					sqlite3_free(db_error_message);
					abort();
				}
			}
		}
		catch (const std::exception &e)
		{
			cerr << "Failed parsing following game:" << endl;

			cerr << vector_to_json_array(movesList) << endl;

			cout << "Resuming parsing..." << endl;
		}

		index += 1;
		if (index % 100000 == 0)
		{
			cout << "Finished parsing game number: " << index << endl;
		}
	}
	static int save_count_table_pgn_games(void *count, int argc, char **argv, char **azColName)
	{
		int *c = (int *)count;

		*c = atoi(argv[0]);

		return 0;
	}
};

int main()
{
	auto pgn_file = ifstream("lichess_db_standard_rated_2016-05.pgn");
	auto pgn_visitor = make_unique<PgnVisitor>("lichess_db_standard_rated_2016-05.sqlite");

	pgn::StreamParser parser(pgn_file);
	parser.readGames(*pgn_visitor);

	pgn_file.close();

	return 0;
}
