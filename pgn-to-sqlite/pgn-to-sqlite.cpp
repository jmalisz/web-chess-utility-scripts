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
// Size of 100 to cover all bits (773) and set length for SQLite (requires bytes)
// Could use 776, but 800 is a nice round number (it doesn't matter)
bitset<800> board_to_binary(Board board)
{
	Color::underlying piece_colors_list[2] = {Color::WHITE, Color::BLACK};
	PieceType::underlying piece_types_list[6] = {
		PieceType::PAWN,
		PieceType::ROOK,
		PieceType::KNIGHT,
		PieceType::BISHOP,
		PieceType::QUEEN,
		PieceType::KING};

	bitset<800> bits(0);
	int position = 0;
	for (auto piece_color : piece_colors_list)
	{
		for (auto piece_type : piece_types_list)
		{
			// Append position bits
			bitset<800> temp_bits = board.pieces(piece_type, piece_color).getBits();
			temp_bits <<= position;
			bits |= temp_bits;

			position += 64;
		}
	}

	// Adjusting position of bits in the byte
	position += 3;
	// Set castling rights bits
	for (auto piece_color : piece_colors_list)
	{
		bits.set(
			position,
			board.castlingRights().has(piece_color, Board::CastlingRights::Side::KING_SIDE));
		position += 1;

		bits.set(
			position,
			board.castlingRights().has(piece_color, Board::CastlingRights::Side::QUEEN_SIDE));
		position += 1;
	}

	// Set turn bits
	bits.set(position, board.sideToMove() == Color::WHITE);

	return bits;
}

class PgnVisitor : public pgn::Visitor
{
public:
	PgnVisitor(string db_name)
	{
		const auto sql_create_table_elo_fen_outcomes = R"(
			CREATE TABLE IF NOT EXISTS elo_fen_outcomes(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				Site TEXT,
				PositionFen TEXT,
				PositionBinary BLOB,
				Elo INTEGER,
				WhiteWon BOOLEAN);
		)";
		const auto sql_count_elo_fen_outcomes = R"(
			SELECT COUNT(1) FROM elo_fen_outcomes
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
			sql_count_elo_fen_outcomes,
			save_count_table_pgn_games,
			&table_count_elo_fen_outcomes,
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
	int table_count_elo_fen_outcomes = 0;

	void startPgn()
	{
		headersMap.clear();
		movesList.clear();
	}
	void header(string_view key, string_view value)
	{
		// Skip already added games
		if (index < table_count_elo_fen_outcomes)
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
		if (index < table_count_elo_fen_outcomes)
		{
			return;
		}

		movesList.push_back(string(move));
	}
	void endPgn()
	{
		// Skip already added games
		if (index < table_count_elo_fen_outcomes)
		{
			index += 1;

			return;
		}
		// Used for early stopping
		// if (index == 100000)
		// {
		// 	abort();
		// }

		int db_exit_code = SQLITE_OK;
		char *db_error_message;

		// Update elo_fen_outcomes table
		try
		{
			stmt = NULL;
			const auto sql_insert_to_elo_fen_outcomes = R"(
				INSERT INTO elo_fen_outcomes (
					Site,
					PositionFen,
					PositionBinary,
					Elo,
					WhiteWon
				) VALUES (?, ?, ?, ?, ?);
			)";

			Board board;
			for (auto moveString : movesList)
			{
				Move moveObj = uci::parseSan(board, string(moveString));
				board.makeMove(moveObj);

				string Site = headersMap["Site"];
				string PositionFen = board.getFen();
				bitset<800> PositionBinary = board_to_binary(board);
				int Elo = (atoi(headersMap["WhiteElo"].c_str()) + atoi(headersMap["WhiteElo"].c_str())) / 2;
				bool WhiteWon = headersMap["Result"] == "1-0";

				try
				{
					db_exit_code = sqlite3_prepare_v2(db, sql_insert_to_elo_fen_outcomes, -1, &stmt, NULL);
					db_exit_code = sqlite3_bind_text(stmt, 1, Site.c_str(), -1, SQLITE_TRANSIENT);
					db_exit_code = sqlite3_bind_text(stmt, 2, PositionFen.c_str(), -1, SQLITE_TRANSIENT);
					db_exit_code = sqlite3_bind_blob(stmt, 3, &PositionBinary, 100, SQLITE_TRANSIENT);
					db_exit_code = sqlite3_bind_int(stmt, 4, Elo);
					db_exit_code = sqlite3_bind_int(stmt, 5, WhiteWon);
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
