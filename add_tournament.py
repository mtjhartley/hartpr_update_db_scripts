import pyodbc
import struct
from datetime import datetime as dt
import uuid
import sys
import create_data_for_db
import database_connector

player_update_query_string = """UPDATE Players SET FirstName = "%s", LastName = "%s", Tag = "%s", State = "%s", UpdatedAt = CURRENT_TIMESTAMP WHERE SggPlayerId = %d """
player_insert_query_string = """INSERT into Players (Id, FirstName, LastName, Tag, State, Trueskill, SggPlayerId, CreatedAt, UpdatedAt) OUTPUT INSERTED.Id VALUES (NEWID(), "%s", "%s", "%s", "%s", 2500, %d, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""" 
set_with_score_insert_query_string = """INSERT into Sets (Id, WinnerId, LoserId, CreatedAt, UpdatedAt, TournamentId, WinnerScore, LoserScore) VALUES (NEWID(), "%s", "%s", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "%s", %d, %d)""" 
set_no_score_insert_query_string = """INSERT into Sets (Id, WinnerId, LoserId, CreatedAt, UpdatedAt, TournamentId, WinnerScore, LoserScore) VALUES (NEWID(), "%s", "%s", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "%s", NULL, NULL)""" 

#Get a map of SGG Player Id to Player GUID from db
def get_player_sgg_ids_to_guids_from_db_map(crsr):
	player_sgg_ids_to_guids_from_db_map = {}

	crsr.execute("SELECT SggPlayerId, Id FROM Players")
	row = crsr.fetchone()
	while row:
		player_sgg_ids_to_guids_from_db_map.update( {str(row[0]): row[1] } )
		row = crsr.fetchone()

	return player_sgg_ids_to_guids_from_db_map

def get_player_last_active_date_map(crsr):
	player_last_active_date_map = {}

	crsr.execute("SELECT SggPlayerId, LastActive FROM Players")
	row = crsr.fetchone()
	while row:
		player_last_active_date_map.update( {str(row[0]): row[1] } )
		row = crsr.fetchone()

	return player_last_active_date_map

def get_game_id(crsr, tournament):
	crsr.execute("""SELECT Id FROM Games WHERE event = "%s" """ % tournament["event_name"])
	game_id = crsr.fetchone()
	if (game_id):
		game_id = str(game_id[0])

	return game_id

#Return a boolean and use it to end the main method if the tournament is already in the DB
def does_tournament_exist(crsr, tournament, game_id):
	print ("Checking if tournament exists in the database already...")
	tournament_id_from_database = None

	crsr.execute("""SELECT SggTournamentId from TOURNAMENTS where SggTournamentId = %d and GameId = "%s" """ % (tournament["sgg_tournament_id"], game_id))
	row = crsr.fetchone()
	if (row):
		print ("The sggTournamentId is '%s' the content of the row is '%s' and the name of the tournament is '%s' " % (tournament["sgg_tournament_id"], row, tournament["name"]))
		print ("This tournament exists already, ending application")
		return True
	print ("""This is a new tournament for this event: "%s" ! Beginning process to add Tournament, Players, and Sets...""" % tournament["event_name"])
	return False

#Process to add tournament to DB
def add_tournament_to_db(crsr, tournament, game_id):
	print ("Adding %s to Database" % tournament["name"])
	crsr.execute("""INSERT into Tournaments (Id, Name, URL, Date, CreatedAt, UpdatedAt, SggTournamentId, Website, GameId) OUTPUT INSERTED.Id VALUES (NEWID(), "%s", "%s", "%s", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "%d", "%s", "%s")""" % (tournament["name"], tournament["url"], tournament["date"], tournament["sgg_tournament_id"], tournament["website"], game_id))
	row = crsr.fetchone()
	tournament_id_from_database = str(row[0])
	return tournament_id_from_database

#Process to add players to DB
def add_or_update_players_to_db(crsr, players, player_sgg_ids_to_guids_from_db_map, player_last_active_date_map):
	print ("Beginning to add players to the database. If the players already exist, they will be updated with their latest information from smashgg.")
	player_added_count = 0
	player_updated_count = 0

	for player in players:
		if str(player["sgg_player_id"]) in player_sgg_ids_to_guids_from_db_map.keys():
			#If the player last active date from teh database is none, we're going to assign the player["last_active"] from the tournament.
			#if (player_last_active_date_map[str(player["sgg_player_id"])] == None):
			#	pass
			#if the player[last_active] is smaller than the one from the database, aka adding an older tournament, rewrite the last active to be the db value
			#REMOVING THIS LAST ACTIVE THING, WILL BE RUN IN THE CALCULATE_TRUESKILL_HISTORY_SCRIPT.
			#elif dt.strptime(player["last_active"], "%Y-%m-%d") < dt.strptime(player_last_active_date_map[str(player["sgg_player_id"])].split(" ")[0], "%Y-%m-%d"):
			#	player["last_active"] = player_last_active_date_map[str(player["sgg_player_id"])]

			update_query_with_params = player_update_query_string % (player["fname"], player["lname"], player["tag"], player["state"], player["sgg_player_id"])
			crsr.execute(update_query_with_params)
			player_updated_count += 1
		else:
			insert_query_with_params = player_insert_query_string % (player["fname"], player["lname"], player["tag"], player["state"], player["sgg_player_id"])
			crsr.execute(insert_query_with_params)
			row = crsr.fetchone()
			player_sgg_ids_to_guids_from_db_map.update( {str(player["sgg_player_id"]): str(row[0])} )
			player_added_count += 1

	print ("Players added : %d" % player_added_count)
	print ("Players updated : %d" % player_updated_count)

#Process to add sets to DB
def add_sets_to_db(crsr, sets, player_sgg_ids_to_guids_from_db_map, tournament_id_from_database):
	print ("Beginning work on adding sets to the database.")
	for set in sets:
		if str(set["sgg_winner_id"]) in player_sgg_ids_to_guids_from_db_map.keys():
			set["database_winner_id"] = player_sgg_ids_to_guids_from_db_map[str(set["sgg_winner_id"])]
		else:
			#could not match for unknown reason, store player as empty guid. 
			set["database_winner_id"] = "00000000-0000-0000-0000-000000000000"
		if str(set["sgg_loser_id"]) in player_sgg_ids_to_guids_from_db_map.keys():
			set["database_loser_id"] = player_sgg_ids_to_guids_from_db_map[str(set["sgg_loser_id"])]
		else:
			set["database_loser_id"] = "00000000-0000-0000-0000-000000000000"
		if (set["winner_score"] == None or set["loser_score"] == None ):
			insert_query_with_params = set_no_score_insert_query_string % (set["database_winner_id"], set["database_loser_id"], tournament_id_from_database)
		else:
			insert_query_with_params = set_with_score_insert_query_string % (set["database_winner_id"], set["database_loser_id"], tournament_id_from_database, set["winner_score"], set["loser_score"])
		crsr.execute(insert_query_with_params)
	print ("Sets successfully added to the database!")


def main(tournament_slug, event_name):
	tournament_exists = False

	cnxn = database_connector.create_connection(database_connector.CONNECTION_STRING)
	cnxn.add_output_converter(-155, database_connector.handle_datetimeoffset)
	cursor = cnxn.cursor()

	data_dictionary = create_data_for_db.create_data_for_database_entry(tournament_slug, event_name)
	players = data_dictionary["players"]
	tournament = data_dictionary["tournament"]
	sets = data_dictionary["sets"]

	game_id = get_game_id(cursor, tournament)

	tournament_exists = does_tournament_exist(cursor, tournament, game_id)
	if (tournament_exists):
		return #end the app lmao
	new_tournament_guid = add_tournament_to_db(cursor, tournament, game_id)

	player_sgg_ids_to_guids_from_db_map = get_player_sgg_ids_to_guids_from_db_map(cursor)
	player_last_active_date_map = get_player_last_active_date_map(cursor)

	add_or_update_players_to_db(cursor, players, player_sgg_ids_to_guids_from_db_map, player_last_active_date_map)

	add_sets_to_db(cursor, sets, player_sgg_ids_to_guids_from_db_map, new_tournament_guid)

	print ("All changes staged!")
	
	cnxn.commit()
	print ("Commited changes to database! Thanks for helping out :)")
	cnxn.close()

if __name__ == "__main__":
	#if running this script directly, you need to supply the tournament slug and the event name
	#this should never be done, use the game specific scripts.
	main(sys.argv[1], sys.argv[2])




##Sample insert query
#cursor.execute("INSERT into Players (Id, FirstName, LastName, Tag, State, Trueskill, SggPlayerId, CreatedAt, UpdatedAt) OUTPUT INSERTED.Id VALUES (NEWID(), 'Brian', 'Hansen', 'Smirked', 'WA', 6969, 300, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)") 
#row = cursor.fetchone()

#while row: 
#    print ('Inserted Player ID is ' + str(row[0])) 
#    row = cursor.fetchone()


#cursor.execute("SELECT * FROM Players")
#row = cursor.fetchone()
#while row:
#	print(row)
#	row = cursor.fetchone()



#data_dictionary = create_data_for_db.create_data_for_database_entry("tony-town-ii-swag-me-out", "melee-singles")
#players = data_dictionary["players"]
#for player in players:
#	print (player)
#	query_string = """INSERT into Players (Id, FirstName, LastName, Tag, State, Trueskill, SggPlayerId, CreatedAt, UpdatedAt) OUTPUT INSERTED.Id VALUES (NEWID(), "%s", "%s", "%s", "%s", 2500, %d, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""" % (player["fname"], player["lname"], player["tag"], player["state"], player["sgg_player_id"])
#	print (query_string)
#	cursor.execute(query_string)
#cnxn.commit()
#cnxn.close()