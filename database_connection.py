import pyodbc
import struct
import uuid
import create_data_for_db

player_update_query_string = """UPDATE Players SET FirstName = "%s", LastName = "%s", Tag = "%s", State = "%s", UpdatedAt = CURRENT_TIMESTAMP WHERE SggPlayerId = %d """
player_insert_query_string = """INSERT into Players (Id, FirstName, LastName, Tag, State, Trueskill, SggPlayerId, CreatedAt, UpdatedAt) OUTPUT INSERTED.Id VALUES (NEWID(), "%s", "%s", "%s", "%s", 2500, %d, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""" 
set_insert_query_string = """INSERT into Sets (Id, WinnerId, LoserId, CreatedAt, UpdatedAt, TournamentId) VALUES (NEWID(), "%s", "%s", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "%s")""" 


#Function to create connection to pyodbc
def create_connection(connection_string):
	print ("Attempting to establish connection to database...")
	return pyodbc.connect(connection_string)

#To handle DATETIMEOFFSET column in MSSQL db
def handle_datetimeoffset(dto_value):
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]

    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)

#Get a map of SGG Player Id to Player GUID from db
def get_player_sgg_ids_to_guids_from_db_map(crsr):
	player_sgg_ids_to_guids_from_db_map = {}

	crsr.execute("SELECT SggPlayerId, Id FROM Players")
	row = crsr.fetchone()
	while row:
		player_sgg_ids_to_guids_from_db_map.update( {str(row[0]): row[1] } )
		row = crsr.fetchone()

	return player_sgg_ids_to_guids_from_db_map

#Return a boolean and use it to end the main method if the tournament is already in the DB
def does_tournament_exist(crsr, tournament):
	print ("Checking if tournament exists in the database already...")
	tournament_id_from_database = None

	crsr.execute("SELECT SggTournamentId from TOURNAMENTS where SggTournamentId = %d" % tournament["sgg_tournament_id"])
	row = crsr.fetchone()
	if (row):
		print ("This tournament exists already, ending application")
		return True
	print ("This is a new tournament! Beginning process to add Tournament, Players, and Sets...")
	return False

#Process to add tournament to DB
def add_tournament_to_db(crsr, tournament):
	print ("Adding %s to Database" % tournament["name"])
	crsr.execute("""INSERT into Tournaments (Id, Name, URL, Date, CreatedAt, UpdatedAt, SggTournamentId, Website) OUTPUT INSERTED.Id VALUES (NEWID(), "%s", "%s", "%s", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "%d", "%s")""" % (tournament["name"], tournament["url"], tournament["date"], tournament["sgg_tournament_id"], tournament["website"]))
	row = crsr.fetchone()
	tournament_id_from_database = str(row[0])
	return tournament_id_from_database

#Process to add players to DB
def add_or_update_players_to_db(crsr, players, player_sgg_ids_to_guids_from_db_map):
	print ("Beginning to add players to the database. If the players already exist, they will be updated with their latest information from smashgg.")
	player_added_count = 0
	player_updated_count = 0

	for player in players:
		if str(player["sgg_player_id"]) in player_sgg_ids_to_guids_from_db_map.keys():
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
		if str(set["sgg_loser_id"]) in player_sgg_ids_to_guids_from_db_map.keys():
			set["database_loser_id"] = player_sgg_ids_to_guids_from_db_map[str(set["sgg_loser_id"])]
		insert_query_with_params = set_insert_query_string % (set["database_winner_id"], set["database_loser_id"], tournament_id_from_database)
		crsr.execute(insert_query_with_params)
	print ("Sets successfully added to the database!")


def main():
	tournament_exists = False

	cnxn = create_connection(r'Driver={ODBC Driver 17 for SQL Server};'
	r'Server=(localdb)\MSSQLLocalDB;'
	r'Database=HartPRDB;'
	r'Trusted_Connection=yes;'
	r'QuotedID=NO;'
	)
	cnxn.add_output_converter(-155, handle_datetimeoffset)
	cursor = cnxn.cursor()

	data_dictionary = create_data_for_db.create_data_for_database_entry("emerald-city-v", "melee-singles")
	players = data_dictionary["players"]
	tournament = data_dictionary["tournament"]
	sets = data_dictionary["sets"]

	tournament_exists = does_tournament_exist(cursor, tournament)
	if (tournament_exists):
		return #end the app lmao
	new_tournament_guid = add_tournament_to_db(cursor, tournament)

	player_sgg_ids_to_guids_from_db_map = get_player_sgg_ids_to_guids_from_db_map(cursor)

	add_or_update_players_to_db(cursor, players, player_sgg_ids_to_guids_from_db_map)

	add_sets_to_db(cursor, sets, player_sgg_ids_to_guids_from_db_map, new_tournament_guid)

	print ("All changes are ready to be staged!")
	
	cnxn.commit()
	print ("Commited changes to database! Thanks for helping out :)")
	cnxn.close()

main()

#TODO: Add RAW INPUT LMAO for tournament slug




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