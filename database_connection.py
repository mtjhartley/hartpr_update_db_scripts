import pyodbc
import struct
import uuid
import create_data_for_db


#ESTABLISH CONNECTION AND HELPER METHODS TO READ DATA IN PYTHON
conn_str = (
    r'Driver={ODBC Driver 17 for SQL Server};'
    r'Server=(localdb)\MSSQLLocalDB;'
    r'Database=HartPRDB;'
    r'Trusted_Connection=yes;'
	r'QuotedID=NO;'
    )
cnxn = pyodbc.connect(conn_str)


def handle_datetimeoffset(dto_value):
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)

cnxn.add_output_converter(-155, handle_datetimeoffset)
cursor = cnxn.cursor()

#GET THE DATA FROM THE SGG API
data_dictionary = create_data_for_db.create_data_for_database_entry("tony-hut-jr", "melee-singles")
players = data_dictionary["players"]
tournament = data_dictionary["tournament"]
sets = data_dictionary["sets"]

#CREATE LISTS FROM THE DATABASE TO DETERMINE HOW TO ENTER DATA

#Get a list of all the players sgg ids
#TODO turn this into a function
player_sgg_ids_to_guids_from_db_map = {}

cursor.execute("SELECT SggPlayerId, Id FROM Players")
row = cursor.fetchone()
while row:
	player_sgg_ids_to_guids_from_db_map.update( {str(row[0]): row[1] } )
	row = cursor.fetchone()
print (player_sgg_ids_to_guids_from_db_map)

#Check if this tournament already exists in the database, by the sgg_tournament_id
tournament_id_from_database = None

cursor.execute("SELECT SggTournamentId from TOURNAMENTS where SggTournamentId = %d" % tournament["sgg_tournament_id"])
row = cursor.fetchone()
if (row):
	#end it all
	print ("this tournament exists already, ending application")
else:
	print ("adding this tournament")
	cursor.execute("""INSERT into Tournaments (Id, Name, URL, Date, CreatedAt, UpdatedAt, SggTournamentId, Website) OUTPUT INSERTED.Id VALUES (NEWID(), "%s", "%s", "%s", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "%d", "%s")""" % (tournament["name"], tournament["url"], tournament["date"], tournament["sgg_tournament_id"], tournament["website"]))
	row = cursor.fetchone()
	tournament_id_from_database = str(row[0])



	
#Under the ELSE, or some boolean condition, insert/update the players conditionally if they already exist!

player_update_query_string = """UPDATE Players SET FirstName = "%s", LastName = "%s", Tag = "%s", State = "%s", UpdatedAt = CURRENT_TIMESTAMP WHERE SggPlayerId = %d """
player_insert_query_string = """INSERT into Players (Id, FirstName, LastName, Tag, State, Trueskill, SggPlayerId, CreatedAt, UpdatedAt) OUTPUT INSERTED.Id VALUES (NEWID(), "%s", "%s", "%s", "%s", 2500, %d, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""" 


for player in players:
	if player["sgg_player_id"] in player_sgg_ids_to_guids_from_db_map.keys():
		update_query_with_params = player_update_query_string % (player["fname"], player["lname"], player["tag"], player["state"], player["sgg_player_id"])
		print (update_query_with_params)
		cursor.execute(update_query_with_params)
	else:
		insert_query_with_params = player_insert_query_string % (player["fname"], player["lname"], player["tag"], player["state"], player["sgg_player_id"])
		print (insert_query_with_params)
		cursor.execute(insert_query_with_params)
		row = cursor.fetchone()
		player_sgg_ids_to_guids_from_db_map.update( {str(player["sgg_player_id"]): str(row[0])} )

set_insert_query_string = """INSERT into Sets (Id, WinnerId, LoserId, CreatedAt, UpdatedAt, TournamentId) VALUES (NEWID(), "%s", "%s", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "%s")""" 

print (type(sets[0]["sgg_winner_id"]))
print (player_sgg_ids_to_guids_from_db_map.keys())
for set in sets:
	if str(set["sgg_winner_id"]) in player_sgg_ids_to_guids_from_db_map.keys():
		set["database_winner_id"] = player_sgg_ids_to_guids_from_db_map[str(set["sgg_winner_id"])]
	if str(set["sgg_loser_id"]) in player_sgg_ids_to_guids_from_db_map.keys():
		set["database_loser_id"] = player_sgg_ids_to_guids_from_db_map[str(set["sgg_loser_id"])]
	insert_query_with_params = set_insert_query_string % (set["database_winner_id"], set["database_loser_id"], tournament_id_from_database)
	cursor.execute(insert_query_with_params)
	
cnxn.commit()
cnxn.close()






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