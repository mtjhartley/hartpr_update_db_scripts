import pyodbc
import struct
import trueskill
import database_connector

global_environment = trueskill.TrueSkill(draw_probability = 0)

defaultRating = trueskill.Rating(25)
#TODO: Change this to include where Tournamenet ID in (select statement from tournament tables for tournaments with specific game id i.e. melee, smash5)
trueskill_history_delete_query = ("""DELETE from TrueskillHistories""")
trueskill_history_insert_query = ("""INSERT INTO TrueskillHistories (Id, PlayerId, Trueskill, TournamentId) values (NEWID(), "%s", %0.2f, "%s")""")
trueskill_player_update_query = ("""UPDATE PLAYERS set Trueskill = %0.2f, LastActive = "%s" where Id = "%s" """)

def delete_trueskill_history_from_database(crsr):
	#maybe catch an exception if somethign fails?
	crsr.execute(trueskill_history_delete_query)
	return True

def create_player_map_with_trueskill(crsr):
	player_to_trueskill_map = {} 
	#key - id value (mu, sigma)
	crsr.execute("SELECT ID FROM Players")
	row = crsr.fetchone()
	while row:
		player_to_trueskill_map.update( {str(row[0]): defaultRating } )
		row = crsr.fetchone()

	return player_to_trueskill_map

def create_list_of_tournaments(crsr):
	ordered_tournaments = []
	crsr.execute("SELECT ID, Name, Date from Tournaments Order By Date")
	row = crsr.fetchone()
	while row:
		tournament_map = {}
		tournament_map.update({"id": row[0], "name": row[1], "date": row[2]})
		ordered_tournaments.append(tournament_map)
		row = crsr.fetchone()

	return ordered_tournaments

def create_set_list_of_set_maps_for_tournament(crsr, tournament_map):
	sets_list = []
	#key = set_id, value (winner, loser)
	crsr.execute("""SELECT ID, WinnerID, LoserID from sets WHERE LoserScore != -1 and WinnerId != '00000000-0000-0000-0000-000000000000' and LoserId != '00000000-0000-0000-0000-000000000000' and TournamentId = "%s" """ % (str(tournament_map["id"]))) #dodge DQ and inability to find player in the entrants api call
	row = crsr.fetchone()
	while row:
		set_map = {}
		set_map.update( {"winner_id": str(row[1]), "loser_id": str(row[2])})
		sets_list.append(set_map)
		row = crsr.fetchone()
	return sets_list

def calculate_all_trueskills(player_to_trueskill_map, sets_list):
    print ("Beginning Trueskill Calculation for sets in tournament...")
    for set in sets_list:
        winner_id = set["winner_id"]
        winner_trueskill = player_to_trueskill_map[winner_id]
        loser_id= set["loser_id"]
        loser_trueskill = player_to_trueskill_map[loser_id]

        winner_trueskill, loser_trueskill = trueskill.rate_1vs1(winner_trueskill, loser_trueskill)

        player_to_trueskill_map[winner_id] = winner_trueskill
        player_to_trueskill_map[loser_id] = loser_trueskill
    print ("Finished Trueskill Calculation for sets in tournament!")

def create_all_players_trueskill_history_map(crsr, player_to_trueskill_map, tournaments):

	all_players_trueskill_history = {}
	print ("Initializing map...")
	for player in player_to_trueskill_map.keys():
		player_map = {}
		player_map.update({player: []}) #player: [] the empty array will hold the trueskill histories.
		all_players_trueskill_history.update({player: player_map})
	
	print ("Beginning iteration through tournaments...")

	for tournament in tournaments[4:]: #TODO CHANGE WHEN REMOVING DUMMY DATA FROM DB, ACTUALLY IT'S FINE LOL
		print ("Creating history for tournament: {}".format(tournament["name"]))
		sets = create_set_list_of_set_maps_for_tournament(crsr, tournament)
		calculate_all_trueskills(player_to_trueskill_map, sets)

		for key, value in player_to_trueskill_map.items():
			mock_database_map = {}
			mock_database_map.update({"PlayerId": key, "Trueskill": value, "TournamentId": tournament["id"], "Date": tournament["date"].split(" ")[0] })
			if not all_players_trueskill_history[key][key]:
				all_players_trueskill_history[key][key].append(mock_database_map)
			#if the trueskill for this tournament is the same as the previous, we can ASSUME that means they didn't enter. Sigma and MU staying the same is so unlikely but...
			#could also get the list of players in each tournament from the sets, and then confirm for sure...
			elif value != all_players_trueskill_history[key][key][len(all_players_trueskill_history[key][key])-1]["Trueskill"]:
				all_players_trueskill_history[key][key].append(mock_database_map)
	#print (all_players_trueskill_history['BB33FE5F-D998-4833-9A4F-A31A938E9D78'])
	#print (len(all_players_trueskill_history['BB33FE5F-D998-4833-9A4F-A31A938E9D78']['BB33FE5F-D998-4833-9A4F-A31A938E9D78']))
	print ("Created the entire history for every player in the database! Wow!!!")
	return all_players_trueskill_history

def update_database_with_trueskill_histores(crsr, all_players_trueskill_history):
	for player in all_players_trueskill_history:
		#
		#print (all_players_trueskill_history[player][player])
		for set in all_players_trueskill_history[player][player]:
			#print (set)
			set["Trueskill"] = (float(set["Trueskill"].mu) - float(3.00 * set["Trueskill"].sigma)) * 100.00
			print (set["Trueskill"])
			insert_query_with_params = trueskill_history_insert_query % (set["PlayerId"], set["Trueskill"], set["TournamentId"])
			print (insert_query_with_params)
			crsr.execute(insert_query_with_params)
		#use the most recent set to update the player table
		most_recent_trueskill = all_players_trueskill_history[player][player][-1]
		update_query_with_params = trueskill_player_update_query % (most_recent_trueskill["Trueskill"], most_recent_trueskill["Date"], most_recent_trueskill["PlayerId"] )
		crsr.execute(update_query_with_params)

	print ("All histories added to the database.")



			
def main():
	cnxn = database_connector.create_connection(database_connector.CONNECTION_STRING)
	cnxn.add_output_converter(-155, database_connector.handle_datetimeoffset)
	cursor = cnxn.cursor()

	delete_trueskill_history_from_database(cursor)

	player_to_trueskill_map = create_player_map_with_trueskill(cursor)
	tournaments = create_list_of_tournaments(cursor)
	
	all_players_trueskill_history_map = create_all_players_trueskill_history_map(cursor, player_to_trueskill_map, tournaments)
	#print (all_players_trueskill_history_map['BB33FE5F-D998-4833-9A4F-A31A938E9D78']['BB33FE5F-D998-4833-9A4F-A31A938E9D78'])
	#print (len(all_players_trueskill_history_map['BB33FE5F-D998-4833-9A4F-A31A938E9D78']['BB33FE5F-D998-4833-9A4F-A31A938E9D78']))
	#print (all_players_trueskill_history_map.keys())
	#print (len(all_players_trueskill_history_map.keys()))
	update_database_with_trueskill_histores(cursor, all_players_trueskill_history_map)

	#a list of maps , each map is a playerid key and a value of a list of these maps.
	print ("All changes staged!")
	
	cnxn.commit()
	print ("Commited changes to database! Thanks for helping out :)")
	cnxn.close()







	"""
	SCHEMA for all_players_trueskill_history_map
	{
		{
			ABCDEFG-FDSFD-SDFSDF-SDF-SDFDFDFDS: [
													{
														PlayerId: ABCDEFG-FDSFD-SDFSDF-SDF-SDFDFDFDS,
														Trueskill: 4200,
														Tournament: "Tony Town 2",
														Date: 2016-04-04
													},
													{
														PlayerId: ABCDEFG-FDSFD-SDFSDF-SDF-SDFDFDFDS,
														Trueskill: 32,
														Tournament: "Tony Town 3",
														Date: 2016-04-04
													},
												]
		},
		{
			BBBBBBB-FBBBDSFD-SDFSDF-SDF-SDFDFDFDS: [
													{
														PlayerId: BBBBBBB-FBBBDSFD-SDFSDF-SDF-SDFDFDFDS,
														Trueskill: 1234,
														Tournament: "Tony Town",
														Date: 2016-04-04
													},
													{
														PlayerId: ABCDEFG-FDSFD-SDFSDF-SDF-SDFDFDFDS,
														Trueskill: 4200,
														Tournament: "Tony Town 4",
														Date: 2016-04-04
													},
												]
		},
	}
	"""

	"""
	select players
FROM (
	SELECT WinnerID
	from sets
	WHERE TournamentId ='68B0CD21-7758-4F31-BE8D-93966B5412ED'
	UNION
	SELECT LoserId
	from sets
	WHERE TournamentId = '68B0CD21-7758-4F31-BE8D-93966B5412ED'
) AS DistinctCodes (players)

SELECT * FROM PLAYERS 
WHERE LastActive >= DATEADD(month,-6,GETDATE()) and state = 'wa'
order by Trueskill desc
"""


main()