import pyodbc
import struct
import trueskill
import database_connector

global_environment = trueskill.TrueSkill(draw_probability = 0)

defaultRating = trueskill.Rating(25)


def create_player_map_with_trueskill(crsr):
	player_to_trueskill_map = {} 
	#key - id value (mu, sigma)
	crsr.execute("SELECT ID FROM Players")
	row = crsr.fetchone()
	while row:
		player_to_trueskill_map.update( {str(row[0]): defaultRating } )
		row = crsr.fetchone()

	return player_to_trueskill_map

def create_set_list_of_set_maps(crsr):
	sets_list = []
	#key = set_id, value (winner, loser)
	crsr.execute("SELECT ID, WinnerID, LoserID from sets WHERE LoserScore != -1 and WinnerId != '00000000-0000-0000-0000-000000000000' and LoserId != '00000000-0000-0000-0000-000000000000'") #dodge DQ and inability to find player in the entrants api call
	row = crsr.fetchone()
	while row:
		set_map = {}
		set_map.update( {"winner_id": str(row[1]), "loser_id": str(row[2])})
		sets_list.append(set_map)
		row = crsr.fetchone()
	return sets_list

def calculate_all_trueskills(player_to_trueskill_map, sets_list):
    for set in sets_list:
        winner_id = set["winner_id"]
        winner_trueskill = player_to_trueskill_map[winner_id]
        loser_id= set["loser_id"]
        loser_trueskill = player_to_trueskill_map[loser_id]

        winner_trueskill, loser_trueskill = trueskill.rate_1vs1(winner_trueskill, loser_trueskill)

        player_to_trueskill_map[winner_id] = winner_trueskill
        player_to_trueskill_map[loser_id] = loser_trueskill

def update_database_with_trueskills(crsr, player_to_trueskill_map):
	update_player_query_string = player_update_query_string = """UPDATE Players SET Trueskill = %d WHERE ID = "%s" """
	for player_id in player_to_trueskill_map.keys():
		player_trueskill = float(player_to_trueskill_map[player_id].mu) - float(3.0 * player_to_trueskill_map[player_id].sigma)
		print (type(player_trueskill))
		player_trueskill = int(player_trueskill * 100.0)
		update_query_with_params = update_player_query_string % (player_trueskill, player_id)
		crsr.execute(update_query_with_params)
		print ("Updated player %s with a trueskill of %d" % (player_id, player_trueskill))



def main():
	cnxn = database_connector.create_connection(database_connector.CONNECTION_STRING)
	cnxn.add_output_converter(-155, database_connector.handle_datetimeoffset)
	cursor = cnxn.cursor()

	player_to_trueskill_map = create_player_map_with_trueskill(cursor)

	sets_list = create_set_list_of_set_maps(cursor)

	calculate_all_trueskills(player_to_trueskill_map, sets_list)

	#print (player_to_trueskill_map)

	#for key, value in sorted(player_to_trueskill_map.items(), key=lambda item:(item[1].mu, item[1].sigma), reverse = True):
	#	print ("%s: %s" % (key, value))

	update_database_with_trueskills(cursor, player_to_trueskill_map)
	
	cnxn.commit()
	print ("Trueskill has been calculated. Thanks!")
	cnxn.close()

main()