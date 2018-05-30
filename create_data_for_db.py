import pysmash
import datetime
import urllib, json
import sys

smash = pysmash.SmashGG()

def create_data_for_database_entry(tourney_name, event_name):
	print ("Creating data dictionary from smashgg API with the tournament slug: '%s'" % tourney_name)
	data_dictionary = {}

	brackets = smash.tournament_show_event_brackets(tourney_name, event_name)
	melee_singles_bracket_ids = brackets["bracket_ids"]

	entrant_id_to_sgg_player_id_map = map_entrant_id_to_sgg_player_id(melee_singles_bracket_ids)

	tournament_from_api = smash.tournament_show_with_brackets(tourney_name, event_name)
	tournament = create_tournament_entity(tournament_from_api, event_name)

	sets = get_sets(melee_singles_bracket_ids)
	update_sets_with_sgg_ids(sets, entrant_id_to_sgg_player_id_map, tournament)

	players = smash.tournament_show_players(tourney_name, event_name)
	update_players_with_sgg_player_id(players, entrant_id_to_sgg_player_id_map, tournament)

	data_dictionary.update({"tournament": tournament, "sets": sets, "players": players})

	print ("Data dictionary successfully created!")
	return data_dictionary



#Get all of the sets from a tournament and flatten them
def get_sets(bracket_ids):
	unflattened_sets = []
	#returns a list of lists, each list containing sets for corresponding bracket_id
	for bracket_id in bracket_ids:
		unflattened_sets.append(smash.bracket_show_sets(bracket_id))

	sets = [s for set_list in unflattened_sets for s in set_list] 

	return sets

#Create a dictioanry mapping entrant id to player id
def map_entrant_id_to_sgg_player_id(bracket_ids):
	entrant_id_to_sgg_player_id = {}
	for bracket_id in bracket_ids:
		url = "https://api.smash.gg/phase_group/{0}{1}".format(bracket_id, "?expand[]=entrants")

		with urllib.request.urlopen(url) as response:
			attendees = json.loads(response.read())

		for player in attendees['entities']['player']:
			sgg_player_id = player["id"]
			entrant_id = player["entrantId"]
			entrant_id_to_sgg_player_id[entrant_id] = sgg_player_id

	return entrant_id_to_sgg_player_id

#Update each set with the sgg winner id, sgg loser id, and sgg tournament id
def update_sets_with_sgg_ids(sets_from_api, entrant_id_to_sgg_player_id_map, tournament):
	for set in sets_from_api:
		if set["winner_id"] in entrant_id_to_sgg_player_id_map.keys():
			set["sgg_winner_id"] = entrant_id_to_sgg_player_id_map[set["winner_id"]]
		if set["loser_id"] in entrant_id_to_sgg_player_id_map.keys():
			set["sgg_loser_id"] = entrant_id_to_sgg_player_id_map[set["loser_id"]]
		set["sgg_tournament_id"] = tournament["sgg_tournament_id"]

		if set["entrant_1_id"] == set["winner_id"]:
			if set["entrant_1_score"] != None and set["entrant_2_score"] != None:
				set["winner_score"] = int(set["entrant_1_score"])
				set["loser_score"] = int(set["entrant_2_score"])
			else:
				set["winner_score"] = None
				set["loser_score"] = None
		else:
			if set["entrant_1_score"] != None and set["entrant_2_score"] != None:
				set["winner_score"] = int(set["entrant_2_score"])
				set["loser_score"] = int(set["entrant_1_score"])
			else:
				set["winner_score"] = None
				set["loser_score"] = None
	return [s for s in sets_from_api if (s["entrant_1_score"] != -1 or s["entrant_2_score"] != -1)]
#Update players with sgg player ids and active date
def update_players_with_sgg_player_id(players_from_api, entrant_id_to_sgg_player_id_map, tournament):
	for player in players_from_api:
		if str(player["entrant_id"]) in entrant_id_to_sgg_player_id_map.keys():
			player["sgg_player_id"] = entrant_id_to_sgg_player_id_map[str(player["entrant_id"])]
		player["last_active"] = tournament["date"]

	return players_from_api

#Create tournament entity from api
def create_tournament_entity(tournament_from_api, event_name):
	tournament = {}
	tournament["name"] = tournament_from_api["name"]
	tournament["website"] = "smashgg"
	tournament["url"] = tournament_from_api["tournament_full_source_url"].split('/')[-1]
	#format date of tournament from unix time to acceptable dt
	start_date_unix = tournament_from_api['start_at']
	start_date_object = datetime.date.fromtimestamp(start_date_unix)
	start_date = start_date_object.isoformat()
	tournament["date"] = start_date
	tournament["sgg_tournament_id"] = tournament_from_api["tournament_id"]
	tournament["event_name"] = event_name
	return tournament

#create_data_for_database_entry("tony-town-ii-swag-me-out", "melee-singles")

if __name__ == "__main__":
	#if running this script directly, you need to supply the tournament slug and the event name
	#this should never be done, use the game specific scripts.
	create_data_for_database_entry("emerald-city-6", "melee-singles")