import pyodbc
import struct
from datetime import datetime as dt
import uuid
import sys
import create_data_for_db
import database_connector


"""Calculate Trueskill History should be run after this script, always"""

def get_tournament_by_slug(crsr, tournament_slug):
	crsr.execute("""SELECT ID, Name FROM tournaments WHERE URL = "%s" """ % tournament_slug)
	row = crsr.fetchone()
	if row:
		return {"id": str(row[0]), "name": (str(row[1]))}
	else:
		return False

def delete_tournament_by_id(crsr, tournament):
	print ("""Deleting tournament "%s" from database""" % tournament["name"])
	id = tournament["id"]
	crsr.execute("""DELETE FROM tournaments WHERE Id = "%s" """ % id)
	print ("Tournament Deleted.")

def delete_sets_by_tournament_id(crsr, tournament):
	print ("""Deleting sets from tournament "%s" from database""" % tournament["name"])
	id = tournament["id"]
	crsr.execute("""DELETE FROM sets WHERE TournamentId = "%s" """ % id)
	print ("Sets Deleted.")

def delete_trueskill_history_by_tournament_id(crsr, tournament):
	print ("""Deleting Trueskill Histories with tournament "%s" from database""" % tournament["name"])
	id = tournament["id"]
	crsr.execute("""DELETE FROM TrueskillHistories WHERE TournamentId = "%s" """ % id)
	print ("Trueskill Histories Deleted.")

def main(tournament_slug):
	cnxn = database_connector.create_connection(database_connector.CONNECTION_STRING)
	cnxn.add_output_converter(-155, database_connector.handle_datetimeoffset)
	cursor = cnxn.cursor()

	tournament = get_tournament_by_slug(cursor, tournament_slug)
	if not tournament:
		print ("Couldn't find the tournament")
		return

	delete_tournament_by_id(cursor, tournament)
	delete_sets_by_tournament_id(cursor, tournament)
	delete_trueskill_history_by_tournament_id(cursor, tournament)

	print ("All changes staged!")
	
	cnxn.commit()
	print ("Deleted tournament from the database. Thanks for helping out :)")
	cnxn.close()

if __name__ == "__main__":
	main(sys.argv[1])