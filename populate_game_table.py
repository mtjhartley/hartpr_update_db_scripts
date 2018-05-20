import pyodbc
import struct
from datetime import datetime as dt
import uuid
import sys
import create_data_for_db
import database_connector

class Game:
	def __init__(self, guid, enum, name, event):
		self.id = guid
		self.enum = enum
		self.name = name
		self.event = event

melee = Game("8FA6C1F8-B06F-4020-A154-3A88260515A4", 0, "Melee", "melee-singles")
smash4 = Game("1F52BB15-DFEF-4FD3-9C0A-E3F8260F9A1C", 1, "Smash4", "wii-u-singles")
pm = Game("482E6798-489F-41EC-98EF-44432DF61AD8", 2, "PM", "project-m-singles")
#ONLY EVER ADD TO THE BOTTOM OF THIS ARRAY
games = [
	melee,
	smash4,
	pm
	#add next game here!
	]


def main():

	cnxn = database_connector.create_connection(database_connector.CONNECTION_STRING)
	cnxn.add_output_converter(-155, database_connector.handle_datetimeoffset)
	cursor = cnxn.cursor()

	cursor.execute("DELETE FROM Games")

	for game in games:
		game_insert_query_string = f"""INSERT INTO Games (Id, Enum, Name, Event) Values ("{game.id}", {game.enum}, "{game.name}", "{game.event}")"""
		print(game_insert_query_string) 
		cursor.execute(game_insert_query_string)

	cnxn.commit()
	print ("Commited changes to database! Thanks for helping out :)")
	cnxn.close()

main()