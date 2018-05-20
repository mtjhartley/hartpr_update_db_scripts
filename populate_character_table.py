import pyodbc
import struct
from datetime import datetime as dt
import uuid
import sys
import create_data_for_db
import database_connector

#ONLY EVER ADD TO THE BOTTOM OF THIS ARRAY
characters = [
	"Sandbag",
	"Fox",
	"Falco",
	"Marth",
    "Sheik",
    "Jigglypuff",
    "Peach",
    "IceClimbers",
    "CaptainFalcon",
    "Pikachu",
    "Samus",
    "DrMario",
    "Yoshi",
    "Luigi",
    "Ganondorf",
    "Mario",
    "YoungLink",
    "DonkeyKong",
    "Link",
    "MrGameAndWatch",
    "Roy",
    "Mewtwo",
    "Zelda",
    "Ness",
    "Pichu",
    "Bowser",
    "Kirby"
	]


def main():

	cnxn = database_connector.create_connection(database_connector.CONNECTION_STRING)
	cnxn.add_output_converter(-155, database_connector.handle_datetimeoffset)
	cursor = cnxn.cursor()

	cursor.execute("DELETE FROM Characters")

	for id in range(len(characters)):
		character_insert_query_string = f"""INSERT INTO Characters (Id, Name) Values ({id}, "{characters[id]}")"""
		print(character_insert_query_string) 
		cursor.execute(character_insert_query_string)

	cnxn.commit()
	print ("Commited changes to database! Thanks for helping out :)")
	cnxn.close()

main()