import os
import sys
#file = "C:/Users/hartlemi/Desktop/sgg_tournaments (1).txt"
import add_tournament




def getfile(filename, results):
	f = open(filename)
	filecontents = f.readlines()
	for line in filecontents:
		foo = line.strip('\n')
		results.append(foo)
	return results

tournaments = []
getfile(sys.argv[1], tournaments)

for tournament in tournaments:
	add_tournament.main(str(tournament), sys.argv[2])