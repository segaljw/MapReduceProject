#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: tarodz
"""

from GoogleMapReduce import GoogleMapReduce

#custom map function
def scrabbleMap(key,value):
	print('in_map input: key =',key,' value =',value)
	ret=[ (value[0],(value[1],key)) ]
	print('in_map output:')
	print(ret)
	return ret;

#custom reduce function
def scrabbleReduce(key,values):
	print('in_reduce input: key =',key)
	print('in_reduce input: values =')
	retScore=0;
	retTeam='';
	for value in values:
		print(value)
		score=value[0]
		name=value[1]
		retScore+=score;
		if (len(retTeam)>0):
			retTeam=retTeam+'&'+name;
		else:
			retTeam=name;
	reduceResult=[str(retScore)+' scored by '+retTeam+' '+key];
	print('in_reduce output:')
	print(reduceResult)
	return reduceResult

def main():
	#each row is: First Name, Second Name, Personal Score
	person1=('Jane', 'Doe',120)
	person2=('Sam', 'Smith', 100)
	person3=('Sammy', 'Smith',110)
	person4=('John', 'Doe',130)
	familyScrabble = (person1,person2,person3,person4 )
	print(">>>INPUT 2D TUPLE (First, Last, Personal Score):")
	print(familyScrabble)
	gmr=GoogleMapReduce();
	print(">>>RUNNING MAP:")
	gmr.googleMap(familyScrabble,(0,),(1,2),scrabbleMap)
	print("-|-DONE WITH MAP")
	print(">>>TESTING COPY OF INTERMEDIATE RESULTS")
	mapCopy=gmr.getMappedCopy();
	print('mapCopy')
	print(mapCopy)
	mapCopy[0]=('NEWMAN',(1000,'JOE'))
	print('mapCopy modified')
	print(mapCopy)
	mapCopy=gmr.getMappedCopy();
	print('mapCopy obtained again, no modification present')
	print(mapCopy)
	groupCopy=gmr.getGroupedByCopy();
	print('groupCopy')
	print(groupCopy)
	groupCopy[0]=('NEWMAN',[(1000,'JOE'),(2000,'JACKIE')]);
	print('groupCopy modified')
	print(groupCopy)
	groupCopy=gmr.getGroupedByCopy();
	print('groupCopy obtained again, no modification present')
	print(groupCopy)
	print("-|-DONE WITH TESTING DEEP COPY")
	print(">>>RUNNING REDUCE:")
	familyScores=gmr.googleReduce(scrabbleReduce)
	print("-|-DONE WITH REDUCE")
	print('>>>FINAL RESULT:')
	print('returned familyScores: ')
	print(familyScores)

if __name__ == '__main__':
	main()
#produces the following output
'''
>>>INPUT 2D TUPLE (First, Last, Personal Score):
(('Jane', 'Doe', 120), ('Sam', 'Smith', 100), ('Sammy', 'Smith', 110), ('John', 'Doe', 130))
>>>RUNNING MAP:
in_map input: key = Jane  value = ['Doe', 120]
in_map output:
[('Doe', (120, 'Jane'))]
in_map input: key = Sam  value = ['Smith', 100]
in_map output:
[('Smith', (100, 'Sam'))]
in_map input: key = Sammy  value = ['Smith', 110]
in_map output:
[('Smith', (110, 'Sammy'))]
in_map input: key = John  value = ['Doe', 130]
in_map output:
[('Doe', (130, 'John'))]
-|-DONE WITH MAP
>>>TESTING COPY OF INTERMEDIATE RESULTS
mapCopy
[('Doe', (120, 'Jane')), ('Doe', (130, 'John')), ('Smith', (100, 'Sam')), ('Smith', (110, 'Sammy'))]
mapCopy modified
[('NEWMAN', (1000, 'JOE')), ('Doe', (130, 'John')), ('Smith', (100, 'Sam')), ('Smith', (110, 'Sammy'))]
mapCopy obtained again, no modification present
[('Doe', (120, 'Jane')), ('Doe', (130, 'John')), ('Smith', (100, 'Sam')), ('Smith', (110, 'Sammy'))]
groupCopy
[('Doe', [(120, 'Jane'), (130, 'John')]), ('Smith', [(100, 'Sam'), (110, 'Sammy')])]
groupCopy modified
[('NEWMAN', [(1000, 'JOE'), (2000, 'JACKIE')]), ('Smith', [(100, 'Sam'), (110, 'Sammy')])]
groupCopy obtained again, no modification present
[('Doe', [(120, 'Jane'), (130, 'John')]), ('Smith', [(100, 'Sam'), (110, 'Sammy')])]
-|-DONE WITH TESTING DEEP COPY
>>>RUNNING REDUCE:
in_reduce input: key = Doe
in_reduce input: values =
(120, 'Jane')
(130, 'John')
in_reduce output:
['250 scored by Jane&John Doe']
in_reduce input: key = Smith
in_reduce input: values =
(100, 'Sam')
(110, 'Sammy')
in_reduce output:
['210 scored by Sam&Sammy Smith']
-|-DONE WITH REDUCE
>>>FINAL RESULT:
returned familyScores: 
['250 scored by Jane&John Doe', '210 scored by Sam&Sammy Smith']
'''