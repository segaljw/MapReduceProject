#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: tarodz
"""

from GoogleMapReduce import GoogleMapReduce

#custom map function
def urlCountMap(key,value):
	print('in_map input: key =',key,' value =',value)
	ret=[];
	urlVisitList=value.split(';')
	for urlTime in urlVisitList:
		urlTimeList=urlTime.split(',')
		ret.append((urlTimeList[0],1))						
	print('in_map output:')
	print(ret)
	return ret;

#custom reduce function
def urlCountReduce(key,values):
	print('in_reduce input: key =',key)
	print('in_reduce input: values =')
	retCount=0;
	for value in values:
		print(value)
		retCount+=value
	reduceResult=[(key,retCount)];
	print('in_reduce output:')
	print(reduceResult)
	return reduceResult

def main():
	log1=('log1.txt', 'abc.com,12:30pm;google.com,1:12pm;google.com,2:34pm;bing.com,4:34pm')
	log2=('log2.txt', 'google.com,11:12pm;abc.com,12:34pm;bing.com,2:22am;bing.com,1:11pm')
	log3=('log3.txt', 'bing.com,1:14am')
	logsAll = (log1,log2,log3)
	print(">>>INPUT 2D TUPLE:")
	print(logsAll)
	gmr=GoogleMapReduce();
	print(">>>RUNNING MAP:")
	gmr.googleMap(logsAll,(0,),(1,),urlCountMap)
	print("-|-DONE WITH MAP")
	print(">>>INSPECTING INTERMEDIATE RESULTS")
	mapCopy=gmr.getMappedCopy();
	print('mapCopy')
	print(mapCopy)
	groupCopy=gmr.getGroupedByCopy();
	print('groupCopy')
	print(groupCopy)
	print("-|-DONE WITH INSPECTING INTERMEDIATE RESULTS")
	print(">>>RUNNING REDUCE:")
	urlCounts=gmr.googleReduce(urlCountReduce)
	print("-|-DONE WITH REDUCE")
	print('>>>FINAL RESULT:')
	print('returned urlCount: ')
	print(urlCounts)

if __name__ == '__main__':
	main()
#produces the following output
'''
>>>INPUT 2D TUPLE:
(('log1.txt', 'abc.com,12:30pm;google.com,1:12pm;google.com,2:34pm;bing.com,4:34pm'), ('log2.txt', 'google.com,11:12pm;abc.com,12:34pm;bing.com,2:22am;bing.com,1:11pm'), ('log3.txt', 'bing.com,1:14am'))
>>>RUNNING MAP:
in_map input: key = log1.txt  value = abc.com,12:30pm;google.com,1:12pm;google.com,2:34pm;bing.com,4:34pm
in_map output:
[('abc.com', 1), ('google.com', 1), ('google.com', 1), ('bing.com', 1)]
in_map input: key = log2.txt  value = google.com,11:12pm;abc.com,12:34pm;bing.com,2:22am;bing.com,1:11pm
in_map output:
[('google.com', 1), ('abc.com', 1), ('bing.com', 1), ('bing.com', 1)]
in_map input: key = log3.txt  value = bing.com,1:14am
in_map output:
[('bing.com', 1)]
-|-DONE WITH MAP
>>>INSPECTING INTERMEDIATE RESULTS
mapCopy
[('abc.com', 1), ('abc.com', 1), ('bing.com', 1), ('bing.com', 1), ('bing.com', 1), ('bing.com', 1), ('google.com', 1), ('google.com', 1), ('google.com', 1)]
groupCopy
[('abc.com', [1, 1]), ('bing.com', [1, 1, 1, 1]), ('google.com', [1, 1, 1])]
-|-DONE WITH INSPECTING INTERMEDIATE RESULTS
>>>RUNNING REDUCE:
in_reduce input: key = abc.com
in_reduce input: values =
1
1
in_reduce output:
[('abc.com', 2)]
in_reduce input: key = bing.com
in_reduce input: values =
1
1
1
1
in_reduce output:
[('bing.com', 4)]
in_reduce input: key = google.com
in_reduce input: values =
1
1
1
in_reduce output:
[('google.com', 3)]
-|-DONE WITH REDUCE
>>>FINAL RESULT:
returned urlCount: 
[('abc.com', 2), ('bing.com', 4), ('google.com', 3)]
'''