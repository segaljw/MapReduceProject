import copy
from itertools import groupby
class GoogleMapReduce:

    def __init__(self):
        self._mappedSorted = []
        self._mappedGrouped = []

    def googleMap(self, input2Dtuple, keyIndices, valueIndecies, googleMapper):
        mapped = []
        for i in input2Dtuple:
            if len(keyIndices) > 1:
                k1 = (i[keyIndices[0], i[keyIndices[1]]])
            else:
                k1 = i[keyIndices[0]]
            if len(valueIndecies) > 1:
                k2 = (i[valueIndecies[0]], i[valueIndecies[1]])
            else:
                k2 = i[valueIndecies[0]]
            mapped.extend(googleMapper(k1, k2))

        def getKey(item):
            return item[0]

        sortedList = sorted(mapped, key=getKey)

        self._mappedSorted = sortedList

        grouped = []

        for k, group in groupby(sortedList, lambda x: x[0]):
            v2 = (thing[1] for thing in group)
            grouped.append((k,list(v2)))
        self._mappedGrouped = grouped

    def googleReduce(self, googleReducer):
        output = []
        for i in self._mappedGrouped:
            output.extend(googleReducer(i[0],i[1]))
        return output

    def getMappedCopy(self):
        shallowCopy = self._mappedSorted
        deepCopy = copy.deepcopy(shallowCopy)
        return deepCopy

    def getGroupedByCopy(self):
        shallowCopy = self._mappedGrouped
        deepCopy = copy.deepcopy(shallowCopy)
        return deepCopy
