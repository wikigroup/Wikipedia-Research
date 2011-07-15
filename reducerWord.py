#!/usr/bin/env python
'''
Python Version 2.6.1
This is our word count reducer. It collects all the word counts per page and aggregates them together. At the end, we print out "word \t total word cout \t how many pages the word appeared in".
'''

import sys,time
from collections import defaultdict

def emptyTwoIntTuple():
    # this stores [total word count, number of pages word is found in]
	return [0,0]

wordCount = defaultdict(emptyTwoIntTuple)

for line in sys.stdin:
    line = line.strip()
    
    word, count = line.split('\t', 1)
    
    try:
        count = int(count)
    except ValueError:
        continue
    
    wordCount[word][0] += count
    wordCount[word][1] += 1
    
for word in sorted(wordCount.keys()):
    print "%s\t%i\t%i"%(word,wordCount[word][0],wordCount[word][1])
    