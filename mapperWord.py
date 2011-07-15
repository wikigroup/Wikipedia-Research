#!/usr/bin/env python
'''
Python Version 2.6.1
This is our mapper function which calculates word counts for individual pages. By using the StreamXmlRecordReader, we get a chunk of pages sent to the mapper. Since this is streaming, we print out each word and its count to send to the reducer.
'''
 
import sys
import xml.sax
import re
from collections import defaultdict
from operator import itemgetter

class PageHandler(xml.sax.handler.ContentHandler):
'''
This is our xml parser which splits up the pages based on tags
'''
    def __init__(self):
        self.buffer =[]
        self.inText = False
        self.inID = False
        self.inRevision = False
        self.currentPageID = None
        
    def startElement(self, name, attributes):
        if name == "text":
            self.inText = True
        elif name == "id":
            self.inID= True
        elif name == "revision":
            self.inRevision = True
        
    def characters(self, data):
        if (self.inID and not self.inRevision) or self.inText:
            self.buffer.append(data)
        
    def endElement(self, name):
        if name == "id":
            self.inID= False
            if not self.inRevision:
                self.currentPageID = "".join(self.buffer)
        elif name == "revision":
            self.inRevision = False
        elif name == "text":
            self.inText = False
            text = "".join(self.buffer)
            self.getWordCounts(text)
        self.buffer = []
    
    def getWordCounts(self,text):
    '''
    This function helps up clean up the text of the wikipedia pages. We want to remove citations and things inside {{ and <. This is not a perfect function, but it helps give us cleaner words than if we just used the pages without any preprocessing.
    '''
        bad = re.compile("(\{\{(?s).+?\}\})")
        bad2 = re.compile("(<(?s).+?>)")
        badMatches = re.findall(bad, text)
        badMatches += re.findall(bad2, text)
        for match in badMatches:
            text = text.replace(match, "")
        
        validstarts = "[ \"\-\[{2}\(]"
        validends = "(?=[ .,:;\-\|\]{2}\)\"])"
        rx2 = re.compile("%s(\w+'?\w*)%s"% (validstarts,validends), re.UNICODE)
        matches = [match.lower() for match in rx2.findall(text)]
               
        c = defaultdict(int)
        for match in matches:
            c[match] += 1

        words = c.items()
        words.sort()
        for k in words[1:]:
            key= k[0].encode("utf-8")
            st = "%s\t%i"% (key,k[1])
            print st
           
parser = xml.sax.make_parser()
handler = PageHandler()
parser.setContentHandler(handler)

#we must feed the parser an overarching tag so that it will read all pages
parser.feed("<doc>")

for line in sys.stdin:
  try:
      parser.feed(line)
  except xml.sax._exceptions.SAXParseException:
      continue
      
parser.feed("/<doc>")      
      
      