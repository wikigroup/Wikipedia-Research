import sys
import xml.sax
from collections import defaultdict,namedtuple
from bz2 import BZ2File
import multiprocessing
import psycopg2

class PageHandler(xml.sax.handler.ContentHandler):
    def __init__(self,outQ):
        self.buffer =[]
        self.inText = False
        self.inID = False
        self.inRevision = False
        self.currentPageID = None
        self.outQ = outQ
        
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
            text = "".join(self.buffer).encode("utf-8")
            self.outQ.put((self.currentPageID,text))
        elif name == "mediawiki":
            self.outQ.put((None,None))
        
        self.buffer = []

def qManager(outQ):
    i=1
    connection = psycopg2.connect(database = "wikigroup", user = "postgres", password = "wiki!@carl", host = "localhost", port = "5432")
    cursor = connection.cursor()
    while True:
        pageID,text = outQ.get()
        if pageID is not None:
            addToDB(pageID,text,cursor)
            print i
            i+=1
        else:
            connection.commit()
            connection.close()
            break

def addToDB(pageID, pageText, cursor):
    cursor.execute("INSERT INTO articleText2 VALUES (%s, %s);", (pageID, pageText))
    print pageID

def readIn(inQ):
    chunksize=10000000
    infile = BZ2File(sys.argv[1])
    next = infile.read(chunksize)
    while next != "":
        inQ.put(next)
        next = infile.read(chunksize)
    inQ.put(None)

#page = namedtuple("Page",['pageid','text'])

if __name__=="__main__":
    
    parser = xml.sax.make_parser()
    inQ = multiprocessing.Queue(10)
    outQ = multiprocessing.Queue(50)
    handler = PageHandler(outQ)
    parser.setContentHandler(handler)

    p = multiprocessing.Process(target=qManager,args=(outQ,))
    p.start()
    q = multiprocessing.Process(target=readIn,args=(inQ,))
    q.start()
    
    
    
    chunk = inQ.get()
    while True:
        if chunk is not None:
            parser.feed(chunk)
            chunk = inQ.get()
        else:
            break
        
    p.join()
    q.join()
