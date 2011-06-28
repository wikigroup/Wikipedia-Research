import time
import bz2
import xml.sax

class FullBreak(Exception):
    '''
    Exception class the is used to fully break out of multiple for loops
    '''
    pass

class pageHandler(xml.sax.handler.ContentHandler):
    '''
    Sax parser page handler class that parsers the XML
    '''
    def __init__(self):
        self.inPage = False
        self.idList = [] #stores list of page ids
        self.buffer = [] #string of text inside tags
        self.pageCount = 0 #number of pages visited
        self.revCount = 0 #number of revisions visited
    
    def startElement(self, name, attrs):
        if name == "page":
            self.inPage = True
        elif name == "revision":
            self.inPage = False
        self.buffer = []
    
    def characters(self, content):
        self.buffer.append(content)
    
    def endElement(self, name):
        self.buffer = "".join(self.buffer)
        if name == "page":
            self.inPage = False
            self.pageCount += 1
        elif name == "id" and self.inPage:
            self.idList.append(self.buffer)
        elif name == "revision":
            self.revCount += 1
        self.buffer = []   
         
    def getPageCount(self):
        return self.pageCount
        
    def getRevCount(self):
        return self.revCount
        
    def getIdList(self):
        return self.idList

def getPageIds(bigFile):
    '''
    Opens the file of the wikipedia dump and sends it through the parser. Stops parsing after 50000 pages have been read. Returns the list of parsed pages and the number of revisions.
    '''
    in_file = open(bigFile, "r")
    dec = bz2.BZ2Decompressor()
    parser = xml.sax.make_parser()
    handler = pageHandler()
    parser.setContentHandler(handler)

    for line in in_file:
        data = dec.decompress(line)
        parser.feed(data)
        if parser.getContentHandler().getPageCount() > 50000:
            break
    print "PAGE: ", parser.getContentHandler().getPageCount()
    print "REV: ", parser.getContentHandler().getRevCount()
    return parser.getContentHandler().getIdList(), parser.getContentHandler().getRevCount()

def main():
    #Big file is the path to the major wikipedia dump
    #folder is the path to the testoutput folder that stores each page text file in the dump
    bigFile  = '/Volumes/LA-PUBLIC/Full-Chunks/enwiki-20110405-pages-meta-history14.xml.bz2'
    folder = '/Volumes/LA-PUBLIC/testoutput/'
    list, revCount = getPageIds(bigFile)
    '''
    Checks to see which is faster when counting revCount revisions
    '''
    # bCount = 0
#     fCount = 0
#     in_file = open(bigFile, "r")
#     dec = bz2.BZ2Decompressor()
#     bStart = time.clock()
#     for line in in_file:
#         data = dec.decompress(line)
#         bCount += data.count("</revision>")
#         if bCount >= revCount:
#             bEnd = time.clock()
#             break
#     print "BigC: ", bCount
#     print "Big: ", bEnd - bStart
#     in_file.close()
#     fStart = time.clock()
#     for page in list:
#         dec = bz2.BZ2Decompressor()
#         fold1 = page[:2]
#         fold2 = page[2:4]
#         in_file = open(folder + fold1 + "/" + fold2 + "/" + page + ".txt.bz2", "r")
#         for line in in_file:
#             data = dec.decompress(line)
#             fCount += data.count("</revision>")
#             if fCount >= revCount:
#                 fEnd = time.clock()
#                 break
#         in_file.close()
#     print "SmallC: ", fCount
#     print "Small: ", fEnd - fStart
 
    '''
    Checks to see which is faster when counting 900000000 characters
    '''
 #    bCount = 0
#     fCount = 0
#     in_file = open(bigFile, "r")
#     dec = bz2.BZ2Decompressor()
#     bStart = time.clock()
#     try:
#         for line in in_file:
#             data = dec.decompress(line)
#             for char in data:
#                 bCount += 1
#                 if bCount == 900000000:
#                     bEnd = time.clock()
#                     raise FullBreak
#     except FullBreak:
#         pass
#     print "BigC: ", bCount
#     print "Big: ", bEnd - bStart
#     in_file.close()
#     try:
#         fStart = time.clock()
#         for page in list:
#              dec = bz2.BZ2Decompressor()
#              fold1 = page[:2]
#              fold2 = page[2:4]
#              in_file = open(folder + fold1 + "/" + fold2 + "/" + page + ".txt.bz2", "r")
#              for line in in_file:
#                  data = dec.decompress(line)
#                  for char in data:
#                     fCount += 1
#                     if fCount == 900000000:
#                         fEnd = time.clock()
#                         raise FullBreak
#              in_file.close()
#     except FullBreak:
#         pass
#     print "SmallC: ", fCount
#     print "Small: ", fEnd - fStart
    '''
    Founds out how long it takes to open and close around 50000 files
    '''
    start = time.clock()
    for page in list:
        dec = bz2.BZ2Decompressor()
        fold1 = page[:2]
        fold2 = page[2:4]
        in_file = open(folder + fold1 + "/" + fold2 + "/" + page + ".txt.bz2", "r")
        in_file.close()
    end = time.clock()
    print "Number Pages: ", len(list)
    print "Time: ", end - start

if __name__ == "__main__":
    main()
    
    
    
    
'''
PAGE:  50036
REV:  288792
BigC:  288934
Big:  105.75133
SmallC:  288793
Small:  131.200628


PAGE:  50036
REV:  288792
BigC:  900000000
Big:  107.615183
SmallC:  900000000
Small:  115.303154

PAGE:  50036
REV:  288792
Number Pages:  50037
Time:  2.22694
'''