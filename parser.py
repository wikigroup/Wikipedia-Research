import xml.sax
import time
import sys
from threading import Thread
import bz2
import os
import csv
from multiprocessing import Process, Pipe, Queue

parseQ = Queue(5)

class PageHandler(xml.sax.handler.ContentHandler):
	def __init__(self,flatFileOutPath,textOutputPath,statusUpdater,procid,encoding="utf-8",writeOutInterval=1000000):
		# Dictionaries that store editor, revision, and page attributes
		self.editors = {}
		self.pageattrs = {}
		self.revattrs = {}
		# State variables.  The interpretation of a tag such as <id> depends on its parent tag, for example.
		self.inPage = False
		self.inRevision = False
		self.inContributor = False
		# How to encode output.  Defaults to utf-8
		self.encoding = encoding
		# How many times we have written out the page, revision, and editor files.
		# writeOutInterval defaults to 1000000 (revisions)
		# procid is an arbitrary string appended to metadata output files.
		self.procid = procid
		self.writeOutNum = 0
		self.writeOutInterval = writeOutInterval
		# Locations to output text and metadata.
		self.textOutputPath = textOutputPath
		self.flatFileOutPath = flatFileOutPath
		# Defines the csv format 
		csv.register_dialect("TabDelim",delimiter="\t",quoting=csv.QUOTE_NONE,escapechar="\\")
		# Initialize the metadata writers
		self.initializeWriters()
		# The number of revisions queued for compression is limited to 1000 at a time.
		# If this is exceeeded, the parser will block until space is available in the queue.
		self.compressQ = Queue(1000)
		# Start the file writer process
		Process(target=fileWriter,args=(self.compressQ,textOutputPath)).start()
		
		self.status = statusUpdater
		
	def initializeWriters(self):
		'''Initialize the csv writer instances used to output the metadata'''
		self.revisionsSinceLastWriteOut =0
		
		self.pageFile = open("{0}/pg{1}_{2}.dat".format(self.flatFileOutPath,self.writeOutNum,self.procid),"w")
		self.revisionFile = open("{0}/rv{1}_{2}.dat".format(self.flatFileOutPath,self.writeOutNum,self.procid),"w")
		self.editorFile = open("{0}/ed{1}_{2}.dat".format(self.flatFileOutPath,self.writeOutNum,self.procid),"w")
		
		self.pageOutputFields=["id","title","namespace","redirect"]
		self.pageWriter = csv.DictWriter(self.pageFile,fieldnames=self.pageOutputFields,\
										restval="",extrasaction='ignore',dialect="TabDelim")
		self.revisionOutputFields=["id","pageid","ed_id","ed_username","minor","timestamp","comment"]
		self.revisionWriter = csv.DictWriter(self.revisionFile,fieldnames=self.revisionOutputFields,\
											restval="",extrasaction='ignore',dialect="TabDelim")
											
		self.editorWriter = csv.writer(self.editorFile,"TabDelim")
		
	def writeOutIntermediateResults(self,final=False):
		'''Write all data to page, revision, and editor files.
		Reset the editors dictionary, and open new files for future output'''
		self.revisionFile.close()
		self.pageFile.close()
		self.editorWriter.writerows(self.editors.iteritems())
		self.editorFile.close()
		self.editors = {}
		self.status.addMessage("Editor {0}, Revision {0}, and Page {0} Files Generated".format(self.writeOutNum))
		
		self.writeOutNum+=1
		if not final:
			self.initializeWriters()
	
	def startElement(self, name, attributes):
		if name =="revision":
			self.inRevision = True
		elif name =="contributor":
			self.inContributor = True
			# If this is a deleted user, set the username to the special deleted user dummy
			if "deleted" in attributes and attributes.getValue("deleted")=="deleted":
				self.revattrs["ed_username"] = "**DELETED_USER"
		elif name == "page":
			self.inPage = True
			
		self.buffer=[]

	def characters(self, data):
		self.buffer.append(data)

	def endElement(self, name):
		self.buffer = "".join(self.buffer)
		
		if self.inRevision:
			self.handleTagWithinRevision(name)
		elif self.inPage:
			self.handleTagWithinPage(name)
		elif name == "mediawiki":
			self.handleEndMediaWiki()
		else:
			pass
			
		self.buffer = []
	
	def handleTagWithinPage(self,name):
		if name == "page":
			self.handleEndOfPage()
		elif name == "redirect":
			self.pageattrs["redirect"]=1
		elif name == "title":
			self.handleTitle()
		elif name == "id":
			self.handleID()
		else:
			self.pageattrs[name]=self.buffer
	
	def handleEndMediaWiki(self):
		# Tell the file writer that it is done with the last file.
		self.writeLineToFile(None,True)
		self.writeOutIntermediateResults(True)
	
	def handleID(self):
		self.buffer = self.buffer.zfill(4)
		self.pageattrs["id"]=self.buffer
		path = "{0}/{1}/{2}/{3}.xml.bz2"\
			.format(self.textOutputPath,self.pageattrs["id"][:2],self.pageattrs["id"][2:4],self.pageattrs["id"])
		self.writeLineToFile(path,True)
		self.writeLineToFile("<page id='{0}'>\n".format(self.buffer))
		self.status.setCurrentPageID(self.pageattrs["id"])
	
	def writeLineToFile(self,line,filechange=False):
		self.compressQ.put((line,filechange))
	
	def handleTitle(self):
		#extracts namespace
		titleInfo = self.buffer.split(":")
		if len(titleInfo) == 2:
			#there is another namespace besides main
			self.pageattrs["namespace"] = titleInfo[0]
			self.pageattrs["title"] = titleInfo[1]
		else:
			self.pageattrs["namespace"] = "Main"
			self.pageattrs["title"] = titleInfo[0]
		self.status.setCurrentPageTitle(self.buffer)
	
	def handleEndOfPage(self):
		self.inPage = False
		if "redirect" not in list(self.pageattrs.keys()):
			self.pageattrs["redirect"]=0
		
		encodeSpecifiedDictValues(self.pageattrs,["title"],self.encoding)
		self.pageWriter.writerow(self.pageattrs)
		
		self.writeLineToFile("\t</page>")
		
		if self.revisionsSinceLastWriteOut > self.writeOutInterval:
			self.writeOutIntermediateResults()
		
		self.status.incrementPagesParsed()
		self.pageattrs={}
	
	def generateRevisionXML(self):
		xmloutput = []
		xmloutput.append("\t<revision id='{0}'>\n".format(self.revattrs["id"]))
		xmloutput.append(xml.sax.saxutils.escape(self.revattrs["text"]))
		xmloutput.append("\n\t</revision>\n")
		
		st = "".join(xmloutput)
		st = st.encode(self.encoding)
		
		return st
	
	def handleEndOfRevision(self):
		self.inRevision = False
		self.revattrs["pageid"]=self.pageattrs["id"]

		if "minor" not in list(self.revattrs.keys()):
			self.revattrs["minor"]=0
		
		encodeSpecifiedDictValues(self.revattrs,["comment","ed_username"],self.encoding)
		self.revisionWriter.writerow(self.revattrs)
		
		if "ed_id" in list(self.revattrs.keys()):
			self.editors[self.revattrs["ed_username"]]=self.revattrs["ed_id"]
		else:
			self.editors[self.revattrs["ed_username"]]=""
			
		if "text" in list(self.revattrs.keys()):
			self.writeLineToFile(self.generateRevisionXML())
		
		self.status.incrementRevisionsParsed()
		self.revisionsSinceLastWriteOut+=1
		self.revattrs={}
	
	def handleTagWithinContributor(self,name):
		if name == "username":
		    self.revattrs["ed_username"] = self.buffer
		elif name == "ip":
		    self.revattrs["ed_username"] = self.buffer
		else:
		    #name = "id"
		    self.revattrs["ed_id"]= self.buffer

	def handleTagWithinRevision(self,name):
		if name == "revision":
			self.handleEndOfRevision()
		elif name == "timestamp":
			self.revattrs["timestamp"] = "{0} {1}".format(self.buffer[0:10],self.buffer[11:-1])
		elif name == "minor":
			self.revattrs["minor"]=1
		elif name == "contributor":
			self.inContributor = False
		elif self.inContributor:
			self.handleTagWithinContributor(name)
		elif name == "id":
			self.revattrs["id"]=self.buffer
			self.status.setCurrentRevisionID(self.revattrs["id"])
		else:
			self.revattrs[name]=self.buffer

def fileWriter(q,outpath):
	f = bz2.BZ2File(outpath+"/INITIAL","w")
	
	while True:
		toWrite,filechg = q.get(True)
		
		if not filechg:
			f.write(toWrite)
		elif toWrite:
			f.close()
			f = bz2.BZ2File(toWrite,"w")
		else:
			f.close()
			break
	
def encodeSpecifiedDictValues(dct,specifiedKeys,encoding):
	for key in specifiedKeys:
		if key in dct.keys():
			dct[key]=dct[key].encode(encoding)

class StatusUpdater(Thread):
	def __init__(self,updateInterval,inputFile):
		Thread.__init__(self)
		self.setName("Status Updater")
		self.setDaemon(True)
		self._revisionsParsed=0
		self._pagesParsed=0
		self._startTime=time.time()
		self._currentParseRevisionID=0
		self._currentParsePageID=0
		self._currentParsePageTitle=""
		self._messages=[]
		self._updateInterval = updateInterval
		self._inputFile = inputFile
		self._terminate=False
	
	def addMessage(self,message):
		self._messages.append(message)
	
	def incrementRevisionsParsed(self):
		self._revisionsParsed+=1
		
	def incrementPagesParsed(self):
		self._pagesParsed+=1
		
	def setCurrentRevisionID(self,revID):
		self._currentParseRevisionID = revID
		
	def setCurrentPageID(self,pageID):
		self._currentParsePageID = pageID
		
	def setCurrentPageTitle(self,pageTitle):
		self._currentParsePageTitle = pageTitle
		
	def terminate(self):
		self._terminate = True
		self.printOutput()
	
	def printOutput(self):
		secs = int(time.time()-self._startTime)
		hrs = secs/3600
		mins = secs%3600/60
		secs = secs - hrs*3600 - mins*60
		
		print("--------")
		print("Stats for File {0}".format(self._inputFile))
		print("Run time so far: {0:02}:{1:02}:{2:02}".format(hrs,mins,secs))
		print("Revisions Parsed: {0:n}".format(self._revisionsParsed))
		print("Pages Parsed: {0:n}".format(self._pagesParsed))
		print("Parser Now Serving Page/Revision: {0}/{1}: {2}")\
				.format(self._currentParsePageID,self._currentParseRevisionID,self._currentParsePageTitle.encode("utf-8"))
		print("Messages:")
		for message in self._messages:
			print(message)
	
		sys.stdout.flush()
		os.fsync(sys.stdout.fileno())
	
	def run(self):
		while not self._terminate:
			self.printOutput()
			time.sleep(self._updateInterval)
		
def decompressFile(path,chunksize):
	f = bz2.BZ2File(path,"r")
	data = f.read(chunksize)
	while data != "":
		parseQ.put(data)
		data = f.read(chunksize)
	parseQ.put(None)
	f.close()

def parse(statusUpdater,metaOutpath,textOutpath,procid):
	parser = xml.sax.make_parser()
	handler = PageHandler(metaOutpath,textOutpath,statusUpdater,procid)
	parser.setContentHandler(handler)
	
	while True:
		p = parseQ.get(True)
		if p:
			parser.feed(p)
		else:
			statusUpdater.addMessage("Parser has finished working.")
			break

def make100numbereddirs(basepath):
	for i in range(100):
		pth = "/{0}/{1:02d}/".format(basepath,i)
		if not os.path.exists(pth):
		    os.makedirs(pth)

def makeCompressedTextDirs(outpath):
	make100numbereddirs(outpath)
	for i in range(100):
		make100numbereddirs("{0}/{1:02d}".format(outpath,i))

if __name__=="__main__":
	assert len(sys.argv) == 4,"Usage: python {0} inputfile procid statusupdateinterval".format(sys.argv[0])
	inpath = sys.argv[1]
	procid = sys.argv[2]
	updateInterval = int(sys.argv[3])
	assert os.path.exists(inpath),"Input file does not exist"
	assert type(updateInterval) is int,"Status update interval must be an integer"
	
	textOutpath = "/wikigroup/textoutput"
	metaOutpath = "/wikigroup/metaoutput"
	
	makeCompressedTextDirs(textOutpath)
	
	if not os.path.exists(metaOutpath):
		os.makedirs(metaOutpath)

	s= StatusUpdater(updateInterval,inpath)
	s.start()
	p = Process(target=decompressFile,args=(inpath,10000000))
	p.start()
	parse(s,metaOutpath,textOutpath,procid)
	s.addMessage("Execution complete!  Final stats shown immediately above.")
	s.terminate()