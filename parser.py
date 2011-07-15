'''
This program takes a wikipedia database dump in XML format with BZ2 compression, and
divides it into metadata, which it outputs to a tab-seaparated format, and article text, 
which it recompresses and outputs page-by-page in a compressed XML format.

Usage: python parser.py dumpfile dumpid [notext]
Dumpfile: The location of the BZ2-compressed XML database dump
Dumpid: An aribitrary ID (often the number of the dump chunk is used) which is appended to the end of the metadata files
notext: An optional flag.  If this option is specified, only metadata will be aggregated (no article text will be output).
		A 4-5x speed increase results from using this option, so it is recommended if the full article text is not needed.
'''

import xml.sax
import time
from datetime import timedelta
import sys
from threading import Thread
import bz2
import os
import csv
from multiprocessing import Process, Pipe, Queue

parseQ = Queue(5)

class PageHandler(xml.sax.handler.ContentHandler):
	def __init__(self,flatFileOutPath,textOutputPath,statusUpdater,procid,textoutput,encoding="utf-8",writeOutInterval=1000000):
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
		
		# Start the file writer process, if we're going to be writing files
		if textoutput:
			Process(target=fileWriter,args=(self.compressQ,textOutputPath)).start()
			self.fileWrite = self.sendToFileWriter
		# Otherwise, we ignore calls to write to text files.
		else:
			statusUpdater.addMessage("Article Text Output Disabled")
			def doesNothing(x,y=None): pass
			self.fileWrite = doesNothing
		
		self.namespaces = ["Main","User","Wikipedia","File","MediaWiki","Template","Help","Category","Thread","Summary","Portal","Book"]
		# Because the talk namespace for Main is Talk, not Main talk, we don't want to append talk to the first element of the list
		self.namespaces = self.namespaces + [n+" talk" for n in self.namespaces[1:]]
		self.namespaces += ["Talk"]
		
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
		
		# If this is the last write out (beacuse the parser has finished), we don't need to open up a new file.
		if not final:
			self.initializeWriters()
	
	def startElement(self, name, attributes):
		if name =="revision":
			self.inRevision = True
		elif name =="contributor":
			self.handleStartContributor(attributes)
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
			# If the content is not within a page or reivison tag, and isn't the end of the document
			# (</mediawiki>), then ignore.
			pass
			
		self.buffer = []
	
	def handleStartContributor(self,attributes):
		self.inContributor = True
		# If this is a deleted user, set the username to the special deleted user dummy
		if "deleted" in attributes and attributes.getValue("deleted")=="deleted":
			self.revattrs["ed_username"] = "**DELETED_USER"
	
	def handleTagWithinPage(self,name):
		if name == "page":
			self.handleEndOfPage()
		elif name == "redirect":
			# 1 is meant to indicate True, in a way that PostgreSQL will like.
			self.pageattrs["redirect"]=1
		elif name == "title":
			self.handleTitle()
		elif name == "id":
			self.handleID()
		else:
			# If the tag isn't something we recognize, stash it in the pageattrs dictionary,
			# in case we want to use it later.
			self.pageattrs[name]=self.buffer
	
	def handleEndMediaWiki(self):
		# Tell the file writer that it is done with the last file, and can shut down.
		self.fileWrite(None,True)
		self.compressQ.put((None,True))
		# Write out any remaining metadata to the revision, page, and editor files
		self.writeOutIntermediateResults(True)
	
	def handleID(self):
		# Make sure that the page ID has at least four digits (by adding leading zeros), so that it can
		# fit within the organizational system for text output. 
		self.buffer = self.buffer.zfill(4)
		self.pageattrs["id"]=self.buffer
		# Where 15431234 is the page ID, the article text output should go in /15/43/15431234.xml.bz2
		path = "{0}/{1}/{2}/{3}.xml.bz2"\
			.format(self.textOutputPath,self.pageattrs["id"][:2],self.pageattrs["id"][2:4],self.pageattrs["id"])
		# Tells the filewriter to open the file at path.
		self.fileWrite(path,True)
		self.fileWrite("<page id='{0}'>\n".format(self.buffer))
		self.status.setCurrentPageID(self.pageattrs["id"])
	
	def sendToFileWriter(self,line,filechange=False):
		""" If filechange is False (default), enqueue line for writing by the file writer process.
		 	If filechange is True, close whatever file the file writer has open, and open the file
			whose path is line
		"""
		self.compressQ.put((line,filechange))
	
	def ignoreFileWriting(self,line,filechg=False):
		pass

	def handleTitle(self):
		""" If this page is in a page other than Main, it will contain a ":", in which case we should extract
		 	the namespace """
		
		titleInfo = self.buffer.split(":",1)
		if len(titleInfo) == 2 and titleInfo[0] in self.namespaces:
			self.pageattrs["namespace"]=titleInfo[0]
			self.pageattrs["title"]=titleInfo[1]
		else:
			self.pageattrs["namespace"]="Main"
			self.pageattrs["title"]=self.buffer
		self.status.setCurrentPageTitle(self.buffer)
	
	def handleEndOfPage(self):
		self.inPage = False
		# If we haven't found a <redirect>, this page must not be a redirect
		# Make the redirect attribute False (for PostgreSQL purposes, 0 is False)
		if "redirect" not in list(self.pageattrs.keys()):
			self.pageattrs["redirect"]=0
		
		encodeSpecifiedDictValues(self.pageattrs,["namespace","title"],self.encoding)
		self.pageWriter.writerow(self.pageattrs)
		self.fileWrite("\t</page>")
		
		# If we've gone a long time without writing out our metadata, do so.
		if self.revisionsSinceLastWriteOut > self.writeOutInterval:
			self.writeOutIntermediateResults()
		
		self.status.incrementPagesParsed()
		# Clear out the page attributes dictionary for the next page.
		self.pageattrs={}
	
	def generateRevisionXML(self):
		""" Generate the XML output for one revision """
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
		
		# If we haven't found a <minor>, this revision must not be minor
		# Make the minor attribute False (for PostgreSQL purposes, 0 is False)
		if "minor" not in list(self.revattrs.keys()):
			self.revattrs["minor"]=0
		
		encodeSpecifiedDictValues(self.revattrs,["comment","ed_username"],self.encoding)
		self.revisionWriter.writerow(self.revattrs)
		self.addRevisionEditorToDictionary()
		
		# If we got any article text, generate XML and write it out.
		if "text" in self.revattrs.keys():
			self.fileWrite(self.generateRevisionXML())
		
		self.status.incrementRevisionsParsed()
		self.revisionsSinceLastWriteOut+=1
		# Clear out the page attributes dictionary for the next page.
		self.revattrs={}
	
	def addRevisionEditorToDictionary(self):
		''' If the editor has an ID, add it to the editors dictionary under the relevant username
			Otherwise, just add the username, but leave the ID blank'''
		if "ed_id" in list(self.revattrs.keys()):
			self.editors[self.revattrs["ed_username"]]=self.revattrs["ed_id"]
		else:
			self.editors[self.revattrs["ed_username"]]=""
	
	def handleTagWithinContributor(self,name):
		if name == "username":
		    self.revattrs["ed_username"] = self.buffer
		elif name == "ip":
		    self.revattrs["ed_username"] = self.buffer
		elif name == "id":
		    self.revattrs["ed_id"]= self.buffer
		else:
			self.status.addMessage("Strange tag <{0}> encountered within contributor tag".format(name))

	def handleTagWithinRevision(self,name):
		if name == "revision":
			self.handleEndOfRevision()
		elif name == "timestamp":
			# Reformat the timestamp to make PostgreSQL happy.
			self.revattrs["timestamp"] = "{0} {1}".format(self.buffer[0:10],self.buffer[11:-1])
		elif name == "minor":
			# PostgreSQL uses 0 and 1 to signify False and True, so we do too!
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
	""" Accepts text from a Queue, and writes it out compressed."""
	
	# Create a dummy file, just so we have something to close when we get our first file open request.
	f = bz2.BZ2File(outpath+"/INITIAL","w")
	
	while True:
		toWrite,filechg = q.get(True)
		
		# If filechg is false, just write out toWrite to whatever file is open
		if not filechg:
			f.write(toWrite)
		# If filechg is true, and toWrite is not None, close the file that's open, and open the file with the path toWrite
		elif toWrite is not None:
			f.close()
			f = bz2.BZ2File(toWrite,"w")
		# If toWrite is None, the parser is signaling that there is nothing else to write.
		# In that case, close the open file, and shut down the file writer
		else:
			f.close()
			break
	
def encodeSpecifiedDictValues(dct,specifiedKeys,encoding):
	""" Given a list of keys, and a dictionary, properly encode the values of those keys."""
	for key in specifiedKeys:
		if key in dct.keys():
			dct[key]=dct[key].encode(encoding)

class StatusUpdater(Thread):
	""" This class prints pretty status messages while the program runs.  It runs in the same process
		as the parser (to facilitate easy message passing), but in a separate thread. """
	def __init__(self,updateInterval,inputFile):
		Thread.__init__(self)
		self.setName("Status Updater")
		self.setDaemon(True)
		self._revisionsParsed=0
		self._pagesParsed=0
		self._startTime=time.time()
		self._prettyStartTime = time.strftime("%m/%d/%y %H:%M:%S")
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
		""" When we receive a call to this method, print the output one last time, and then end the process """
		self._terminate = True
		self.printOutput()
	
	def printOutput(self):
		current = time.strftime("%m/%d/%y %H:%M:%S")
		secs = int(time.time()-self._startTime)
		runtime = str(timedelta(seconds=secs))
		
		print("--------")
		print("Stats for File {0}".format(self._inputFile))
		print("Started: {0}. Current Time: {1}".format(self._prettyStartTime,current))
		print("Run time so far: {0}".format(runtime))
		print("Revisions Parsed: {0:n}".format(self._revisionsParsed))
		print("Pages Parsed: {0:n}".format(self._pagesParsed))
		print("Parser Now Serving Page/Revision: {0}/{1}: {2}")\
				.format(self._currentParsePageID,self._currentParseRevisionID,self._currentParsePageTitle.encode("utf-8"))
		print("Messages:")
		for message in self._messages:
			print(message)
	
		# Force writes file, so that status can be piped and still viewed during execution, etc...
		# http://docs.python.org/library/os.html#os.fsync
		sys.stdout.flush()
		os.fsync(sys.stdout.fileno())
	
	def run(self):
		while not self._terminate:
			self.printOutput()
			time.sleep(self._updateInterval)
		
def decompressFile(path,chunksize):
	""" Runs in its own process, decompressing chunks and putting them onto the paring Queue.
		Note that for memory management reasons, only 5 decompressed chunks may be in parseQ.
		After that, this process will block until space becomes available."""
	f = bz2.BZ2File(path,"r")
	data = f.read(chunksize)
	while data != "":
		parseQ.put(data)
		data = f.read(chunksize)
	parseQ.put(None)
	f.close()

def parse(statusUpdater,metaOutpath,textOutpath,procid,textoutput):
	# Sets up the XML parser
	parser = xml.sax.make_parser()
	handler = PageHandler(metaOutpath,textOutpath,statusUpdater,procid,textoutput)
	parser.setContentHandler(handler)
	
	# As long as we don't get a None (which indicates the end of the file),
	# pull decompressed chunks from parseQ and send them through the parser.
	while True:
		p = parseQ.get(True)
		if p is not None:
			parser.feed(p)
		else:
			statusUpdater.addMessage("Parser has finished working.")
			break

def make100numbereddirs(basepath):
	''' Generate 100 directories, numebred 00 through 99, in the specified folder.
		This method is used to create directories to hold the article text output '''
	
	for i in range(100):
		pth = "/{0}/{1:02d}/".format(basepath,i)
		if not os.path.exists(pth):
		    os.makedirs(pth)

def makeDirectories(textoutpath,metaoutpath):
	make100numbereddirs(textoutpath)
	for i in range(100):
		make100numbereddirs("{0}/{1:02d}".format(textoutpath,i))

	if not os.path.exists(metaoutpath):
		os.makedirs(metaoutpath)

def processArguments():
	assert len(sys.argv) in [4,5],"Usage: python {0} inputfile procid statusupdateinterval [notext]".format(sys.argv[0])
	inpath = sys.argv[1]
	procid = sys.argv[2]
	updateInterval = int(sys.argv[3])
	
	if len(sys.argv)==5 and sys.argv[4] == "notext":
		textoutput=False
	else:
		textoutput=True
	
	assert os.path.exists(inpath),"Input file does not exist"
	assert type(updateInterval) is int,"Status update interval must be an integer"
	
	return inpath,procid,updateInterval,textoutput

if __name__=="__main__":
	inpath,procid,updateInterval,textoutput = processArguments()
	
	textoutpath = "/wikigroup/textoutput"
	metaoutpath = "/wikigroup/metaoutput"
	
	makeDirectories(textoutpath,metaoutpath)
	
	s= StatusUpdater(updateInterval,inpath)
	s.start()
	p = Process(target=decompressFile,args=(inpath,10000000))
	p.start()
	parse(s,metaoutpath,textoutpath,procid,textoutput)
	s.addMessage("Execution complete!  Final stats shown immediately above.")
	s.terminate()