import xml.sax
import time
import sys
from threading import Thread
import bz2
import os
import csv
from collections import deque
from multiprocessing import Process, Pipe, Pool, Queue

parseQ = Queue(5)
statusQ = deque()
outpath = "/wikigroup/textoutput"
flatOutPath = "/wikigroup/flatoutput"

class PageHandler(xml.sax.handler.ContentHandler):
	def __init__(self,flatFileOutPath,textOutputPath,encoding="utf-8",writeOutInterval=1000000):
		encodedtab = "\t".encode(encoding)
		
		csv.register_dialect("TabDelim",delimiter=encodedtab,quoting=csv.QUOTE_NONE,escapechar="\\")
		
		self.editors = {}
		self.attrs = {}
		self.revattrs = {}
		self.inRevision = False
		self.inContributor = False
		self.encoding = encoding
		self.writeOutNum = 0
		self.writeOutInterval = writeOutInterval
		self.textOutputPath = textOutputPath
		self.flatFileOutPath = flatFileOutPath
		self.initializeWriters()
		self.compressQ = Queue(1000)
		self.writer = Process(target=pipedFileWriter,args=(self.compressQ,))
		self.writer.start()

	def initializeWriters(self):
		global procid
		
		self.revisionsSinceLastWriteOut =0
		
		self.pageFile = open("{0}/pg{1}_{2}.dat".format(self.flatFileOutPath,self.writeOutNum,procid),"w")
		self.revisionFile = open("{0}/rv{1}_{2}.dat".format(self.flatFileOutPath,self.writeOutNum,procid),"w")
		self.editorFile = open("{0}/ed{1}_{2}.dat".format(self.flatFileOutPath,self.writeOutNum,procid),"w")
		
		self.pageOutputFields=["id","title","namespace","redirect"]
		self.pageWriter = csv.DictWriter(self.pageFile,fieldnames=self.pageOutputFields,\
										restval="",extrasaction='ignore',dialect="TabDelim")
		self.revisionOutputFields=["id","pageid","ed_id","ed_username","minor","timestamp","comment"]
		self.revisionWriter = csv.DictWriter(self.revisionFile,fieldnames=self.revisionOutputFields,\
											restval="",extrasaction='ignore',dialect="TabDelim")
											
		self.editorWriter = csv.writer(self.editorFile,"TabDelim")
		
	def writeOutIntermediateResults(self,final=False):
		self.revisionFile.close()
		self.pageFile.close()
		self.editorWriter.writerows(self.editors.iteritems())
		self.editorFile.close()
		self.editors = {}
		statusQ.appendleft(("messages","Editor {0}, Revision {0}, and Page {0} Files Generated".format(self.writeOutNum)))
		
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
		self.buffer=[]

	def characters(self, data):
		self.buffer.append(data)

	def endElement(self, name):
		self.buffer = "".join(self.buffer)
		
		if name == "page":
			self.handleEndOfPage()
		elif name == "revision":
			self.handleEndOfRevision()
		elif self.inRevision:
			self.handleTagWithinRevision(name)
		elif name == "mediawiki":
			self.writeLineToFile(None,True)
			self.writeOutIntermediateResults(True)
		else:
			self.handleTagWithinPage(name)
			
		self.buffer = []
	
	def handleTagWithinPage(self,name):		
		if name == "redirect":
			self.attrs["redirect"]="1"
		elif name == "title":
			self.handleTitle()
		elif name == "id":
			self.handleID()
		else:
			self.attrs[name]=self.buffer
	
	def handleID(self):
		while len(self.buffer)<4:
			self.buffer = "0"+self.buffer
		self.attrs["id"]=self.buffer
		path = "{0}/{1}/{2}/{3}.xml.bz2"\
			.format(self.textOutputPath,self.attrs["id"][:2],self.attrs["id"][2:4],self.attrs["id"])
		self.writeLineToFile(path,True)
		self.writeLineToFile("<page id='{0}'>\n".format(self.buffer))
		statusQ.appendleft(("currentParsePageID",self.attrs["id"]))
	
	def writeLineToFile(self,line,filechange=False):
		self.compressQ.put((line,filechange))
	
	def handleTitle(self):
		#extracts namespace
		titleInfo = self.buffer.split(":")
		if len(titleInfo) == 2:
			#there is another namespace besides main
			self.attrs["namespace"] = titleInfo[0]
			self.attrs["title"] = titleInfo[1]
		else:
			self.attrs["namespace"] = "Main"
			self.attrs["title"] = titleInfo[0]
		statusQ.appendleft(("currentParsePageTitle",self.buffer))
	
	def handleEndOfPage(self):
		if "redirect" not in list(self.attrs.keys()):
			self.attrs["redirect"]="0"
		
		encodeSpecifiedDictValues(self.attrs,self.pageOutputFields,self.encoding)
		self.pageWriter.writerow(self.attrs)
		
		self.writeLineToFile("\t</page>")
		
		# if bigfile:
		# 	statusQ.appendleft(("messages","{0} ({1}) is going to use the streaming compressor.  Delays possible."\
		# 			.format(self.attrs["id"],self.attrs["title"].encode(self.encoding))))
		
		if self.revisionsSinceLastWriteOut > self.writeOutInterval:
			self.writeOutIntermediateResults()
		
		statusQ.appendleft(("pagesParsed","increment"))
		self.attrs={}
	
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
		self.revattrs["pageid"]=self.attrs["id"]

		if "minor" not in list(self.revattrs.keys()):
			self.revattrs["minor"]="0"
		
		encodeSpecifiedDictValues(self.revattrs,self.revisionOutputFields,self.encoding)
		self.revisionWriter.writerow(self.revattrs)
		
		if "ed_id" in list(self.revattrs.keys()):
			self.editors[self.revattrs["ed_username"]]=self.revattrs["ed_id"]
		else:
			self.editors[self.revattrs["ed_username"]]=""
			
		if "text" in list(self.revattrs.keys()):
			statusQ.appendleft(("revisionsParsed","increment"))
			self.writeLineToFile(self.generateRevisionXML())
			
		self.revisionsSinceLastWriteOut+=1
		self.revattrs={}
	
	def handleTagWithinContributor(self,name):
		if name == "username":
		    self.revattrs["ed_username"] = self.buffer
		if name == "ip":
		    self.revattrs["ed_username"] = self.buffer
		else:
		    #name = "id"
		    self.revattrs["ed_id"]= self.buffer

	def handleTagWithinRevision(self,name):
		if name == "timestamp":
			self.revattrs["timestamp"] = "{0} {1}".format(self.buffer[0:10],self.buffer[11:-1])
		elif name == "minor":
			self.revattrs["minor"]="1"
		elif name == "contributor":
			self.inContributor = False
		elif self.inContributor:
			self.handleTagWithinContributor(name)
		elif name == "id":
			self.revattrs["id"]=self.buffer
			statusQ.appendleft(("currentParseRevisionID",self.revattrs["id"]))
		else:
			self.revattrs[name]=self.buffer

def pipedFileWriter(q):
	global outpath
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
	
def encodeSpecifiedDictValues(dct,keylist,encoding):
	for key in keylist:
		if key in list(dct.keys()):
			dct[key]=dct[key].encode(encoding)

class StatusUpdater(Thread):
	def __init__(self,updateInterval):
		Thread.__init__(self)
		self.setName("Status Updater")
		self.setDaemon(True)
		
		self.chunksDecompressed=0
		self.dataParsed=0
		self.filesCompressed=0
		self.revisionsParsed=0
		self.pagesParsed=0
		self.revisionsWrittenOut=0
		self.startTime=0
		self.currentParseRevisionID=0
		self.currentParsePageID=0
		self.currentParsePageTitle=""
		self.currentCompressPageID=0
		self.messages=[]
		self.updateInterval = updateInterval
		
	def run(self):
		global inpath
		
		while True:
			self.getNewNotifications()
			
			secs = int(time.time()-self.startTime)
			hrs = secs/3600
			mins = secs%3600/60
			secs = secs - hrs*3600 - mins*60
			
			print("--------")
			print("Stats for File {0}".format(inpath))
			print("Run time so far: {0:02}:{1:02}:{2:02}".format(hrs,mins,secs))
			print("Revisions Parsed: {0:n}".format(self.revisionsParsed))
			print("Pages Parsed: {0:n}".format(self.pagesParsed))
			print("Parser Now Serving Page/Revision: {0}/{1}: {2}")\
					.format(self.currentParsePageID,self.currentParseRevisionID,self.currentParsePageTitle.encode("utf-8"))
			print("Messages:")
			for message in self.messages:
				print(message)
			
			sys.stdout.flush()
			os.fsync(sys.stdout.fileno())
			time.sleep(self.updateInterval)
			
	def getNewNotifications(self):
		while True:
			try:
				update = statusQ.pop()
			except IndexError:
				break
			else:
				try:
					if update[1]=="increment":
						setattr(self,update[0],getattr(self,update[0])+1)
					elif update[0]=="messages":
						self.messages.append(update[1])
					else:
						setattr(self,update[0],update[1])
				except AttributeError,e:
					statusQ.appendleft(("messages",e))
		
def decompressFile(path,chunksize):		
	f = bz2.BZ2File(path,"r")
	data = f.read(chunksize)
	while data != "":
		parseQ.put(data)
		data = f.read(chunksize)
	parseQ.put(None)
	f.close()

class ParseThread(Thread):
	def __init__(self,pagefile,revfile,edfile):
		Thread.__init__(self)
		self.setName("Parser")
		
	def run(self):
		global flatOutPath
		global outpath
		parser = xml.sax.make_parser()
		handler = PageHandler(flatOutPath,outpath)
		parser.setContentHandler(handler)
		
		while True:
			p = parseQ.get(True)
			if p:
				parser.feed(p)
			else:
				statusQ.appendleft(("messages",("{0} has finished working.".format(self.getName()))))
				break

def make100numbereddirs(basepath):
	for i in range(100):
		if i >= 10:
			pth = "/{0}/{1}/".format(basepath,str(i))
		else:
			pth = "/{0}/0{1}/".format(basepath,str(i))
		if not os.path.exists(pth):
		    os.makedirs(pth)

def makeCompressedTextDirs(outpath):
	make100numbereddirs(outpath)
	for i in range(100):
		if i >= 10:
			make100numbereddirs("{0}/{1}".format(outpath,i))
		else:
			make100numbereddirs("{0}/0{1}".format(outpath,i))

def launchThreads(inpath,outpath,updateInterval):
	StatusUpdater(updateInterval).start()
	p = Process(target=decompressFile,args=(inpath,10000000))
	p.start()
	pt = ParseThread("pages.dat","revisions.dat","editors.dat")
	pt.start()

if __name__=="__main__":
	assert len(sys.argv) == 4,"Usage: python {0} inputfile procid statusupdateinterval".format(sys.argv[0])
	inpath = sys.argv[1]
	procid = sys.argv[2]
	updateInterval = int(sys.argv[3])
	assert os.path.exists(inpath),"Input file does not exist"
	assert type(updateInterval) is int,"Status update interval must be an integer"
	
	makeCompressedTextDirs(outpath)
	
	if not os.path.exists(flatOutPath):
		os.makedirs(flatOutPath)
	
	statusQ.appendleft(("startTime",time.time()))
	launchThreads(inpath,outpath,updateInterval)