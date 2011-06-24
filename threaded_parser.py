import xml.sax
import time
import sys
import threading
import bz2
import Queue
import os
import csv
import collections

parseQ = Queue.Queue(5)
compressQ = Queue.Queue(100000)
statusQ = collections.deque()
writeQ = Queue.Queue(100000)

inpath = "../enwiki-latest-pages-articles1.xml.bz2"
#path = "/wikigroup/enwiki-20110405-pages-meta-history11.xml.bz2"
outpath = "/tmp/testoutput"

messages = []


totalCompression = 0
totalParse = 0

class PageHandler(xml.sax.handler.ContentHandler):
	def __init__(self,pageFile,revisionFile,editorFile,encoding="utf-8"):
		encodedtab = "\t".encode(encoding)
		
		csv.register_dialect("TabDelim",delimiter=encodedtab,quoting=csv.QUOTE_NONE,escapechar="\\")
		
		self.editors = {}
		
		self.pageOutputFields=["id","title","namespace","redirect"]
		self.pageWriter = csv.DictWriter(pageFile,fieldnames=self.pageOutputFields,\
										restval="",extrasaction='ignore',dialect="TabDelim")
		
		
		
		self.revisionOutputFields=["id","pageid","ed_id","ed_username","minor","timestamp","comment"]
		self.revisionWriter = csv.DictWriter(revisionFile,fieldnames=self.revisionOutputFields,\
											restval="",extrasaction='ignore',dialect="TabDelim")
		
		self.editorFile = editorFile
		self.attrs = {}
		self.revattrs = {}
		self.inRevision = False
		self.inContributor = False
		self.encoding = encoding
		self.articleRevisions = []

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
		#print(type(self.buffer))
		self.buffer.append(data)
		#pass

	def endElement(self, name):
		#print "Found a",name+":",self.buffer[:50]
		self.buffer = "".join(self.buffer)
		
		if name == "page":
			self.handleEndOfPage()
		elif name == "revision":
			self.handleEndOfRevision()
		elif self.inRevision:
			self.handleTagWithinRevision(name)
		elif name == "mediawiki":
			self.makeEditorsFile()
		else:
			self.handleTagWithinPage(name)
			
		self.buffer = []
	
	def handleTagWithinPage(self,name):		
		if name == "redirect":
			self.attrs["redirect"]="1"
		elif name == "title":
			#extracts namespace
			titleInfo = self.buffer.split(":")
			if len(titleInfo) == 2:
				#there is another namespace besides main
				self.attrs["namespace"] = titleInfo[0]
				self.attrs["title"] = titleInfo[1]
			else:
				self.attrs["namespace"] = "Main"
				self.attrs["title"] = titleInfo[0]
				
			statusQ.appendleft(("currentPageTitle",self.buffer))
		elif name == "id":
			while len(self.buffer)<4:
				self.buffer = "0"+self.buffer
			self.attrs["id"]=self.buffer
			
			statusQ.appendleft(("currentPageID",self.attrs["id"]))
		else:
			self.attrs[name]=self.buffer
		
	
	def handleEndOfPage(self):
		if "redirect" not in list(self.attrs.keys()):
			self.attrs["redirect"]="0"
		
		encodeSpecifiedDictValues(self.attrs,self.pageOutputFields,self.encoding)
		self.pageWriter.writerow(self.attrs)
		
		output = self.generatePageXML()
		
		compressQ.put((self.attrs["id"],output))
		
		statusQ.appendleft(("pagesParsed","increment"))
		self.articleRevisions=[]
		self.attrs={}
	
	def generatePageXML(self):
		xmloutput = []
		xmloutput.append("<page id='{0}'>\n".format(self.attrs["id"]))
		for rev in self.articleRevisions:
			xmloutput.append("\t<revision id='{0}'>\n".format(rev[0]))
			xmloutput.append(rev[1])
			xmloutput.append("\n\t</revision>\n")
		xmloutput.append("</page>\n")
		
		st = "".join(xmloutput)
		st = st.encode(self.encoding)
		
		return st
	
	def handleEndOfRevision(self):
		self.inRevision = False
		self.revattrs["pageid"]=self.attrs["id"]

		if "minor" not in list(self.revattrs.keys()):
			self.revattrs["minor"]="0"
		
		#if "comment" not in self.revattrs.keys():
		#	self.revattrs["comment"]=""
		
		encodeSpecifiedDictValues(self.revattrs,self.revisionOutputFields,self.encoding)
		self.revisionWriter.writerow(self.revattrs)
		
		if "ed_id" in list(self.revattrs.keys()):
			self.editors[self.revattrs["ed_username"]]=self.revattrs["ed_id"]
		else:
			try:
				self.editors[self.revattrs["ed_username"]]=""
			except KeyError as e:
				print(e)
				print("Page ID:",self.attrs["id"])
				print("Namespace:",self.attrs["namespace"])
				print("Page Title:",self.attrs["title"])
				print("Revision ID:",self.revattrs["id"])
			
		if "text" in list(self.revattrs.keys()):
			
			statusQ.appendleft(("revisionsParsed","increment"))
			
			self.articleRevisions.append((self.revattrs["id"],self.revattrs["text"]))
			
			#printQ.put("Revision {0:9} sent to compression Queue.  {1:6} Revisions Processed.".format(self.revattrs["id"],self.revisionsParsed))
			
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
			statusQ.appendleft(("currentRevisionID",self.revattrs["id"]))
		else:
			#self.escapeTabs()
			self.revattrs[name]=self.buffer
	
	
	def makeEditorsFile(self):
		encodedtab = "\t".encode(self.encoding)
		encodednewline = "\n".encode(self.encoding)
		
		for ed in self.editors.items():
			st = "{0}{1}{2}{3}\n".format(ed[0],encodedtab,ed[1],encodednewline)
			self.editorFile.write(st)
		
		statusQ.appendleft(("messages","Editors File Generation Complete"))

def encodeSpecifiedDictValues(dct,keylist,encoding):
	for key in keylist:
		if key in list(dct.keys()):
			dct[key]=dct[key].encode(encoding)

class StatusUpdater(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.setName("Status Updater Daemon")
		self.setDaemon(True)
		
		self.chunksDecompressed=0
		self.dataParsed=0
		self.filesCompressed=0
		self.revisionsParsed=0
		self.pagesParsed=0
		self.pagesWrittenOut=0
		self.startTime=0
		self.currentRevisionID=0
		self.currentPageID=0
		self.currentPageTitle=""
		self.messages=[]
		
	def run(self):
		global inpath
		
		while True:
			start = time.time()
			while True:
				try:
					update = statusQ.pop()
				except IndexError:
					break
				else:
					if update[1]=="increment":
						setattr(self,update[0],getattr(self,update[0])+1)
					elif update[0]=="messages":
						self.messages.append(update[1])
					else:
						setattr(self,update[0],update[1])
					#statusQ.task_done()
			print "Status update process took {0} seconds".format(time.time()-start)
			
			secs = int(time.time()-self.startTime)
			mins = secs%3600/60
			hrs = mins/60
			secs = secs - hrs*3600 - mins*60
			
			print("--------")
			print("Stats for File {0}".format(inpath))
			print("Run time so far: {0:02}:{1:02}:{2:02}".format(hrs,mins,secs))
			print("Chunks Decompressed: {0}.  ({1} enqueued for Parsing)".format(self.chunksDecompressed,parseQ.qsize()))
			print("Revisions Parsed: {0:n}".format(self.revisionsParsed))
			print("Pages Parsed: {0:n}".format(self.pagesParsed))
			print("Pages Compressed: {0:n}.  ({1:n} enqueued for compression)".format(self.filesCompressed,compressQ.qsize()))
			print("Pages Written Out: {0:n} ({1:n} enqueued for writing)".format(self.pagesWrittenOut,writeQ.qsize()))
			print("Now Serving Page/Revision: {0}/{1}: {2}").format(self.currentPageID,self.currentRevisionID,self.currentPageTitle.encode("utf-8"))
			print("Messages:")
			for message in self.messages:
				print(message)
				
			time.sleep(5)
			
class ContingentShutdownThread(threading.Thread):
	def __init__(self,contingentThreads):
		threading.Thread.__init__(self)
		self.contingentThreads = contingentThreads
		
	def canDie(self):
		''' Checks whether this thread is allowed to shut down, based on whether all 
			threads on which it depends have shut down
		'''
		for t in self.contingentThreads:
			if t.is_alive():
				return False
		return True
	
	def getContingentThreads(self):
		return self.contingentThreads

class FileReadDecompress(threading.Thread):
	def __init__(self,path,chunksize):
		threading.Thread.__init__(self)
		self.path = path
		self.chunksize = chunksize
		self.setName("File Reader and Decompressor")
		
	def run(self):
		global totalCompression
		
		decom = bz2.BZ2Decompressor()
		
		with open(self.path) as infile:
			# for line in infile:
			# 	dec = decom.decompress(line)
			# 	parseQ.put(dec)

			data = infile.read(self.chunksize)
			while data != "":
				sdecom = time.time()
				dec = decom.decompress(data)
				edecom = time.time()
				totalCompression += edecom-sdecom
				
				statusQ.appendleft(("chunksDecompressed","increment"))
				
				parseQ.put(dec)
				data = infile.read(self.chunksize)

class ParseThread(ContingentShutdownThread):
	def __init__(self,pagefile,revfile,edfile,contingentThreads):
		ContingentShutdownThread.__init__(self,contingentThreads)
		self.pagefile = open(pagefile,'w')
		self.revfile = open(revfile,"w")
		self.edfile = open(edfile,"w")
		self.setName("Parser")
		
		
	def run(self):
		self.runParser()
		
	def runParser(self):
		global totalParse
		
		parser = xml.sax.make_parser()
		handler = PageHandler(self.pagefile,self.revfile,self.edfile)
		parser.setContentHandler(handler)
		
		while True:
			try:
				p = parseQ.get(True,5)
				sparse = time.time()
				parser.feed(p)
				eparse = time.time()
				
				totalParse += eparse-sparse
				
			except Queue.Empty:
				if self.canDie():
					[f.close() for f in [self.pagefile,self.revfile,self.edfile]]
					statusQ.appendleft(("messages",("{0} has finished working.".format(self.getName()))))
					break
			else:
				parseQ.task_done()
		
class FileWrite(ContingentShutdownThread):
	def __init__(self,contingentThreads):
		ContingentShutdownThread.__init__(self,contingentThreads)
		self.setName("File Writer")
		
	def run(self):
		while True:
			try:
				nextFile = writeQ.get(True,5)
			except Queue.Empty:
				
				if self.canDie():
					statusQ.appendleft(("messages","{0} has finished working.".format(self.getName())))
					break
			else:
				# nextFile[0] is write path
				# nextFile[1] is file contents
				self.procFile(nextFile[0],nextFile[1])
				
	
	def procFile(self,path,content):
		global pagesWrittenOut
		f = open(path,"w")
		f.write(content)
		f.close()
		writeQ.task_done()
		# "{0:40} file written to disk.  File number {1:6}.".format(path,self.written)
		
		statusQ.appendleft(("pagesWrittenOut","increment"))		

class FileCompress(ContingentShutdownThread):
	def __init__(self,writernum,basepath,contingentThreads):
		ContingentShutdownThread.__init__(self,contingentThreads)
		self.writernum = writernum
		self.basepath = basepath
		self.setName("File Compressor {0}".format(writernum))
		
	def run(self):
		while True:
			try:
				nextFile = compressQ.get(True,5)
			except Queue.Empty:
				if self.canDie():
					statusQ.appendleft(("messages","{0} has finished working.".format(self.getName())))
					break
			else:
				# nextFile[0] should be the page id
				# nextFile[1] should be XML output for the page
			
				compressed = bz2.compress(nextFile[1])

				path = "{0}/{1}/{2}/{3}.txt.bz2".format(self.basepath,nextFile[0][:2],nextFile[0][2:4],nextFile[0])
			
				writeQ.put((path,compressed))
			
				#printQ.put("{0:9} file compressed by {1}.  File number {2:6}.".format(nextFile[0],self.writernum,processed))
				
				statusQ.appendleft(("filesCompressed","increment"))
				compressQ.task_done()
				
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

def launchThreads(inpath,outpath):
	StatusUpdater().start()
	frd = FileReadDecompress(inpath,10000000)
	frd.start()
	pt = ParseThread("pages.dat","revisions.dat","editors.dat",[frd])
	pt.start()
	for i in range(4):
		fc = FileCompress(i,outpath,[frd,pt])
		fc.start()
	FileWrite([frd,pt,fc]).start()

def main():
	global inpath
	global outpath
	makeCompressedTextDirs(outpath)
	
	statusQ.appendleft(("startTime",time.time()))

	launchThreads(inpath,outpath)
	
	time.sleep(20)
	
	parseQ.join()
	print("ParseQ Empty")
	compressQ.join()
	print("CompressQ Empty")
	writeQ.join()
	print("WriteQ Empty")
	
	print(" Runtime: " +str(time.time()-startTime) +" seconds.")
	global totalCompression
	global totalParse
	print("Compression Took {0} seconds".format(totalCompression))
	print("Parsing Took {0} secconds".format(totalParse))

main()
