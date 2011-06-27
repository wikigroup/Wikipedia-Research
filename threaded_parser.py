import xml.sax
import time
import sys
import threading
import bz2
import Queue
import os
import csv
import collections
from multiprocessing import Process, Pipe, Pool

parseQ = Queue.Queue(5)
compressQ = Queue.Queue(10000)
statusQ = collections.deque()
writeQ = Queue.Queue(10000)

#inpath = "../enwiki-latest-pages-articles1.xml.bz2"
inpath = sys.argv[1]
# inpath= "/wikigroup/enwiki-20110405-pages-meta-history11.xml.bz2"
outpath = "/tmp/testoutput"
flatOutPath = "/tmp/flatoutput"
procid = sys.argv[2]

messages = []

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
		self.writerPool = Pool(processes=10)
		
		
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
			while len(self.buffer)<4:
				self.buffer = "0"+self.buffer
			self.attrs["id"]=self.buffer
			self.openCompressedOutputFile()
			statusQ.appendleft(("currentParsePageID",self.attrs["id"]))
		else:
			self.attrs[name]=self.buffer
	
	def openCompressedOutputFile(self):
		global writerReceiver
		
		path = "{0}/{1}/{2}/{3}.txt.bz2"\
			.format(self.textOutputPath,self.attrs["id"][:2],self.attrs["id"][2:4],self.attrs["id"])
		
		self.toWriter, writerReceiver = Pipe()
		
		#while active_children()<5:
		#	thread.sleep(.01)
			
		#p = Process(target=pipedFileWriter,args=(writerReceiver,path)).start()
			
		self.writerPool.apply_async(pipedFileWriter,(path))
		self.toWriter.send("<page id='{0}'>\n".format(self.buffer))
		
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
		
		self.toWriter.send("\t</page>")
		self.toWriter.close()
		
		
		# if bigfile:
		# 	statusQ.appendleft(("messages","{0} ({1}) is going to use the streaming compressor.  Delays possible."\
		# 			.format(self.attrs["id"],self.attrs["title"].encode(self.encoding))))
		
		if self.revisionsSinceLastWriteOut > self.writeOutInterval:
			self.writeOutIntermediateResults()
		
		statusQ.appendleft(("pagesParsed","increment"))
		self.attrs={}
		time.sleep(.1)
	
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
			self.toWriter.send(self.generateRevisionXML())
			
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
			#self.escapeTabs()
			self.revattrs[name]=self.buffer

def pipedFileWriter(filePath):
	f = bz2.BZ2File(filePath,"w")
	
	while True:
		try:
			toWrite = writerReceiver.recv()
		except EOFError:
			f.close()
			break
		else:
			f.write(toWrite)
		
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
		self.revisionsWrittenOut=0
		self.startTime=0
		self.currentParseRevisionID=0
		self.currentParsePageID=0
		self.currentParsePageTitle=""
		self.currentCompressPageID=0
		self.messages=[]
		
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
			print("Chunks Decompressed: {0}.  ({1} enqueued for Parsing)"\
					.format(self.chunksDecompressed,parseQ.qsize()))
			print("Revisions Parsed: {0:n}".format(self.revisionsParsed))
			print("Pages Parsed: {0:n}".format(self.pagesParsed))
			print("Pages Compressed: {0:n}.  ({1:n} enqueued for compression)"\
					.format(self.filesCompressed,compressQ.qsize()))
			print("Revisions Written Out: {0:n} ({1:n} enqueued for writing)"\
					.format(self.revisionsWrittenOut,writeQ.qsize()))
			print("Parser Now Serving Page/Revision: {0}/{1}: {2}")\
					.format(self.currentParsePageID,self.currentParseRevisionID,self.currentParsePageTitle.encode("utf-8"))
			print("Compressor Now Serving Page: {0}").format(self.currentCompressPageID)
			print("Messages:")
			for message in self.messages:
				print(message)
			
			sys.stdout.flush()
			os.fsync(sys.stdout.fileno())
			
			time.sleep(5)
			
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
		decom = bz2.BZ2Decompressor()
		
		with open(self.path) as infile:
			data = infile.read(self.chunksize)
			while data != "":
				dec = decom.decompress(data)
				statusQ.appendleft(("chunksDecompressed","increment"))
				parseQ.put(dec)
				data = infile.read(self.chunksize)

class ParseThread(ContingentShutdownThread):
	def __init__(self,pagefile,revfile,edfile,contingentThreads):
		ContingentShutdownThread.__init__(self,contingentThreads)
		self.setName("Parser")
		
		
	def run(self):
		self.runParser()
		
	def runParser(self):
		global flatOutPath
		global outpath
		parser = xml.sax.make_parser()
		handler = PageHandler(flatOutPath,outpath)
		parser.setContentHandler(handler)
		
		while True:
			try:
				p = parseQ.get(True,5)
				parser.feed(p)
			except Queue.Empty:
				if self.canDie():
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
				# nextFile[1] should be XML output for the revision, etc...
				# nextFile[2] should be whether or not we need to toggle the "openness" of the file
				
				path = "{0}/{1}/{2}/{3}.txt.bz2".format(self.basepath,nextFile[0][:2],nextFile[0][2:4],nextFile[0])
				f = bz2.BZ2File(path,"w")
				f.write(nextFile[1])
				f.close()
				
				statusQ.appendleft(("filesCompressed","increment"))
				statusQ.appendleft(("currentCompressPageID",nextFile[0]))
				statusQ.appendleft(("revisionsWrittenOut","increment"))
				
				compressQ.task_done()

	# def writeToFile(self,path,content,toggleOpen):
	# 	incremental = bz2.BZ2File(path,"a")
	# 	index = 0
	# 	st = content[index:index+20000]
	# 	while st != "":
	# 		incremental.write(st)
	# 		index += 20000
	# 		st= content[index:index+20000]
	# 	
	# 	incremental.close()
	# 	
	# 	statusQ.appendleft(("filesCompressed","increment"))
		
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
	# for i in range(4):
	# 	fc = FileCompress(i,outpath,[frd,pt])
	# 	fc.start()
	#FileWrite([frd,pt,fc]).start()

def main():
	global inpath
	global outpath
	global flatOutPath
	makeCompressedTextDirs(outpath)
	
	if not os.path.exists(flatOutPath):
		os.makedirs(flatOutPath)
	
	statusQ.appendleft(("startTime",time.time()))

	launchThreads(inpath,outpath)

main()
