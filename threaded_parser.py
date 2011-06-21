import xml.sax
import time
import sys
import threading
import bz2
import Queue
import os
import cProfile


parseQ = Queue.Queue(5)
compressQ = Queue.Queue()
printQ = Queue.Queue()
l = threading.Lock()
printLock = threading.Lock()
processed=0
totalCompression = 0
totalParse = 0
totalWrite = 0
totalEnQ = 0

class PageHandler(xml.sax.handler.ContentHandler):
	def __init__(self,pageFile,revisionFile,editorFile,encoding="utf-8"):
		self.editors = {}
		self.pageFile = pageFile
		self.revisionFile = revisionFile
		self.editorFile = editorFile
		self.attrs = {}
		self.revattrs = {}
		self.inRevision = False
		self.inContributor = False
		self.encoding = encoding
		self.revisionsParsed= 0

	def startElement(self, name, attributes):
		if name =="revision":
			self.inRevision = True
		elif name =="contributor":
			self.inContributor = True
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
		else:
			self.attrs[name]=self.buffer
		
	
	def handleEndOfPage(self):
		
		if "redirect" not in self.attrs.keys():
			self.attrs["redirect"]="0"
		
		
		writeSpecifiedDictValuesToFile(self.attrs,["id","title","redirect"],self.pageFile,self.encoding)
		#writeDictValsToFile(self.attrs,self.pageFile)
		
		self.attrs={}
	
	def handleEndOfRevision(self):
		
		self.inRevision = False
		self.revattrs["pageid"]=self.attrs["id"]

		if "minor" not in self.revattrs.keys():
			self.revattrs["minor"]=0
		
		if "comment" not in self.revattrs.keys():
			self.revattrs["comment"]=""
		
		writeSpecifiedDictValuesToFile(self.revattrs,["id","pageid","ed_id","ed_ip","minor","timestamp","comment"],self.revisionFile,self.encoding)
		
		if "ed_id" in self.revattrs.keys():
			self.editors[self.revattrs["ed_id"]]=self.revattrs["ed_username"]
		
		if "text" in self.revattrs.keys():
			
			self.revisionsParsed+=1
			printQ.put("Revision {0:9} sent to compression queue.  {1:6} Revisions Processed.".format(self.revattrs["id"],self.revisionsParsed))
			
			title = self.attrs["title"].encode(self.encoding)
			text = self.revattrs["text"].encode(self.encoding)
			
			global totalEnQ
			sEnQ = time.time()
			compressQ.put((title,self.revattrs["id"],text))
			eEnQ = time.time()
			totalEnQ += eEnQ - sEnQ
			
		self.revattrs={}
	
	def handleTagWithinRevision(self,name):
		if name == "timestamp":
			self.revattrs["timestamp"] = self.buffer[0:10]+" "+self.buffer[11:-1]
		elif name == "minor":
			self.revattrs["minor"]=1
		elif name == "contributor":
			self.inContributor = False
		elif self.inContributor:
			self.revattrs["ed_"+name]=self.buffer
		else:
			self.revattrs[name]=self.buffer
	
	def makeEditorsFile(self):
		for ed in self.editors.iteritems():
			st = ed[0]+u"\t"+ed[1]+u"\n"
			st = st.encode(self.encoding)
			self.editorFile.write(st)
	
def writeSpecifiedDictValuesToFile(d,vals,f,encoding):
	global totalWrite
	
	swrite = time.time()
	for a in vals:
		if a in d.keys():
			if not isinstance(d[a],basestring):
				d[a]=str(d[a])
			st = (d[a]+u"\t").encode(encoding)
			#f.write(st)
		f.write(st)
	f.write(u"\n")
	totalWrite+=time.time()-swrite

class PrintThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.setName("Printer")
		self.setDaemon(True)
		
	
	def run(self):
		while True:
			try:
				nextToPrint = printQ.get(True)
			except Queue.Empty:
				break
			else:
				print nextToPrint
				printQ.task_done()

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
				
				
				parseQ.put(dec)
				data = infile.read(self.chunksize)

class ParseThread(threading.Thread):
	def __init__(self,pagefile,revfile,edfile):
		threading.Thread.__init__(self)
		self.pagefile = open(pagefile,"w")
		self.revfile = open(revfile,"w")
		self.edfile = open(edfile,"w")
		self.setName("Parser")
		
	def run(self):
		self.runParser()
		#cProfile.run("self.runParser()")
		
	def runParser(self):
		parser = xml.sax.make_parser()
		handler = PageHandler(self.pagefile,self.revfile,self.edfile)
		parser.setContentHandler(handler)
		
		#decom = bz2.BZ2Decompressor()
		
		# with open("enwiki-latest-stub-articles1.xml.bz2") as infile:
		# 	data = infile.read(1000000)
		# 	while data != "":
		# 		data = decom.decompress(data)
		# 		parser.feed(data)
		# 		data = infile.read(1000000)
		 
	#
		# for line in infile:
		# 	dec = decom.decompress(line)
		# 	parser.feed(dec)
		
		
		while True:
			try:
				printQ.put("Parser Wating...")
				p = parseQ.get(True,10)
				printQ.put("Parser Got Work..")
				sparse = time.time()
				parser.feed(p)
				eparse = time.time()
				
				global totalParse
				totalParse += eparse-sparse
				printQ.put("Another 10MB Parsed!")
				
			except Queue.Empty:
				[f.close() for f in [self.pagefile,self.revfile,self.edfile]]
				printQ.put("{0} has had nothing to do for 10 seconds... terminating.".format(self.getName()))
				break
			else:
				parseQ.task_done()
		

class FileWrite(threading.Thread):
	def __init__(self,writernum):
		threading.Thread.__init__(self)
		self.writernum = writernum
		self.setName("File Writer {0}".format(writernum))
		
	def run(self):
		global processed
		while True:
			try:
				nextFile = compressQ.get(True,5)
			except Queue.Empty:
				printLock.acquire()
				printQ.put("{0} has had nothing to do for five seconds... terminating.".format(self.getName()))
				printLock.release()
				break
			else:
				# nextFile[0] should be the page title
				# nextFile[1] should be the revision ID
				# nextFile[2] should be the contents of the file
			
				compressed = bz2.compress(nextFile[2])

				path = "/wikigroup/testoutput/"+nextFile[1][:2]+"/"+nextFile[1][2:]+".txt.bz2"
				f = open(path,"w")

				f.write(compressed)
			
				l.acquire()
				printQ.put("{0:30} file written by {1}.  File number {2:6}.".format(path,self.writernum,processed))
				processed+=1
				l.release()
				
				f.close()
				compressQ.task_done()
				
	
def main():
#	import yappi
	
	outpath = "/wikigroup/testoutput/"
	
	for i in range(100):
		if i >= 10:
			pth = outpath+str(i)+"/"
		else:
			pth =outpath +"0"+str(i)+"/"
		if not os.path.exists(pth):
		    os.makedirs(pth)
	
	start = time.time()

	PrintThread().start()
	
	path = "/wikigroup/enwiki-latest-pages-articles1.xml.bz2"
#	path = "enwiki-20100130-pages-meta-history.xml.bz2"
#	path = "enwiki-latest-stub-articles1.xml.bz2"
	
#	yappi.start()
	
	FileReadDecompress(path,10000000).start()
	ParseThread("pages.dat","revisions.dat","editors.dat").start()

	for i in range(40):
			FileWrite(i).start()
		
	
	time.sleep(5)
	
	parseQ.join()
	compressQ.join()
	
#	stats = yappi.get_stats()
#	for stat in stats:
#		print stat
#	yappi.stop()
	
	
	print " Runtime: " +str(time.time()-start) +" seconds."
	global totalCompression
	global totalParse
	global totalWrite
	global totalEnQ
	print "Compression Took {0} seconds".format(totalCompression)
	print "Parsing Took {0} secconds".format(totalParse)
	print "Writing Took {0} seconds".format(totalWrite)
	print "EnQ Took {0} seconds".format(totalEnQ)

main()
