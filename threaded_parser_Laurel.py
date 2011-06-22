import xml.sax
import time
import sys
import threading
import bz2
import Queue
import os

# workq = Queue.Queue()
# l = threading.Lock()
# processed=0


class PageHandler(xml.sax.handler.ContentHandler):
	def __init__(self,pageFile,revisionFile,editorFile,encoding="utf-8"):
		self.editors = {} #dictionary where keys are usernames and values are id's or ""
		self.pageFile = pageFile
		self.revisionFile = revisionFile
		self.editorFile = editorFile
		self.attrs = {} #page tags are keys and text is value
		self.revattrs = {} #revision tags are keys and text is value
		self.inRevision = False
		self.inContributor = False
		self.encoding = encoding

	def startElement(self, name, attributes):
		if name =="revision":
			self.inRevision = True
		elif name =="contributor":
			self.inContributor = True
		self.buffer= ""

	def characters(self, data):
		self.buffer += data

	def endElement(self, name):
		#print "Found a",name+":",self.buffer[:50]
		
		if name == "page":
			self.handleEndOfPage()
		elif name == "revision":
			self.handleEndOfRevision()
		elif self.inRevision:
			self.handleTagWithinRevision(name)
		elif name == "mediawiki":
			#reached end of file
			self.makeEditorsFile()
		else:
			self.handleTagWithinPage(name)
	
	def handleTagWithinPage(self,name):
		if name == "redirect":
			self.attrs["redirect"] = "1"
		elif name == "title":
		    self.escapeTabs()
		    #extracts namespace
		    titleInfo = self.buffer.split(":")
		    if len(titleInfo) == 2:
		        #there is another namespace besides main
		        self.attrs["namespace"] = titleInfo[0]
		        self.attrs["title"] = titleInfo[1]
		    else:
		        self.attrs["namespace"] = "Main"
		        self.attrs["title"] = titleInfo[0]  
		else:
			self.attrs[name]=self.buffer
		
	
	def handleEndOfPage(self):
		
		if "redirect" not in self.attrs.keys():
			self.attrs["redirect"]="0"
		writeSpecifiedDictValuesToFile(self.attrs,["id","title","namespace", "redirect"],self.pageFile,self.encoding)
		#writeDictValsToFile(self.attrs,self.pageFile)
		
		self.attrs={}
	
	def handleEndOfRevision(self):
		
		self.inRevision = False
		self.revattrs["pageid"]=self.attrs["id"]

		if "minor" not in self.revattrs.keys():
			self.revattrs["minor"]=0
		
		if "comment" not in self.revattrs.keys():
			self.revattrs["comment"]=""
		writeSpecifiedDictValuesToFile(self.revattrs,["id","pageid","ed_id","ed_username","minor","timestamp","comment"],self.revisionFile,self.encoding)
		
		self.editors[self.revattrs["ed_username"]]=self.revattrs["ed_id"]
		
		#if "text" in self.revattrs.keys():
			#print self.revattrs["id"]+"++"+self.revattrs["text"]+"++"
			#workq.put((self.attrs["title"],self.revattrs["id"],self.revattrs["text"]))
		
		self.revattrs={}
	
	def handleTagWithinRevision(self,name):
		if name == "timestamp":
			self.revattrs["timestamp"] = self.buffer[0:10]+" "+self.buffer[11:-1]
		elif name == "comment":
		    self.escapeTabs()
		elif name == "minor":
			self.revattrs["minor"]=1
		elif name == "contributor":
			self.inContributor = False
		elif self.inContributor:
			#name is id, ip, or username
			if name == "username":
			    self.escapeTabs()
			    self.revattrs["ed_username"] = self.buffer
			if name == "ip":
			    self.revattrs["ed_username"] = self.buffer
			    self.revattrs["ed_id"] = ""
			else:
			    #name = "id"
			    self.revattrs["ed_id"]=self.buffer
		else:
			#handles id and text
			self.revattrs[name]=self.buffer
	
	def escapeTabs(self):
	    #changes \t to \\t for postgres
	    self.buffer = self.buffer.replace("\t", "\\t")
	
	def makeEditorsFile(self):
		for ed in self.editors.iteritems():
			st = ed[0]+u"\t"+ed[1]+u"\n" #storing username \t userid ("" if username is ip)
			st = st.encode(self.encoding)
			self.editorFile.write(st)
	
def writeSpecifiedDictValuesToFile(d,vals,f,encoding):
	#Input: dictionary, list of columns, file of page to write to, encoding
	for a in vals:
		if a in d.keys():
			if not isinstance(d[a],basestring):
				d[a]=str(d[a])
			st = d[a].encode(encoding)
			f.write(st)
		f.write(u"\t")
	f.write(u"\n")
	
class FileWrite(threading.Thread):
	def __init__(self,writernum):
		threading.Thread.__init__(self)
		self.writernum = writernum
		
	def run(self):
		global processed
		while True:
			try:
				nextFile = workq.get(True,5)
			except Queue.Empty:
				print "{0} has had nothing to do for five seconds... terminating.".format(self.getName())
				break
			else:
				# nextFile[0] should be the page title
				# nextFile[1] should be the revision ID
				# nextFile[2] should be the contents of the file
			
				compressed = bz2.compress(nextFile[2])
				#noslashes = nextFile[0].replace("/","-")
				#path = "testoutput/"+noslashes+"-"+nextFile[1]+".txt.bz2"
				path = "testoutput/"+nextFile[1]+".txt.bz2"
				f = open(path,"w")

				f.write(compressed)
				
				l.acquire()
				print "{0} file written by {1}.  File number {2}.\n".format(path,self.writernum,processed)
				processed+=1
				l.release()
				
				f.close()
				workq.task_done()
				
	
def main():
	
	pagefile = open("pages.dat","w")
	revfile = open("revisions.dat","w")
	edfile = open("editors.dat","w")
	
	start = time.clock()
		
	parser = xml.sax.make_parser()
	handler = PageHandler(pagefile,revfile,edfile)
	parser.setContentHandler(handler)
	
# 	for i in range(30):
# 		FileWrite(i).start()
# 	
# 	print "Past that"
	
	parser.parse("enwiki-latest-pages-meta-current1.xml")
	
	print "TIME: " + str(time.clock() - start)
	
	pagefile.close()
	revfile.close()
	edfile.close()
	
	#[f.close() for f in [pagefile,revfile,edfile]]

	# for page in allpages.values():
	# 	print "--Begin New Page--"
	# 	for attr in page.keys():
	# 		print attr+":",page[attr]
	# 	
	# for rev in allrevisions.values():
	# 	print "--Begin New Revision--"
	# 	for attr in rev.keys():
	# 		if attr =="text":
	# 			print attr+":",rev[attr][:50]+"..."
	# 		else:
	# 			print attr+":",rev[attr]
	
		
# import cProfile
# cProfile.run("main()")

main()