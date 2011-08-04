import urllib, feedparser
from extMainText import *
import tfidf
import calais
import sphinx_inter
from sphinx_vars import *
from operator import itemgetter

calaisAPI = "pqwxutrgjk8zhk5geutnhpyj"
cc = calais.Calais(calaisAPI)

class TestResults(object):
	def __init__(self):
		self._rankList = []
		self._numTested = 0
		self._successes = []
		self._failures = []
		self._errors = []

	def addResult(self, rank,pageid,isError=False):
		self._numTested += 1
		if isError:
			self._errors.append(pageid)
		elif rank != None:
			self._rankList.append(rank)
			self._successes.append(pageid)
		else:
			self._failures.append(pageid)
	
	def getRankList(self):
		return self._rankList

	def getTotRank(self):
		return self._totRank

	def getNumTested(self):
		return self._numTested

	def getAverageRank(self):
		if len(self.getRankList()) > 0:
			return sum(self.getRankList())/float(len(self.getRankList()))
		else:
			return 0
			
	def getMedianRank(self):
		return sorted(self.getRankList())[len(self.getRankList())/2+1]

	def getSuccesses(self):
		return self._successes
		
	def numSuccesses(self):
		return len(self.getSuccesses())

	def getFailures(self):
		return self._failures
		
	def numFailures(self):
		return len(self.getFailures())
		
	def getErrors(self):
		return self._errors
		
	def numErrors(self):
		return len(self.getErrors())
		
	def printSummary(self):
		print "Tested: {0}".\
					format(self.getNumTested())
		print "Successes: {0} ({1:.2%})".\
					format(self.numSuccesses(),self.numSuccesses()/float(self.getNumTested()))
		print "Failures: {0}".format(self.numFailures())
		print "Errors: {0}".format(self.numErrors())
		print "Ranks: {0}".format(sorted(self.getRankList()))
		print "Average Rank: {0:.3}".format(self.getAverageRank())
		print "Median Rank: {0}".format(self.getMedianRank())
		
def getTopics(resultobj):
	if hasattr(resultobj, "entities"):
		return [(item['_type'], item['name'], item['relevance']) for item in resultobj.entities]
	else:
		return []
	
def importTestCites(path):
	f = open(path)
	page = {}
	
	for line in f:
		url,pageid = line.split("\t")
		page[url.strip()]=int(pageid.strip())
		
	return page
	
def getText(url):
	page = urllib.urlopen(url).read()
	page = unicode(page, "utf-8")
	text = extMainText(page).strip()
	return text

def removeSphinxReservedChars(word):
	word= word.lstrip("!@^-*'")
	word = word.rstrip("$'")
	return word

def getTextFromURL(url):
	try:
		text = getText(url)
	except IOError,e:
		print e
		return None
	except UnicodeDecodeError,e:
		print e
		return None
	else:
		return text

def getCalaisTags(cc,url,limit=10):
	result = cc.analyze(url)
	topics = getTopics(result)
	topics.sort(reverse=True,key=itemgetter(2))
	return topics[:limit]

def reformMultiwordTopic(t):
	#words = t.split(" ")
	if len(t)>1:
		return "( " + t + " )"
	else:
		return t

def tagsToQueryString(taglist):
	topics = [t[1] for t in taglist]
	topics = [reformMultiwordTopic(t) for t in topics]
	query = " | ".join(topics)
	return query


def cleanText(tfidfobject,wordList):
	wordList = [removeSphinxReservedChars(word) for word in wordList]
	wordList = [word for word in wordList if not word.isdigit() and len(word)>1]
	wordList = tfidfobject.remove_stopwords(wordList)
	
	return wordList

def getTFIDFQuery(tfidfobject,text,numwords=10):
	words = tfidfobject.get_doc_keywords(text)
	words = [word[0] for word in words]
	words = cleanText(tfidfobject,words)[:numwords]

	return " | ".join(set(words))

def getFirstNWordsQuery(tfidfobject,text,numwords=100):
	words = tfidfobject.get_tokens(text)[:numwords]
	words = cleanText(tfidfobject,words)
	return " | ".join(words)
	
def getCalaisQuery(tfidfobject,text,numresults=10):
	tags = getCalaisTags(cc,text)
	return tagsToQueryString(tags)

def cPrint(s,boolexpr):
	if boolexpr:
		print s

def runQuery(query,pageid,maxrank,sphinxclient,verbose=False):
	error = False
	rank = None
	
	try:
		results = sphinxclient.query(query)
	except sphinx_inter.SphinxAPIException,e:
		print "Error: {0}".format(e)
		error = True
	else:
		ids = results.getMatchIDs()[:maxrank]
		cPrint("Results: {0}".format(ids),verbose)
		cPrint("Correct: {0}".format(pageid),verbose)
		
		if pageid in ids:
			cPrint("Rank: {0}".format(ids.index(pageid)),verbose)
			rank = ids.index(pageid)
		else:
			cPrint("Rank: {0}".format(None),verbose)
			
	return (rank,pageid,error)
	

def testCitation(pageid,text,queryGen,maxrank,sphinxclient,tfidfobject,verbose=False):
	cPrint("----------",verbose)
	rank = None
	queryExecutionError = False
	queryGenerationError = False
	
	try:
		q = queryGen(tfidfobject,text)
	except ValueError as e:
		queryGenerationError = True
		print "Error Generating Query: {0}".format(e)
	else:
		cPrint("Query: {0}".format(q),verbose)
		rank,pageid,queryExecutionError = runQuery(q,pageid,maxrank,sphinxclient,verbose)
	finally:
		return (rank,pageid,queryGenerationError or queryExecutionError)

def main():
	testpath = "/Accounts/groenemm/summer/repo/tfidf/cachedtexts"
	testfiles =	 os.listdir(testpath)
	maxrank = 10
	verbose = True
	t = tfidf.TfIdf(corpus_filename="reformattedfreqlist.txt",stopword_filename="stopwords.txt")
	sc = sphinx_inter.SphinxClient("dmusican41812",rankingmode=SPH_RANK_BM25,fieldweights={"title":4, "body":1})
	#tests = importTestCites("citelist.txt")
	testresults = TestResults()
	

	for path,pageid in [(os.path.join(testpath,testfile),int(testfile)) for testfile in testfiles]:
		with open(path) as f:
			text = f.read()

		if text == None or text == "":
			continue
		
		testresults.addResult(*testCitation(pageid,text,getCalaisQuery,maxrank,sc,t,verbose))

	testresults.printSummary()
		
if __name__ == "__main__":
	main()	
	# rss = "feed://www.startribune.com/local/index.rss2"
	# feed = feedparser.parse(rss)
	# links = [entry.link for entry in feed.entries]
