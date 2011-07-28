#
# $Id: test.py 2081 2009-11-18 18:13:43Z shodan $
#

from sphinxapi import *
import sys, time

class SphinxAPIException(Exception):
	pass

class SphinxResult(object):
	def __init__(self,resultdict):
		for key,value in resultdict.iteritems():
			setattr(self,"_"+key,value)
			
		#Sanity Check
		[getattr(self,key) for key in \
			['_status','_matches','_fields','_time','_total_found','_warning','_attrs','_words','_error','_total']]
	
	def hadError(self):
		return self._error!=""
		
	def error(self):
		return self._error
		
	def hadWarning(self):
		return self._warning!=""
		
	def warning(self):
		return self._warning
		
	def matches(self):
		return self._matches
		
	def getAttributeForAllMatches(self,attr):
		try:
			return [m["attrs"][attr] for m in self._matches]
		except KeyError:
			raise SphinxAPIException, "{0} not a valid field in this query.".format(attr)
			
	def queryFields(self):
		return self._fields
		
	def queryAttrs(self):
		return self._attrs
	
	def queryWords(self):
		return self._words
	
	def time():
		return self._time
	
	def __len__(self):
		return self._total_found

def initializeClient(host,port=9312,fieldweights={"title":4,"body":1},mode=SPH_MATCH_EXTENDED):
	cl = SphinxClient()
	cl.SetServer(host, port)
	cl.SetFieldWeights(fieldweights)
	cl.SetMatchMode(mode)
	return cl
	
def query(client,q): #filtervals=False,filercol=None,filtervals=None,groupby=False,groupsort=None,sortby=False,limit=None):
	# do query
	res = client.Query ( q, "articletext_sample" )

	if not res:
		raise SphinxAPIException, 'query failed: %s' % client.GetLastError()

	if client.GetLastWarning():
		print 'WARNING: %s\n' % client.GetLastWarning()
		
	return SphinxResult(res)

if __name__ =="__main__":
	cl = initializeClient('dmusican41812')
	res = query(cl,"Carleton College")	
	print res.warning()
	print zip([m["id"] for m in res.matches()],res.getAttributeForAllMatches("title"))
	print res.queryWords()

# print 'Query \'%s\' retrieved %d of %d matches in %s sec' % (q, res['total'], res['total_found'], res['time'])
# print 'Query stats:'
# 
# if res.has_key('words'):
# 	for info in res['words']:
# 		print '\t\'%s\' found %d times in %d documents' % (info['word'], info['hits'], info['docs'])
# 
# if res.has_key('matches'):
# 	n = 1
# 	print '\nMatches:'
# 	for match in res['matches']:
# 		attrsdump = match['attrs']
# 		print attrsdump["title"]
# 			
# 	#	print '%d. doc_id=%s, weight=%d%s' % (n, match['id'], match['weight'], attrsdump)
# 		n += 1

#
# $Id: test.py 2081 2009-11-18 18:13:43Z shodan $
#
