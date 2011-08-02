#
# $Id: test.py 2081 2009-11-18 18:13:43Z shodan $
#

import sphinxapi
from sp_vars import *
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
		
	def getMatchesByAttr(self,attr):
		''' i.e. getFieldMatches("title") returns a list cotnaining the title field for all matches.'''
		try:
			return [m["attrs"][attr] for m in self._matches]
		except KeyError:
			raise SphinxAPIException, "{0} not a valid field in this query.".format(attr)
	
	def getMatchIDs(self):
		return [m['id'] for m in self.matches()]
	
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

class SphinxClient(sphinxapi.SphinxClient):
	def __init__(self,host,\
			port=9312,\
			fieldweights={"title":5,"body":1},\
			matchmode=SPH_MATCH_EXTENDED2,\
			rankingmode=SPH_RANK_PROXIMITY_BM25,\
			sortmode=SPH_SORT_RELEVANCE):
		super(SphinxClient,self).__init__()
		self.SetServer(host,port)
		self.SetFieldWeights(fieldweights)
		self.SetMatchMode(matchmode)
		self.SetRankingMode(rankingmode)
		self.SetSortMode(sortmode)
	
	def query(self,q,index="*"): #filtervals=False,filercol=None,filtervals=None,groupby=False,groupsort=None,sortby=False,limit=None):
		# do query
		res = super(SphinxClient,self).Query ( q, index )

		if not res:
			raise SphinxAPIException, 'query failed: %s' % client.GetLastError()

		if self.GetLastWarning():
			print 'WARNING: %s\n' % self.GetLastWarning()
		
		return SphinxResult(res)

if __name__ =="__main__":
	cl = SphinxClient('dmusican41812')
	res = cl.query("Carleton College")	
	print res.warning()
	print zip([m["id"] for m in res.matches()],res.getAttributeForAllMatches("title"))
	print res.queryWords()