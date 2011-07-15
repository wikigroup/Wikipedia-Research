import psycopg2
import time
import calais
from operator import itemgetter
apikey = "pqwxutrgjk8zhk5geutnhpyj"
connection = calais.Calais(apikey)

def getCalaisTags(url,limit=10):
	result = connection.analyze_url(url)
	topics = getTopics(result)
	topics.sort(reverse=True,key=itemgetter(2))
	return topics[:limit]

def getTopics(resultobj):
	if not hasattr(resultobj, "entities"):
		return None
	return [(item['_type'], item['name'], item['relevance']) for item in resultobj.entities]

def reformMultiwordTopic(t):
	words = t.split(" ")
	if len(words)>1:
		return "("+" & ".join(words)+")"
	else:
		return t

def tagsToQueryString(taglist):
	topics = [t[1] for t in taglist]
	topics = [reformMultiwordTopic(t) for t in topics]
	query = " | ".join(topics)
	return query

def getWikipagesForTextQuery(query):
	print query
	connection = psycopg2.connect(database = "wikigroup", user = "postgres", password = "wiki!@carl", host = "floyd", port = "5432")
	cursor = connection.cursor()

	start = time.time()
	cursor.execute("""
	  select P.title, A.pageid, ts_rank_cd(vect, q) as rank
	  from atext4 A, to_tsquery('english', %s) q, pages P
	  where vect @@ q and A.pageid=P.pageid
	  order by rank desc
	  limit 10;""",(query,))

	connection.commit()

	print "Query took {0:.2f} seconds".format(time.time()-start)

	a = cursor.fetchone()
	print a
	while a != None:
	    a = cursor.fetchone()
	    print a

	connection.close()

tags = getCalaisTags("http://www.cnn.com/2011/SPORT/football/07/13/football.wc.semis.usa/index.html")
query = tagsToQueryString(tags)
getWikipagesForTextQuery(query)