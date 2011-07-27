import psycopg2,re

connection = psycopg2.connect(database = "wikigroup", user = "postgres", password = "wiki!@carl", host = "floyd", port = "5432")

cursor = connection.cursor()
cursor.execute("SELECT pageid,title,body from atext_sample;")

pageid,title,body = cursor.fetchone()
while True:
	res = re.match("#REDIRECT *\[\[(.*)\]\]",body)
	if res is not None:
		redir = res.group(1)
		print "Page {0} ({1}) redirects to {2}.".format(pageid,title,redir)
		
	try:
		pageid,title, body = cursor.fetchone()
	except TypeError:
		break
		
cursor.close()
connection.close()