import psycopg2
import sys




def prepDb(connection, cur):
    cur.execute("DROP TABLE IF EXISTS revisions")   
    cur.execute("DROP TABLE IF EXISTS pages")
    cur.execute("DROP TABLE IF EXISTS editors")
    cur.execute("CREATE TABLE revisions (revisionid integer, pageid integer, userid text, users text, minor boolean, timestamp time without time zone, comment text);")
    cur.execute("CREATE TABLE pages (pageid integer, title text, namespace text, redirect boolean);")
    cur.execute("CREATE TABLE editors (users text, editorid text);")
    connection.commit()
    

def moveToTable (connection, cursor, file, table, delimiter, nullText = ''):
    print 'starting', file
    cursor.copy_from(file, table, delimiter, nullText)
    connection.commit()
    


if __name__ == '__main__':

    #if the user ommitted host and port, default them to the unix socket and 5432 respectively
    if len(sys.argv) == 6:
        connection = psycopg2.connect(database = sys.argv[3], user = sys.argv[4], password = sys.argv[5])
    else:
        connection = psycopg2.connect(database = sys.argv[3], user = sys.argv[4], password = sys.argv[5], host = sys.argv[6], port = sys.argv[7])

    cursor = connection.cursor()
    
    prepDb(connection, cursor)
        
    for i in range (1, int(sys.argv[1]) + 1):
        j = 0
        while True:
            try:
                myFile = open(sys.argv[2] + 'ed' + str(j) + '_' + str(i) + '.dat')
                moveToTable(connection, cursor, myFile, 'editors', '\t')
                myFile = open (sys.argv[2] + 'rv' + str(j) + '_' + str(i) + '.dat')
                moveToTable(connection, cursor, myFile, 'revisions', '\t')
                myFile = open (sys.argv[2] + 'pg' + str(j) + '_' + str(i) + '.dat')
                moveToTable(connection, cursor, myFile, 'pages', '\t')
                j += 1
            except IOError:
                i+=1
                break
                    
    
    
    print 'ALL DONE'
    connection.close()