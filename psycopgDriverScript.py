

import psycopg2

class dbConnection:
    '''Object which represents the connection between python and postgreSQL.  Takes as input the database you would like to connect to, the account under which you would like to connect, the password for that account, the host for the database, and the port number (all as strings).  Then establishes a connection'''
    def __init__(self, db, account, password, host, port):
        self.connection = psycopg2.connect(database = db, user = account, password = password, host = host, port = port)
        
    def moveToTable(self, file, table, delimiter, nullText = ''):
        '''takes as input a file, a pre-existing table in the database, a delimeter and optionally a value specifying how the null value is represented in the file text (default is \N).  Adds the text from the file to the specified table in the db.'''
        cursor = self.connection.cursor()
        cursor.copy_from(file, table, delimiter, nullText)
        self.connection.commit()
        cursor.close()
        
    def killConnection(self):
        self.connection.close()
    
    
if __name__ == '__main__':
    connection = dbConnection('wikigroup', 'wiki', 'heydave', 
    'localhost', '5432')
    
    #EditorFiles = 3
    #for i in range (EditorFiles):
    #   myFile = open('ed' + str(i) + '_12.dat')
    #    connection.moveToTable(myFile, 'editors', '\t')
    
    #RevFiles = 1
    #for i in range (RevFiles):
    #    myFile = open ('rv' + str(i) + '_12.dat')
    #    connection.moveToTable(myFile, 'revisions', '\t')
        
    PageFiles = 1
    for i in range(PageFiles):
        myFile = open ('pg' + str(i) + '_12.dat')
        connection.moveToTable(myFile, 'pages', '\t')
        
    connection.killConnection()