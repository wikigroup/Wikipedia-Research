import bz2
from wmf import dump
path = "../enwiki-20110405-pages-meta-current1.xml.bz2"

fp = bz2.BZ2File(path,"r")
dumpIterator = dump.Iterator(fp)

for page in dumpIterator.readPages():
	print(page.getText())