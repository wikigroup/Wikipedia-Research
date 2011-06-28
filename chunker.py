import bz2,time

path = "/wikigroup/enwiki-latest-pages-articles1.xml.bz2"
decom = bz2.BZ2Decompressor()
recom = bz2.BZ2Compressor()

f = open(path)

# cont = [decom.decompress(f.read(100000000))]
# 
# cline = f.readline()
# look = decom.decompress(cline)
# cont.append(look)
# while "</page>" not in look:
# 	print look
# 	cline = f.readline()
# 	look = decom.decompress(cline)
# 	cont.append(look)
# 
# alladditional = "".join(cont[1:])
# compressed = recom.compress(alladditional)
# 
# o = open("/wikigroup/chunk.xml.bz2","w")
# o.write(alladditional+compressed)

for line in f:
	print decom.decompress(line)
#	time.sleep(.01)