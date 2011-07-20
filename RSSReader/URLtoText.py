'''
Will take in an url for an article and give back and string of the main text of the article
'''
import sys, urllib, re, feedparser
from extMainText import*

def main():
    file = open("text.txt", "w")
    
    rss = "http://feeds.nytimes.com/nyt/rss/Space"
    feed = feedparser.parse(rss)
    for entry in feed.entries:
        links = entry.links
        url = ""
        for link in links:
            if re.match("http://www",link.href):
                url = link.href
                break
        if url != '':    
            text = getText(url)
            text = text.encode("utf-8")
            file.write("LINK: " + entry.link)
            try:
                file.write(text)
            except:
                print "ERROR"
                print entry.link
                file.write("SOMETHING WENT WRONG HERE") 
            else:
                file.write("\n********************************\n")
    file.close()
    
def getText(url):
    page = urllib.urlopen(url).read()
    page = unicode(page, "utf-8")
    text = extMainText(page).strip()
    # matches = re.findall("\s{2,}", text)
#     if len(matches) > 0:
#         string = max(matches, key = len)
#         index = text.find(string)
#         text = text[:index]
    return text
    
if __name__ == "__main__":
    main()
    #getText("http://www.nytimes.com/2011/05/17/science/space/17shuttle.html?ref=endeavour")