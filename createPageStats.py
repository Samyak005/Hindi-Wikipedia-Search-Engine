# Indexer4 - removed reduntant code for performance optimisation
import sys
import heapq
from collections import defaultdict
import re
import os
import xml.sax
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer as Stemmer
import timeit
import string

stemmer = Stemmer('english')
NUMPAGES_IN_PREINDEX = 20000
DICTIONARYSUBDIV = 50000
debug = 0
dictID = {}

# global number_of_unique_unstemmed_words
# number_of_unique_unstemmed_words = set()

start = timeit.default_timer()

stopWords = set(stopwords.words('english'))
stop_dict = defaultdict(int)
for word in stopWords:
    stop_dict[word] = 1

fileCount = 0
pageCount = 0
offset = 0

#64 base mapping of integers - encode and decode requires following strings
ALPHABET ='+' + string.digits+  string.ascii_uppercase + '_' + string.ascii_lowercase
ALPHABET_REVERSE = dict((c, i) for (i, c) in enumerate(ALPHABET))
BASE = len(ALPHABET)

# Encodes a positive integer to base 64 coding
def num_encode(n):
    s = []
    while True:
        n, r = divmod(n, BASE)
        s.append(ALPHABET[r])
        if n == 0: break
    return ''.join(reversed(s))

# Decodes a base 64 coding to a positive integer
def num_decode(s):
    n = 0
    for c in s:
        n = n * BASE + ALPHABET_REVERSE[c]
    return n


class Page():

    def __init__(self):
        pass

    def tokenstem(self, data):
        #next set of operations are on a single string
        data = data.encode("ascii", errors="ignore").decode()
        data = re.sub(r'http[^\ ]*\ ', r' ', data) 
        data = re.sub(r'&nbsp;|&lt;|&gt;|&amp;|&quot;|&apos;', r' ', data) 
        data = re.sub(r'\—|\%|\$|\'|\~|\||\.|\*|\[|\]|\:|\;|\,|\{|\}|\(|\)|\=|\+|\-|\_|\#|\!|\`|\"|\?|\/|\>|\<|\&|\\|\u2013|\n', r' ', data)
        # remove alphanumeric strings
        data = re.sub(r'\b(?=[a-z]*[0-9])(?=[0-9]*[a-z])[a-z0-9]+\b', r'', data)
        # remove all leading zeros
        data = re.sub(r'\b0+', r'', data)

        # convert string data into a list
        data = data.split()
        
        # global NumberOfTokensBeforeStemming # make a heap to get unique tokens before stemming
        # NumberOfTokensBeforeStemming = data.length() 

        stemlist = []
        for t1 in data:
            # number_of_unique_unstemmed_words.add(t1)
            if stop_dict[t1] != 1:
                stemlist.append(stemmer.stem(t1))
        return stemlist

    # This function processes the XML Element content after parser enters End of Page
    # 6 fields of interest are separately extracted from the content
    # Extraction logic depends on repeated patterns observed from the content
    # Extraction is hence dependent on how well the patterns have been identified
    # Each field is cleaned, tokenized, stop words are removed and tokens are stemmed                               
    def processText(self, text, title):
        
        # currenttime = timeit.default_timer()
        # print (0, title, currenttime - start)
        text = text.lower() 
        data=text.split('==references==')
        if data[0]==text:
            data=text.split('==references ==')	
        if data[0]==text:
            data=text.split('== references==')
        if data[0]==text:
            data=text.split('== references ==')
        if data[0]==text:
            references = []
            links = []
        else:
            # currenttime = timeit.default_timer()
            # print (1, currenttime - start)
            references = self.extractReferences(data[1])
            links = self.extractExternalLinks(data[1])
        categories = self.extractCategories(text)
        title = self.tokenstem(title.lower())
        body = self.extractBody(data[0])
        # split data for Infobox
        info = self.extractInfobox(data[0])        
    
        return title, body, info, categories, links, references


    def extractBody(self, text):
        data = re.sub(r'\{\{.*\}\}', r' ', text)
        # if (debug == 1):
        #     print("body =", data)        
        data = self.tokenstem(data)
        return data

    def extractInfobox(self, data):
        infob1 = []
        text1=re.split(r'\{\{infobox',data)
        # text1 = data.split('{{infobox')
        for i in range(1, len(text1)):
            text2=text1[i].split('\n')
            flag=0
            j=0
            while flag==0 and j< len(text2):
                # print(j, flag, text2[j], len(text2))
                if text2[j]=='}}':
                    flag=1
                    continue
                infob1.append(text2[j])
                j +=1
        # if (debug == 1):
        #     print("infobox =", infob1)

        data = self.tokenstem(' '.join(infob1))
        return data

    def extractReferences(self, text):
        # remove the text after next occurrence of '==' to remove external links section
        data=text.split('==')
        dt = data[0]
        dt1=dt.split('[[category')
        text1 = dt1[0]
        
        refs=[]
        data1=re.findall(r'(?:\[*.*\]|\{*.*\})',text1)
        for i in data1:
            # if (debug == 1):
            #     print(data1)
            refs.append(i[1:len(i)-1])

        # if (debug == 1):
        #     print("References =", refs)        
        data = self.tokenstem(' '.join(refs))
        return data

    # Use findall to find all instances of categories data
    def extractCategories(self, data):

        data=re.findall(r'\[\[category:.*\]\]',data)
        categories=[]
        for i in data:
            categories.append(i[11:len(i)-2])
        # if (debug == 1):
        #     print("categories =", categories)
        data = self.tokenstem(' '.join(categories))
        return data

    def extractExternalLinks(self, text):
        
        data=text.split('==external links==')
        if len(data)==1:
            data=text.split('==external links ==')	
        if len(data)==1:
            data=text.split('== external links==')
        if len(data)==1:
            data=text.split('== external links ==')
        if len(data)==1:
            return []
        dt1=data[1]
        dt1=re.findall(r'\*\s*.*(?:\[.*\]|\{.*\})',dt1)
        links=[]
        for i in dt1:
            links.append(i[1:len(i)-1])

        # if (debug == 1):
        #     print("External Links =", links)
        return self.tokenstem(' '.join(links))

class PageHandler(xml.sax.ContentHandler):

    def __init__(self):
        self.CurrentData = ''
        self.title = ''
        self.text = ''
        # self.ID = ''
        # self.idFlag = 0
        self.numTextBytes = 0
        self.currenttext = ''
        self.scounts=0
        self.pageCount = 0
        self.fileCount = 0
        self.dictID = {}
        self.offset = 0
        global pageCount
        global fileCount
        global dictID
        global offset

    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if tag == 'text':
            attset = attributes.getNames()
            if "bytes" in attset:
                self.numTextBytes = int(attributes.getValue("bytes"))
        # if debug ==1:
        #     if tag == 'page':
        #         currenttime = timeit.default_timer()
        #         print ("parser", tag, pageCount, currenttime - start)    

    def endElement(self, tag):

        if tag == 'text':
            if self.numTextBytes > 50000:
                self.text += self.currenttext
        elif tag == 'page':
            d = Page()
            title, body, info, categories, links, references = d.processText(self.text, self.title)
            ID = num_encode(self.pageCount)    
            self.dictID[self.pageCount] = ID + ' ' + str(len(body))+ ' ' + str(len(title)) + ' ' + str(len(categories))+ ' ' + str(len(info)) + ' ' + str(len(references)) + ' ' + str(len(links)) + ' ' +self.title.strip().encode("ascii", errors="ignore").decode()

            self.pageCount += 1
            if self.pageCount%NUMPAGES_IN_PREINDEX == 0:   
                print(self.pageCount)       
                prevTitleOffset = self.offset
                data = []
                dataOffset = []
                for key in sorted(self.dictID):
                    temp = self.dictID[key].strip()
                    data.append(temp)
                    dataOffset.append(str(prevTitleOffset))
                    prevTitleOffset += len(temp) + 1

                filename = sys.argv[2] + '/pageStats.txt'
                with open(filename, 'a') as f:
                    f.write('\n'.join(data))
                    f.write('\n')
                
                filename = sys.argv[2] + '/pageStatsOffset.txt'
                with open(filename, 'a') as f:
                    f.write('\n'.join(dataOffset))
                    f.write('\n')
                self.offset = prevTitleOffset
                offset = prevTitleOffset
                self.dictID = {}

            self.CurrentData = ''
            self.title = ''
            self.text = ''

    def characters(self, content):

        if self.CurrentData == 'title':
            self.title += content
        elif self.CurrentData == 'text':
            if self.numTextBytes < 50000:
                self.text += content
            else: # append in multiples of 3000
                self.currenttext += content
                if self.scounts > 3000:
                    self.text += self.currenttext
                    self.currenttext = ''
                    self.scounts = 0
                else:
                    self.scounts += 1
                    # print(self.scounts, content)

class Parser():

    def __init__(self, filename):

        self.parser = xml.sax.make_parser()
        self.parser.setFeature(xml.sax.handler.feature_namespaces, 0)
        self.handler = PageHandler()
        self.parser.setContentHandler(self.handler)
        self.parser.parse(filename)

if __name__ == '__main__':

    FIELDS = ['b','t','c','i','r','l']
    parser = Parser(sys.argv[1])
    
    # print(parser.handler.offset, parser.handler.pageCount)
    prevTitleOffset = parser.handler.offset
    dictID = parser.handler.dictID
    data = []
    dataOffset = []
    for key in sorted(dictID):
        temp = dictID[key].strip()
        data.append(temp)
        dataOffset.append(str(prevTitleOffset))
        prevTitleOffset += len(temp) + 1

    filename = sys.argv[2] + '/pageStats.txt'
    with open(filename, 'a') as f:
        f.write('\n'.join(data))
        f.write('\n')
    
    filename = sys.argv[2] + '/pageStatsOffset.txt'
    with open(filename, 'a') as f:
        f.write('\n'.join(dataOffset))
        f.write('\n')
    offset = prevTitleOffset

    dictID = {}
    fileCount += 1

    currenttime = timeit.default_timer()
    print ("pagecount", parser.handler.pageCount, "number of files", fileCount, "before merge", currenttime - start)
