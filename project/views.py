from django.shortcuts import render
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from threading import Thread
from elasticsearch import Elasticsearch
import os, re
import math, random
import numpy
import codecs

sc = SparkContext.getOrCreate(SparkConf())
flag = [0, 0]

def initialize_fast(flag):
    if not flag[1]:
        global wy
        wy = ElasticWy("8", "wy", ip="localhost")
        wy.create_index(index_name="9",index_type="wy")
        readfile("/root/group/1G/")
        import result as rs
        flag[1] = 1
        

'''遍历TXT文件并且调用es插入数据'''
def readfile(pth):
    global wy
    filename=[]   #存储文件种类名
    times = 0
    for root, dirs, files in os.walk(pth):
        for name in files:
            filename.append(name)
    
    for name in filename:
        every_file_path = pth+name
        print(name)
        with codecs.open(every_file_path,encoding='utf-8') as f:
            temp = f.read()
            wy.Index_Data('/',name,temp,times)
            times+=1
    print("file finished")


class ElasticWy:
    def __init__(self, index_name,index_type,ip ="localhost"):
 
        self.index_name =index_name
        self.index_type = index_type
        self.es = Elasticsearch([ip],
                                http_auth=('elastic',
                                           'password'),
                                port=9200)
 
    def create_index(self,index_name,index_type):
        '''
        创建索引,创建索引名称为ott，类型为ott_type的索引
        :param ex: Elasticsearch对象
        :return:
        '''
        #创建映射
        _index_mappings = {
            "settings":{
                "number_of_replicas":0
            },
            "mappings": {
                self.index_type: {
                    "properties": {
                        "source": {
                            "type": "text",
                            "index": True,
#                             "analyzer": "ik_max_word",
#                             "search_analyzer": "ik_max_word"
                            "analyzer": "whitespace",
                            "search_analyzer": "whitespace"
                        }
                    }
                }
 
            }
        }
        if self.es.indices.exists\
                    (index=self.index_name) is not True:
            res = self.es.indices.create\
                (index=self.index_name, body=_index_mappings)
            print('1',res)
     
    def Index_Data(self,category,docname,content,cishu):
 
        if isinstance(content,str):   #判断是否为表类型
            #print('插入数据')
           # for category,name,line in namelist,docname,filelist:
 
            action = {
                    #'category': category,
                    'name': docname,
                    "title": content}
            #ACTIONS.append(action)
            self.es.index(index=self.index_name,
                          doc_type=self.index_type,
                          body=action)
            print('already insert',cishu,'of record')
        else:
            print("error: cannot read the files from dataset!")
 
    def Search_data(self,input_text):
        # doc = {'query': {'match_all': {}}}
        #start_time = time()
        doc = {
            "track_total_hits":True,
            "query": {
                "match":{
                    "title": {
                            "minimum_should_match":1,
                            "query": input_text
                            #"operator": "and"
 
                    }
                }
            },
            "min_score":1e-5,
            "explain":True
        }
        _searched = self.es.search(
            index=self.index_name,
            doc_type=self.index_type,
            body=doc)
        i=0
        last_category = []
        last_docxname = []
        last_sentence = []
        score_ =[]
        for hit in _searched['hits']['hits']:
            #print (hit['_source'])
            #print ( hit['_source']['title'])
            # print(hit['_score'])
           # last_category.append(hit['_source']['category'])
            score_.append(hit['_score'])
            last_docxname.append(hit['_source']['name'])
            last_sentence.append(hit['_source']['title'])
            i=i+1
#             if i==1:
#                 break
        #print(len(last_sentence))
        #for temp in last_sentence:
            #print(temp)
      #  print(last_category)
        print(last_docxname)
        print(score_)
        return last_docxname, score_
        #cost_time = time()-start_time
        #print(cost_time)

def search_fast(request):
    topN = 20
    if request.method == "GET":
        global flag
        Thread(target=(initialize_fast), args=(flag,)).start()
        return render(request, "default/search_fast.html")
        
    else:
        global wy
        query = request.POST.get("top-search")

        scores = wy.Search_data(input_text=query)
        print(scores)
        result_dic = {"pro_data":[(i+1, rename(scores[0][i]), scores[1][i]) for i in range(len(scores[0]))]}

        # if we search nothing, we show all projects we have
        if query == "":
            return render(request, "default/search_fast.html")

        return render(request, "default/search_fast.html", result_dic)

def tokenize(s):
    if not s:
        return []
    return re.split("\\W+", s.lower())

def read_data(flag):
    if not flag[0]:
        sql = SQLContext(sc)

        #The RDD is mapped as map(lambda x: (x['term'],(x['doc'],x['tfidf']))), for example:
        #tfidf_RDD = tfidf.map(lambda x: (x[1][0],(x[0],x[1][1])))

        #Now we load the TF-IDF index from the disk, can convert it to an RDD, and map as x['term'],(x['doc'],x['tfidf'])
        global tfidf_RDD
        tfidf_RDD = sql.read.parquet("tfidf-index_1000M").rdd.map(lambda x: (x['_2'],(x['_1'],x['_3'])))
        import result as rs
        flag[0] = 1

def rename(old):
    if len(old) > 0:
        old = int(old.split(".")[0])
        if old > 2600:
            old = random.randint(0, 2580)
        try:
            return rs.result_dic[str(old)]
        except:
            import result as rs
            return rs.result_dic[str(old)]
    
        
# Create your views here.
def search(request):
    """
        Func:
            The function for "search" page. To implement the searching function.
    """
    topN = 20
    global tfidf_RDD
    if request.method == "GET":
        global flag
        Thread(target=(read_data), args=(flag,)).start()
        return render(request, "default/search.html")
        
    else:
        query = request.POST.get("top-search")
        
        tokens = sc.parallelize(tokenize(query)).map(lambda x: (x, 1) ).collectAsMap()
        bcTokens = sc.broadcast(tokens)

        #connect to documents with terms in the Query. to Limit the computation space
        #so that we don't attempt to compute similarity for docs that have no words in common with our query.  
        try:
            joined_tfidf = tfidf_RDD.map(lambda x: (x[0], bcTokens.value.get(x[0], '-'), x[1]) ).filter(lambda x: x[1] != '-' )
        except:
            read_data(flag=[0])
            joined_tfidf = tfidf_RDD.map(lambda x: (x[0], bcTokens.value.get(x[0], '-'), x[1]) ).filter(lambda x: x[1] != '-' )

        #compute the score using aggregateByKey
        scount = joined_tfidf.map(lambda a: a[2]).aggregateByKey((0,0),
        (lambda acc, value: (acc[0] + value, acc[1] + 1)),
        (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])) )

        scores = scount.map(lambda x: ( x[1][0]*x[1][1]/len(tokens), x[0]) ).top(topN)
        print(scores)
        result_dic = {"pro_data":[(i+1, rename(scores[i][1]), scores[i][0]) for i in range(len(scores))]}

        # if we search nothing, we show all projects we have
        if query == "":
            return render(request, "default/search.html")

        return render(request, "default/search.html", result_dic)
