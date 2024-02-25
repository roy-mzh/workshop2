LabS04_04_SparkInvIndex.py is the source code to generate inverted index documents.

For indexing:
  inverted_index_large is the inverted index document for large file set by calling the LabS04_04_SparkInvIndex.py
  inverted_index_small is teh inverted index document for small file set by calling the LabS04_04_SparkInvIndex.py

For ranking:
  all the source codes of ranking are in the search function in views.py in Django zip file.  

For Bonus(Elastic search for indexing and ranking):
  all the source codes of elastic search are in views.py in Django zip file.

For elastic search service install:
  Our group run elastic search service under virtual machine and follows the online instruction. Here is the path: https://www.cnblogs.com/jingping/p/9448099.html

Since search engine is based on 1GB file, tfidf-index_1000M is the file that stored tf-idf score of all the words in large file set. Data can be read by python.
