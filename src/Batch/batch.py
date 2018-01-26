import MySQLdb as mdb

import csv
import re
import nltk
from nltk.corpus import stopwords 

from nltk.stem.wordnet import WordNetLemmatizer
import string
import numpy as np
from pprint import pprint # pretty-printer
import itertools
from collections import OrderedDict
import collections

from gensim import corpora, models, similarities



con = mdb.connect('localhost', 'testuser', 'test623', 'testdb');
cursor = con.cursor()
cursor.execute("SELECT * FROM TWEETDATA")
rows = cursor.fetchall()
tweets_negative = []
tweets_positive = []
tweets_neutral = []

for row in rows:
	if (row[3] == 'neu'):
		tweets_neutral.append(row[2])
	elif (row[3] == 'pos'):
		tweets_positive.append(row[2])
	else:
		tweets_negative.append(row[2])


stoplist = stopwords.words('english') # create English stop words list
# stoplist.append('superbowl') # you may add other words to the list 
stop=set(stoplist)
exclude = set(string.punctuation) # for punctuation removal
lemma = WordNetLemmatizer() # for lemmatization


def clean(doc):
  stop_free = " ".join([i for i in doc.lower().split() if i not in stop])
  punc_free = ''.join(ch for ch in stop_free if ch not in exclude)
  normalized = " ".join(lemma.lemmatize(word) for word in punc_free.split())
  return normalized

texts_neg = [clean(doc).split() for doc in tweets_negative]
texts_pos = [clean(doc).split() for doc in tweets_positive]
texts_neu = [clean(doc).split() for doc in tweets_neutral]


np.random.seed(0)
# Create Dictionary.
id2word = corpora.Dictionary(texts_neg)
# Creates the Bag of Word corpus.
mm = [id2word.doc2bow(text) for text in texts_neg]
# Trains the LDA models.
trained_models = OrderedDict()
# Can vary number of topics, here I use just 5 topics to speed up calculations
for ntopics in range(1,6,1):
  print("Doing LDA(k=%d)" % ntopics)
  lda = models.ldamodel.LdaModel(corpus=mm, id2word=id2word, num_topics=ntopics, update_every=1, chunksize=10000, passes=10)
  trained_models[ntopics] = lda
  #print(trained_models[ntopics].print_topics())
  
# calculate coherence for each case
cm = models.CoherenceModel.for_models(trained_models.values(), dictionary=id2word, texts=texts_neg, coherence='c_v')
coherence_estimates = cm.compare_models(trained_models.values())
coherences = dict(zip(trained_models.keys(), coherence_estimates))
avg_coherence = \
  [(num_topics, avg_coherence)
    for num_topics, (_, avg_coherence) in coherences.items()]
ranked = sorted(avg_coherence, key=lambda tup: tup[1], reverse=True)
# print the best case 
print("Ranked by average '%s' coherence:\n" % cm.coherence)
for item in ranked:
  print("num_topics=%d:\t%.4f" % item)
print("\nBest: %d" % ranked[0][0])
#=== LDA, final run with the chosen above number of topics
# Input: texts_neg and number of topics (ntopics_use)
ntopics_use=ranked[0][0] 
np.random.seed(0)
lda = models.ldamodel.LdaModel(corpus=mm, id2word=id2word, num_topics=ntopics_use, update_every=1, chunksize=10000, passes=10)
# Assigns the topics to the documents in corpus
lda_corpus = lda[mm]
# Input: lda_corpus and tweets_negative
# Printing the results, perhaps not the most optimal way
topic_list=[]
score_list=[]
twtext_list=[]

for m,k in zip(lda_corpus,tweets_negative):
  max_tuple=max(m,key=lambda item:item[1]) # find cluster based on max score
  #print(max_tuple[0],max_tuple[1],k)
  #print(m,k)
  topic_list.append(max_tuple[0])
  score_list.append(max_tuple[1])
  twtext_list.append(k)
score_twtext=tuple(zip(score_list,twtext_list))

#print(score_twtext)
#Select top tweet in each cluster
top_list=[]
for ntop in range(0,ntopics_use):
  indices = [i for i, x in enumerate(topic_list) if x == ntop]
  list_sel=[score_twtext[i] for i in indices]
  print(list_sel)
  top_list.append(max(list_sel,key=lambda item:item[0])[1])
#print(top_list)
#Number of messages in each cluster
num_cluster=[]
counter=collections.Counter(topic_list)
for i in range(0,ntopics_use):
  num_cluster.append(counter[i])
#print(num_cluster)
result_tuple=tuple(zip(num_cluster,top_list)) # return this tuple
for i in result_tuple:
  print('Number of tweets in cluster:', i[0])
  print('Top-ranked tweet:', i[1])
  print(' ')
