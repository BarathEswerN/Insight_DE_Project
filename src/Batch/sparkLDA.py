from collections import defaultdict
from pyspark import SparkContext
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.sql import SQLContext
import re
import MySQLdb as mdb

num_of_stop_words = 50      # Number of most common words to remove, trying to eliminate stop words
num_topics = 3              # Number of topics we are looking for
num_words_per_topic = 10    # Number of words to display for each topic
max_iterations = 35         # Max number of times to iterate before finishing

# Initialize
sc = SparkContext('local', 'PySPARK LDA Example')
sql_context = SQLContext(sc)

con = mdb.connect('localhost', 'testuser', 'test623', 'testdb');
cursor = con.cursor()

source_df = sql_context.read.format('jdbc').options(
          url='jdbc:mysql://localhost/testdb',
          driver='com.mysql.jdbc.Driver',
          dbtable='TWEETDATA',
          user='testuser',
          password='test623').load()

source_df.show()
#rdd = sc.parallelize(rows)
file = open("output.txt","w")
file.write("----------CLUSTER ANALYSIS------------\n")

cursor.execute('SELECT DISTINCT BRAND FROM TWEETDATA')
brandsList = cursor.fetchall()
brands = []

for brand in brandsList:
    brands.append(brand[0])

for brand in brands:
    brand = str(brand)
    print(brand)
    cursor.execute("SELECT * FROM TWEETDATA WHERE BRAND = '%s'" %(brand))
    rows = cursor.fetchall()
    if (len(rows) < 62):
        continue

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
    
    tweets = {'POSITIVE' : tweets_positive, 'NEUTRAL' : tweets_neutral, 'NEGATIVE' : tweets_negative}
   
    for tweet in tweets.keys():
        rows = tweets[tweet]
        if (len(rows) < 1):
            continue;
        file = open("sql/testfile.txt","w") 
        for row in rows:
            file.write(str(row)+'\n')

        file.close() 
        # Process the corpus:
        # 1. Load each file as an individual document
        # 2. Strip any leading or trailing whitespace
        # 3. Convert all characters into lowercase where applicable
        # 4. Split each document into words, separated by whitespace, semi-colons, commas, and octothorpes
        # 5. Only keep the words that are all alphabetical characters
        # 6. Only keep words larger than 3 characters

        data = sc.wholeTextFiles('sql/*').map(lambda x: x[1])

        tokens = data                                                   \
            .map( lambda document: document.strip().lower())            \
            .map( lambda document: re.split("[\s;,#]", document))       \
            .map( lambda word: [x for x in word if x.isalpha()])        \
            .map( lambda word: [x for x in word if len(x) > 3] )

        # Get our vocabulary
        # 1. Flat map the tokens -> Put all the words in one giant list instead of a list per document
        # 2. Map each word to a tuple containing the word, and the number 1, signifying a count of 1 for that word
        # 3. Reduce the tuples by key, i.e.: Merge all the tuples together by the word, summing up the counts
        # 4. Reverse the tuple so that the count is first...
        # 5. ...which will allow us to sort by the word count

        termCounts = tokens                             \
            .flatMap(lambda document: document)         \
            .map(lambda word: (word, 1))                \
            .reduceByKey( lambda x,y: x + y)            \
            .map(lambda tuple: (tuple[1], tuple[0]))    \
            .sortByKey(False)

        # Identify a threshold to remove the top words, in an effort to remove stop words
        print("***********************************************")
        print(brand)
        print(termCounts.count())
        print("***********************************************")
        num_words = min(num_of_stop_words, (termCounts.count() / 3))
        threshold_value = termCounts.take(num_words)[num_words - 1][0]

        # Only keep words with a count less than the threshold identified above, 
        # and then index each one and collect them into a map
        vocabulary = termCounts                         \
            .filter(lambda x : x[0] < threshold_value)  \
            .map(lambda x: x[1])                        \
            .zipWithIndex()                             \
            .collectAsMap()

        # Convert the given document into a vector of word counts
        def document_vector(document):
            id = document[1]
            counts = defaultdict(int)
            for token in document[0]:
                if token in vocabulary:
                    token_id = vocabulary[token]
                    counts[token_id] += 1
            counts = sorted(counts.items())
            keys = [x[0] for x in counts]
            values = [x[1] for x in counts]
            return (id, Vectors.sparse(len(vocabulary), keys, values))

        # Process all of the documents into word vectors using the 
        # `document_vector` function defined previously
        documents = tokens.zipWithIndex().map(document_vector).map(list)

        # Get an inverted vocabulary, so we can look up the word by it's index value
        inv_voc = {value: key for (key, value) in vocabulary.items()}

        # Open an output file
        with open("output.txt", 'a') as f:
            lda_model = LDA.train(documents, k=num_topics, maxIterations=max_iterations)
            topic_indices = lda_model.describeTopics(maxTermsPerTopic=num_words_per_topic)
                
            # Print topics, showing the top-weighted 10 terms for each topic

            f.write('*************  '+brand+'  '+tweet+'  ****************')
            f.write("\n")
            for i in range(len(topic_indices)):
                f.write("Topic #{0}\n".format(i + 1))
                for j in range(len(topic_indices[i][0])):
                    f.write("{0}\t{1}\n".format(inv_voc[topic_indices[i][0][j]] \
                        .encode('utf-8'), topic_indices[i][1][j]))
                f.write("\n")
                    

            f.write("{0} topics distributed over {1} documents and {2} unique words\n"  \
                .format(num_topics, documents.count(), len(vocabulary)))
            f.write("-------------------------------------------------------------------")
            f.write("\n")
