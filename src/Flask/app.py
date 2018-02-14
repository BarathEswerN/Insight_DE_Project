from flask import Flask, render_template, request
from dateutil.parser import parser
from datetime import datetime
import MySQLdb as mdb

app = Flask(__name__)

def remove_paren(rows):
    newList = []
    for row in rows:
        row = str(row)
        row = row[1 : -1]
        newList.append(row)
    return newList

def get_clusters(table_name, brand, cntList):
    
    query = "SELECT topic, COUNT(*), COUNT(DISTINCT USER_ID), SUM(RETWEET_COUNT), SUM(FAV_COUNT), AVG(SENTIMENT_SCORE) FROM %s where BRAND = '%s' GROUP BY topic" %(table_name, brand)
    cursor.execute(query)
    rows = cursor.fetchall()
    result = []

    for row in rows:
        num_row = int(float(row[0]))
        q = "SELECT TWEET_ID from %s WHERE TOPIC=%s AND BRAND='%s' LIMIT 3" %(table_name, num_row, brand)
        cursor.execute(q)
        tweet_is_list = cursor.fetchall()

        tweet_ids = []
        for tweetid in tweet_is_list:
          for i in range(0, len(tweetid)):
            tweet_ids.append(tweetid[i])


        topic_query = "SELECT TOPIC_WORDS FROM CLUSTER_TOPICS WHERE BRAND='%s' AND TOPIC=%s"%(brand, num_row)
        cursor.execute(topic_query)
        topic_row = cursor.fetchall()[0][0]
        topic_row = topic_row[:-1]
        topics = topic_row.split('|')
        cntList[0] += 1
        data = {'id': cntList[0],
            'num_tweets': int(row[1]),
            'num_users': int(row[2]),
            'num_retweet': int(row[3]),
            'favorite_count': int(row[4]),
            'senti_score': float(row[5]),
            'tweet_ids':tweet_ids,
            'topics': topics
           }

        result.append(data)

    return result

@app.route('/events:<string:brand>', methods=['GET'])
def get_brand_info(brand):
    brand = brand.encode('utf8')
    cntList = [0]
    cluster_neg = get_clusters('NEG_TWEETS', brand, cntList)
    cluster_pos = get_clusters('POS_TWEETS', brand, cntList)
    cluster_neu = get_clusters('NEU_TWEETS', brand, cntList)

    clusters = []
    for cl in cluster_neg:
        clusters.append(cl)
    for cl in cluster_neu:
        clusters.append(cl)
    for cl in cluster_pos:
        clusters.append(cl)
    
    query = "SELECT result, count(result), AVG(SENTIMENT_SCORE) FROM TWEETDATA where BRAND = '%s' group by result order by result" %(brand)
    cursor.execute(query)
    rows = cursor.fetchall()
    pos_cnt = 0
    pos_avg = 0
    neg_cnt = 0
    neg_avg = 0
    neu_cnt = 0
    neu_avg = 0

    for i in range(0, len(rows)):
        if rows[i][0] == 'pos':
            pos_cnt = int(float(rows[i][1]))
            pos_avg = float(rows[i][2])

        elif rows[i][0] == 'neg':
            neg_cnt = int(float(rows[i][1]))
            neg_avg = float(rows[i][2])
        else:
            neu_cnt = int(float(rows[i][1]))
            neu_avg = float(rows[i][2])

    brand_query = "SELECT count(TWEET), SUM(RETWEET_COUNT), SUM(FAV_COUNT), AVG(SENTIMENT_SCORE) FROM TWEETDATA WHERE BRAND='%s'"%(brand)
    cursor.execute(brand_query)
    rows = cursor.fetchall()    
    data = { 'brand': brand,
             'total_num_tweets': int(rows[0][0]),
             'total_retweet_cnt': 0,
             'total_fav_cnt': int(rows[0][2]),
             'toral_avg_sentiment_score': float(rows[0][3]),
             'pos_tweet_count': pos_cnt,
             'pos_sentiment_avg': pos_avg,
             'neutral_tweets_cnt': neu_cnt,
             'neu_sentiment_avg': neu_avg,
             'neg_tweets_cnt': neg_cnt,
             'neg_sentiment_avg': neg_avg,
             'clusters': clusters
           }

    return jsonify(data)

@app.route('/')
def index():
	return render_template("index.html")

@app.route('/', methods=['POST'])
def my_form_post():
    brand = request.form['brand_txt']
    from_date = request.form['from_date'].encode('utf-8')[0 : -3]
    to_date = request.form['to_date'].encode('utf-8')[0 : -3]

    from_year = from_date[6:10]
    from_mnth = from_date[0:2]
    from_day = from_date[3:5]
    from_date = from_year+'-'+from_mnth+'-'+from_day

    to_year = to_date[6:10]
    to_mnth = to_date[0:2]
    to_day = to_date[3:5]
    to_date = to_year+'-'+to_mnth+'-'+to_day

    con = mdb.connect('YOUR_IP_ADDRESS', 'USER_NAME', 'YOUR_PASSWORD', 'YOUR_DATABASE')
    cursor = con.cursor()
    query = "SELECT result, count(result) FROM TWEETDATA where BRAND = '%s' and CREATED_DATE between date('%s') and date('%s') group by result order by result" %(brand, from_date, to_date)
 
    cursor.execute(query)
    rows = cursor.fetchall()

    query1 = "SELECT TWEET FROM TWEETDATA where BRAND = '%s' and result = 'pos' order by id desc limit 3" %(brand)
    query2 = "SELECT TWEET FROM TWEETDATA where BRAND = '%s' and result = 'neg' order by id desc limit 3" %(brand)
    query3 = "SELECT TWEET FROM TWEETDATA where BRAND = '%s' and result = 'neu' order by id desc limit 3" %(brand)

    cursor.execute(query1)
    rows_pos = cursor.fetchall()

    cursor.execute(query2)
    rows_neg = cursor.fetchall()

    cursor.execute(query3)
    rows_neu = cursor.fetchall()

    return render_template("analysis.html", pos=rows[2][1], neg=rows[0][1], neu=rows[1][1], brand=brand, rows_neu=rows_neu, rows_neg=rows_neg, rows_pos=rows_pos)
	

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, debug=True)