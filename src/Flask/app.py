# Hello, Flask!
from flask import Flask, render_template, request
from dateutil.parser import parser
from datetime import datetime
import MySQLdb as mdb

app = Flask(__name__)

# Index page, no args
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

    #dt = parser.parse(from_date)

    # from_date = datetime.datetime.strptime(from_date, '%m/%d/%Y')
    # to_date = datetime.datetime.strptime(to_date, '%m/%d/%Y')
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


    #return str(rows)
 
    #select result from TWEETDATA where DATE between
    #return render_template("analysis.html", brand=brand, from_date=from_date, to_date=to_date)
	# With debug=True, Flask server will auto-reload 
	# when there are code changes
	

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, debug=True)