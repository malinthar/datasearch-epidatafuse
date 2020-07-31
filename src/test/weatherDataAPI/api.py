from flask import Flask, request
from datetime import datetime
import weatherScrapper 


app = Flask(__name__)

@app.route("/")
def hello_world():
   return ("Hello World")

@app.route("/weather")
def weather():
   dateString = request.args.get('date')
   date = datetime.strptime(dateString.strip(), '%Y/%m/%d').date()
   data = weatherScrapper.getPercipitation(date.day, 7, 2020)
   return (str(data))

if __name__ == '__main__':
   app.run()
