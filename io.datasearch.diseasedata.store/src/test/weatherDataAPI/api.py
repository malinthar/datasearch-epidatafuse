from flask import Flask, request, jsonify
from datetime import datetime
import weatherScrapper 


app = Flask(__name__)

@app.route("/")
def hello_world():
   return ("Hello World")

@app.route("/test")
def hello_test():
   print("Executed!")
   obj = {"dataPoints" :[{"add":"Hello_malintha"},{"add":"Hello_chanuka"},{"add":"Hello_dimuthu"}] }
   addlist = []
   addlist.append(obj)
   return jsonify(obj)

@app.route("/weather")
def weather():
   dateString = request.args.get('date')
   date = datetime.strptime(dateString.strip(), '%Y/%m/%d').date()
   print(date.month, date.year)
   data = weatherScrapper.getPercipitation(date.day, date.month, date.year)
   return (str(data))

if __name__ == '__main__':
   app.run()
