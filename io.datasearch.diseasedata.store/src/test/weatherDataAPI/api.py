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
   obj = {
   "dataPoints" :[
   {
       "StationId":"01CB068A"
       "StationName":"Cinnamon Lake Side Hotel",
       "Latitude":"6.929543",
       "Longitude":"79.8492668",
       "dtg":"20130526",
       "ObservedValue":"0.6"
   },
   {
      "StationId":"01GM0528"
      "StationName":"Walpita",
      "Latitude":"7.27",
      "Longitude":"80",
      "dtg":"20130526",
      "ObservedValue":"4.2"
   },]
   }

# ,,,,,0.6
# 01GM0528,Walpita,7.27,80,20131206,0
# 01KG092B,Dodangaslanda,7.57,80.5,20130130,0
# 01JF0425,Point Pedro,9.83,80.2,20130106,0
# 01PU313A,Marawila,7.4234916,79.8353622,20130508,4.2
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
