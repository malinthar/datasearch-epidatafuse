from flask import Flask, request, jsonify
from datetime import datetime
import weatherScrapper
from multiprocessing import Value
import csv

app = Flask(__name__)

file = "../data/denguecases.csv"
with open(file,'rt')as f:
    reader = csv.reader(f)
    dengue_data = []
    for row in reader:
        dengue_data.append(row)

file = "../data/percipitation.csv"
with open(file,'rt')as f:
    reader = csv.reader(f)
    percipitation_data = []
    for row in reader:
        percipitation_data.append(row)

percipitation_counter = Value('i', 0)
dengue_counter = Value('i', 0)

@app.route("/")
def hello_world():
   return ("Hello World")

@app.route("/weatherdata")
def hello_test():
   print("Executed!")
   res = {
       "stationID" :"28u3rfknfcv",
       "stationName":"Cinnamon Lake Side Hotel",
       "observedValue":"0.6",
       "dtg":"20130526",
       "spatialGranule":"Cinnamon Lake Side Hotel",
       "temporalGranule":"20130526"
   }
   return jsonify(res)

@app.route("/weatherstations")
def hello_weather():
   print("Executed!")
   res = {
       "StationName":"Cinnamon Lake Side Hotel",
       "Latitude":"6.929543",
       "Longitude":"79.8492668"
   }
   return jsonify(res)

@app.route("/percipitation")
def hello_percipitation():
    with percipitation_counter.get_lock():
        percipitation_counter.value += 1
        index = percipitation_counter.value
        index = (index%len(percipitation_data))
    response_data = percipitation_data[index]
    res = {
        "stationID":response_data[0],
        "stationName":response_data[1],
        "observedValue":response_data[2],
        "dtg":response_data[3],
        "spatialGranule":response_data[4],
        "temporalGranule":response_data[5]
    }
    return jsonify(res)

@app.route("/denguecases")
def hello_denguecases():
    with dengue_counter.get_lock():
        dengue_counter.value += 1
        index = dengue_counter.value
        index = (index%len(dengue_data))
    response_data = dengue_data[index]
    res = {
        "mohID":response_data[0],
        "mohName":response_data[1],
        "dtg":response_data[2],
        "week":response_data[3],
        "casesCount":response_data[4],
        "spatialGranule":response_data[5],
        "temporalGranule":response_data[6]
    }
    return jsonify(res)

@app.route("/weather")
def weather():
   dateString = request.args.get('date')
   date = datetime.strptime(dateString.strip(), '%Y/%m/%d').date()
   print(date.month, date.year)
   data = weatherScrapper.getPercipitation(date.day, date.month, date.year)
   return (str(data))

@app.route("/temp")
def temperature():
    res = {
           "sensorId":"01CB068A",
           "temperature":50.5
    }
    return jsonify(res)

@app.route("/sweet")
def sweet():
    res = {
        "name": "TestName",
        "temperature": 56.5
    }
    return jsonify(res)

if __name__ == '__main__':
   app.run()
