#!/usr/bin/env python2.7
"""Gets Temperature and Humidity values 
from api.openweathermap.org using the api key"""

__author__ = "Artur Balanuta"
__version__ = "2.0.5"
__email__ = "arturb [at] andrew.cmu.edu"


import signal
import time
import ssl
import logging
import threading
import paho.mqtt.client as mqtt

from urllib2 import urlopen
from json import loads as decode_json
from datetime import datetime
from optparse import OptionParser
from configparser import ConfigParser


# create logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def parse_arguments():
    optp = OptionParser()
    optp.add_option('-f','--config_file', dest='config_file', help='Config file')
    opts, args = optp.parse_args()
    
    if opts.config_file is None:
        optp.print_help()
        exit()

    print("Loading configuration file: "+str(opts.config_file))

    config = ConfigParser()
    config.read(opts.config_file)

    opts = dict()

    for key in config['DEFAULT']:
        opts[key] = config['DEFAULT'][key]


    print opts
    return opts


class OpenWeatherMapAPI():

    QUERY_PAGE = "http://api.openweathermap.org/"
    WEATHER_PATH = "data/2.5/weather?"
    UVI_PATH = "/data/2.5/uvi?"
    CARBON_MONOXIDE_PATH = "pollution/v1/co/"
    SULFUR_DIOXIDE_PATH = "pollution/v1/so2/"

    LAT = "40.44433"
    LON = "-79.94481"
    COORDENATES = "lat="+LAT+"&lon="+LON
    UNITS = "&units=metric"
    APPID = "&appid="

    def __init__(self, appId):
        #self.updateValues()  # Runs one time
        self.APPID = "&appid="+appId
        pass

    def getJSON(self, url):
        json = dict()
        try:
            resp = urlopen(url)
            json = decode_json(resp.read())
        except:
            print "Error getting data from Server"

        return json

    def publishData(self, publisFunc):
        self.updateWeather(publisFunc)
        self.updateUVindex(publisFunc)
        self.updateCarbonMonoxide(publisFunc)
        self.updateSulfurDioxide(publisFunc)

    def updateSulfurDioxide(self, publisFunc):
        url = self.QUERY_PAGE + self.SULFUR_DIOXIDE_PATH + str(int(float(self.LAT))) + "," + str(int(float(self.LON))) + "/current.json?" +self.APPID
        json = self.getJSON(url)

        for (name, items) in json.items():
            if name in ["location", "time"]:
                continue
            elif name == "data" and len(items) > 18 and "value" in items[18]:
                logging.info("sulfur_dioxide_ratio_per_billion_at_1hPa : " + str(float(items[18]["value"]) * 1000000000))
                publisFunc("sulfur_dioxide_ratio_per_billion_at_1hPa", float(items[18]["value"]) * 1000000000)
                continue
            else:
                logging.Error("New Items: " + str(name) + " " + str(items))

    def updateCarbonMonoxide(self, publisFunc):
        url = self.QUERY_PAGE + self.CARBON_MONOXIDE_PATH + str(int(float(self.LAT))) + "," + str(int(float(self.LON))) + "/current.json?" +self.APPID
        json = self.getJSON(url)

        for (name, items) in json.items():
            if name in ["location", "time"]:
                continue
            elif name == "data" and len(items) > 0 and "value" in items[0]:
                logging.info("carbon_monoxide_ratio_per_billion_at_1bar : " + str(float(items[0]["value"]) * 1000000000))
                publisFunc("carbon_monoxide_ratio_per_billion_at_1bar", float(items[0]["value"]) * 1000000000)
                continue
            else:
                logging.Error("New Items: " + str(name) + " " + str(items))


    def updateUVindex(self, publisFunc):
        url = self.QUERY_PAGE + self.UVI_PATH + self.COORDENATES + self.UNITS + self.APPID
        #print url
        json = self.getJSON(url)

        for (name, items) in json.items():
            if name in ["lat", "date", "lon", "date_iso"]:
                continue
            elif name == "value":
                logging.info("ultraviolet_index : " + str(items))
                publisFunc("ultraviolet_index", items)
            else:
                logging.Error("New Items: " + str(name) + " " + str(items))

    def updateWeather(self, publisFunc):

        url = self.QUERY_PAGE + self.WEATHER_PATH + self.COORDENATES + self.UNITS + self.APPID
        json = self.getJSON(url)

        for (name, items) in json.items():
            if name in ["name", "sys", "coord", "weather", "base", "dt", "id", "cod"]:
                continue

            elif name == "clouds" and "all" in items.keys():
                logging.info("cloudiness_percentage : " + str(items["all"]))
                publisFunc("cloudiness_percentage", items["all"])
                continue

            elif name == "visibility":
                logging.info("visibility_meters : " + str(items))
                publisFunc("visibility_meters", items)
                continue

            elif name == "wind":
                if "deg" in items.keys():
                    logging.info("wind_direction_degrees : " + str(items["deg"]))
                    publisFunc("wind_direction_degrees", items["deg"])
                if "speed" in items.keys():
                    logging.info("wind_speed_meters_per_second : " + str(items["speed"]))
                    publisFunc("wind_speed_meters_per_second", items["speed"])
                continue

            elif name == "main":
                if "pressure" in items.keys():
                    logging.info("pressure_hPa : " + str(items["pressure"]))
                    publisFunc("pressure_hPa", items["pressure"])
                if "temp" in items.keys():        
                    logging.info("temperature_C : " + str(items["temp"]))
                    publisFunc("temperature_C", items["temp"])
                if "humidity" in items.keys():
                    logging.info("humidity_percentage : " + str(items["humidity"]))
                    publisFunc("humidity_percentage", items["humidity"])
                continue

            elif name == "rain" and "3h" in items.keys():
                logging.info("precipitation_volume_last_3h_mm : " + str(items["3h"]))
                publisFunc("precipitation_volume_last_3h_mm", items["3h"])
                continue

            elif name == "snow" and "3h" in items.keys():
                logging.info("snow_volume_last_3h_mm : " + str(items["3h"]))
                publisFunc("snow_volume_last_3h_mm", items["3h"])
                continue

            else:
                loggin.Error("New Items: " + str(name) + " " + str(items))


class Runner():

    CONF = parse_arguments()
    API = OpenWeatherMapAPI(CONF['weather_app_id'])
    HOST = CONF['mqtt_host']
    HOST_PORT = int(CONF['mqtt_port'])  
    USER = CONF['mqtt_user']
    TOKEN = CONF['mqtt_token']
    ENDPOINT = 'openchirp/device/'+USER+'/#'
    TRANSDUCER_ENDPOINT = 'openchirp/device/'+USER+'/'

    def __init__(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        self.client = mqtt.Client(client_id=self.USER)
        self.client.on_connect = self.on_connect
        self.client.on_log = self.on_log
        self.client.username_pw_set(self.USER, self.TOKEN)
        self.client.tls_set(tls_version = ssl.PROTOCOL_TLS)

    def run(self):
        print "Starting..."
        while True:
            self.client.connect(self.HOST, self.HOST_PORT, 60)
            self.API.publishData(self.publish)
            self.client.disconnect()
            time.sleep(10 * 60) # once every 10 minutes

    def publish(self, transducer, value):
        self.client.publish(self.TRANSDUCER_ENDPOINT+transducer, value)
	logging.info(self.TRANSDUCER_ENDPOINT+transducer+":"+str(value))

    def signal_handler(self, signal, frame):
        logging.info('Received kill signal ..Stopping service daemon')
        try:
	        self.client.disconnect()
        except:
            pass
        exit(0)

    def on_connect(self, client, userdata, flags, rc):
        logging.info("Connected with result code "+str(rc))
        client.subscribe(self.ENDPOINT)

    def on_log(self, client, userdata, level, buf):
        logging.debug(buf)

if __name__ == "__main__":
    Runner().run()
