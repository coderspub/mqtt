#!/home/suriya/virtual_env_python/fleet_1/bin/python3
import json
import logging
import queue
import threading
from datetime import datetime

import paho.mqtt.client as mqtt
from pymongo import MongoClient

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s')
ip = "35.244.17.132"
sub_topic = "fleet/#"
q = queue.Queue(maxsize=0)


def mqtt_thread():
    myclient = MongoClient("mongodb://localhost:27017/")
    while True:
        data_1 = q.get()
        try:
            mydb = myclient[data_1[0]]
            mydb.mqtt.insert_one(data_1[1])
            logging.debug("Inserted successfully:"+str(data_1))
        except Exception as e:
            logging.debug(str(e))


def on_connect(client, userdata, flags, rc):
    client.subscribe(sub_topic)
    logging.debug("Subscribed to topic "+str(sub_topic))


def on_message(client, userdata, msg):
    logging.debug("start time:"+str(datetime.now()))
    try:
        topic = (msg.topic).split("/")
        data = [topic[1], json.loads((msg.payload).decode("utf-8"))]
        q.put(data)
    except Exception as e:
        logging.debug("Error: "+str(e))

    logging.debug("end time:"+str(datetime.now()))


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(ip, 1883, 60)
logging.debug("Connected to broker successfully")
threading.Thread(target=mqtt_thread).start()
client.loop_forever()
