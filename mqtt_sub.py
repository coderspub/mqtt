#!/home/suriya/virtual_env_python/fleet_1/bin/python3
import json
import logging
import queue
import threading
from datetime import datetime
import pymysql
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
ip = "35.244.17.132"
sub_topic = "fleet/#"
q = queue.Queue(maxsize=0)

def database(db1='fleet_admin'):
    return pymysql.connect(host='localhost', user='fleet',password='Fleet@123', db=db1, cursorclass=pymysql.cursors.DictCursor)

def mqtt_thread():
    while True:
        data_1 = q.get()
        try:
            logging.debug("T:start time:"+str(datetime.now()))
            dba = database()
            with dba.cursor() as mycursor:
                mycursor.execute("SELECT db FROM reg_user WHERE email_id=%s",[data_1[0]])
                result = mycursor.fetchone()
            dba.close()
            if result!=None:
                lon=data_1[1]['Longitude']
                lat=data_1[1]['Latitude']
                speed=data_1[1]['Speed']
                accuracy=data_1[1]['Accuracy']
                dt=str(datetime.now())
                appid=data_1[1]['AppId']
                dba = database(result['db'])
                with dba.cursor() as mycursor:
                    mycursor.execute("INSERT into app_tracking_details (location,speed,accuracy,datetime,appid) values (ST_GEOMFROMTEXT('POINT(%s %s)'),%s,%s,%s,%s)",(lon,lat,speed,accuracy,dt,appid))
                dba.commit()
                dba.close()
                #mydb.tracking_details.insert_one(data_1[1])
                logging.debug("Inserted successfully")
            logging.debug("T:end time:"+str(datetime.now()))
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

if __name__=="__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(ip, 1883, 60)
    logging.debug("Connected to broker successfully")
    threading.Thread(target=mqtt_thread).start()
    client.loop_forever()
