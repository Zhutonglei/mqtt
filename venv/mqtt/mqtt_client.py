# -*- coding: utf-8 -*-

import paho.mqtt.client  as mqtt
import time
import json
import base64
import xlrd
import thread
import time
import re
import xlwt
import datetime
import pymysql as mysqldb



HOST = "47.99.40.19"
PORT = 18470


def client_loop():
    client = mqtt.Client("MQTT_FX_Client",protocol=mqtt.MQTTv311,transport="tcp")  # ClientId不能重复，所以使用当前时间
    client.username_pw_set("02004300", "sJnnGpzY")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(HOST, PORT, 60)
    client.loop_forever()



def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("application/29/device/ff00020043000001/rx")
    client.subscribe("application/29/device/ff00020043000002/rx")
    client.subscribe("application/29/device/ff00020043000003/rx")
    client.subscribe("application/29/device/ff00020043000004/rx")
    client.subscribe("application/29/device/ff00020043000005/rx")
    client.subscribe("application/29/device/ff00020043000006/rx")
    client.subscribe("application/29/device/ff00020043000007/rx")
    client.subscribe("application/29/device/ff00020043000008/rx")
    client.subscribe("application/29/device/ff00020043000009/rx")
    client.subscribe("application/29/device/ff0002004300000a/rx")
def on_message(client, userdata, msg):
    # 写入数据库
    config = {
        'host': '192.168.31.102',
        'port': 3306,
        'user': 'root',
        'passwd': '1qaz@WSX',
        'db': 'MQTT',
        'charset': 'utf8'
    }
    conn = mysqldb.connect(**config)
    cursor = conn.cursor()
    conn.autocommit(1)
    #打印消息
    message = msg.payload.decode("utf-8")
    print(msg.topic + " " + message)
    params = json.loads(str(message))
    data = params["data"]
    devEUI = params["devEUI"]
    params_2=params["rxInfo"]
    list_rxInfo = list(params_2)
    rx_Info = dict(list_rxInfo[0])
    rssi = str(rx_Info["rssi"])
    loRaSNR =  str(rx_Info["loRaSNR"])
    txInfo = params["txInfo"]
    frequency = str(txInfo["frequency"])

    #frequency = params["frequency"]

    print devEUI

    #转为16进制
    a = str(base64.b64decode(data)).encode("hex")
    print a
    list_a = cut_text(str(a), 8)
    # 获取计数

    count_a = list_a[0] # 获取data的数组
    count_b = cut_text(count_a, 2)
    count = count_b[3] + count_b[2] + count_b[1] + count_b[0]
    number = int(count, 16)
    #h获取version
    ver_a = list_a[1]
    ver_b = cut_text(ver_a, 2)
    version  = ver_b[3] + ver_b[2] + ver_b[1] + ver_b[0]
    version_serial = int(version, 16)
    
    #当前时间
    cur_time = time.time()
    n= time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))
    #n =datetime.datetime.fromtimestamp(cur_time)

    total_micrsec =int((cur_time)*1000)
    #print total_micrsec
    #获取发送时间
    sendtime1 =list_a[2]
    sendtime2 =list_a[3]
    count_d =cut_text(sendtime2,2)
    count_c = cut_text(sendtime1, 2)
    timesend =count_c[3]+count_c[2]+count_c[1]+count_c[0]
    timesend1 =count_d[1]+count_d[0]
    send_time = int (timesend,16)
    send_time1 =int(timesend1,16)
    if send_time1 <= 99 and send_time1 > 9:
        send_time1 = "0" + str(send_time1)
    if send_time1 <= 9:
        send_time1 = "00" + str(send_time1)
    sendtime_now =int(str(send_time)+str(send_time1))
    t = datetime.datetime.fromtimestamp(send_time)
    delay = total_micrsec-sendtime_now
    print total_micrsec
    print sendtime_now
    print  delay

    excute_sql ="INSERT INTO receive_data VALUES('%(dev)s','%(count)s','%(version)s','%(starttime)s','%(nowtime)s','%(interval)s','%(rssi)s','%(loRaSNR)s','%(frequency)s')"%{"dev":devEUI,"count":number,"version":version_serial,"starttime":t,"nowtime": n,"interval":delay,"rssi":rssi,"loRaSNR":loRaSNR,"frequency":frequency}
    print excute_sql
    cursor.execute(excute_sql)

def cut_text(text,lenth):
    textArr = re.findall('.{'+str(lenth)+'}', text)
    textArr.append(text[(len(textArr)*lenth):])
    return textArr



if __name__ == '__main__':

    client_loop()

