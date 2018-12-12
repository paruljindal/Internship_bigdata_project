import os
os.environ['ORACLE_HOME'] = '/oracle/12c/product/12.1.0/client'
os.environ['ORACLE_BASE'] = '/oracle/12c'
os.environ['LD_LIBRARY_PATH'] = '/oracle/12c/product/12.1.0/client/lib'
os.environ['PATH'] = '/oracle/12c/product/12.1.0/client/bin:/home/oracle/scripts:{0}'.format(os.environ.get('PATH'))
print os.environ
import cx_Oracle
import datetime
from influxdb import InfluxDBClient

# app configuration
app_home = "/data/spark_reporting/recharge_events_graphs"
data_dir = app_home+"/data"
config_dir = app_home+"/config"

# oracle configuration
os.environ['ORACLE_HOME'] = "/oracle/12c/product/12.1.0/client"
conn_string_sw = u'visual_clm/oracle789@10.5.232.88:1521/clmdbsw'
conn_string_ne = u'visual_clm/oracle789@10.5.232.90:1521/clmdbne'
conn_sw = cx_Oracle.connect(conn_string_sw)
conn_ne = cx_Oracle.connect(conn_string_ne)
cursor_sw = conn_sw.cursor()
cursor_ne = conn_ne.cursor()
# influx db configuration
influx_client = InfluxDBClient('10.5.252.195', 8086, 'root', 'Mycl0ud@2016', 'telegraf')

# airtel circles
n_circles = ['HP', 'DL', 'PB', 'HR', 'UW', 'UE', 'JK']
s_circles = ['CH','KE', 'KK', 'TN', 'AP']
e_circles=['WB','BH','OR','AS','NE','KO']
w_circles=['MH','MU','MP','GJ','RJ']
all_circles = n_circles + s_circles +e_circles+ w_circles
ne_circles=n_circles+e_circles
sw_circles=s_circles+w_circles

print("process starting at %s"%datetime.datetime.now())
#extracting date upto which data needs to be loaded(current_time-1 hour)
today_date = datetime.datetime.now()
today_date -= datetime.timedelta(hours=1)
today_date_ymd= today_date.strftime("%Y-%m-%d")
current_hour = today_date.strftime("%H")
print current_hour
#Providing the start date as 7 days ahead of the required date to load data(eg. 17 days will load data of past 10 days).
start_date=today_date-datetime.timedelta(days=17)
start_date_ymd=start_date.strftime("%Y-%m-%d")
print(start_date)
print(today_date)
start_hour=start_date.strftime("%H")
event_date=start_date
hubn={}
hubs={}
hube={}
hubw={}

recharge_events_data=[]
recharge_events_ne=[]
recharge_events_sw=[]
#extracting the entire data.
for each_circle in all_circles:
        event_date=start_date
        while(event_date<=today_date):
                event_date_ymd=event_date.strftime("%Y-%m-%d")
                #extracting data on the given date from each circle respectively.
                sql="""
                        SELECT
                                '{0}' AS CIRCLE,
                                COUNT(*) AS COUNT,
                                 '{1}  '||TO_CHAR(TRANSACTION_DATE, 'HH24')||':00:00' AS COUNT_TIME,
                                TO_CHAR(TRANSACTION_DATE, 'HH24') AS COUNT_HOUR
                        FROM
                                DARTSOFR_{0}.RECHARGE_EVENTS
                        PARTITION FOR (to_date('{1}','YYYY-MM-DD'))
                        GROUP BY TO_CHAR(TRANSACTION_DATE, 'HH24')
                """.format(each_circle,event_date_ymd)
                if each_circle in ne_circles:
                        cursor_ne.execute(sql)
                        recharge_events_data1 = cursor_ne.fetchall()
                elif each_circle in sw_circles:
                        cursor_sw.execute(sql)
                        recharge_events_data1= cursor_sw.fetchall()
                else:
                        print("Unknown circle: {0} received, Skipping it.".format(each_circle))
                        continue

                event_date+=datetime.timedelta(days=1)
                recharge_events_data.append(recharge_events_data1)

print("\n\n\n\n")
#extracting hubwise data from the entire data.
#format of each hub data:list of list of date containing the data of that particular date.
for circle in recharge_events_data:
        print(circle)
        if(circle[0][0] in n_circles):
          for k in circle:
                datet=datetime.datetime.strptime(k[2],'%Y-%m-%d %H:%M:%S')
                if datet in hubn:
                        hubn[datet][0]+=k[1]
                else:
                        hubn[datet]=[]
                        hubn[datet].append(k[1])
                        hubn[datet].append(0)
                        hubn[datet].append(0)
                        hubn[datet].append(0)

        elif(circle[0][0] in e_circles):
                for k in circle:
                        datet=datetime.datetime.strptime(k[2],'%Y-%m-%d %H:%M:%S')
                        if datet in hube:
                                hube[datet][0]+=k[1]
                        else:
                                hube[datet]=[]
                                hube[datet].append(k[1])
                                hube[datet].append(0)
                                hube[datet].append(0)
                                hube[datet].append(0)

        elif(circle[0][0] in w_circles):
                for k in circle:
                        datet=datetime.datetime.strptime(k[2],'%Y-%m-%d %H:%M:%S')
                        if datet in hubw:
                                hubw[datet][0]+=k[1]
                        else:
                                hubw[datet]=[]
                                hubw[datet].append(k[1])
                                hubw[datet].append(0)
                                hubw[datet].append(0)
                                hubw[datet].append(0)
        elif(circle[0][0] in s_circles):
                for k in circle:
                        datet=datetime.datetime.strptime(k[2],'%Y-%m-%d %H:%M:%S')
                        if datet in hubs:
                                hubs[datet][0]+=k[1]
                        else:
                                hubs[datet]=[]
                                hubs[datet].append(k[1])
                                hubs[datet].append(0)
                                hubs[datet].append(0)
                                hubs[datet].append(0)
        else:
                continue
#caluclating average of recharge_count as average of data of past 7 days for that date of that particular hub  for each day whose previous data is available for each hub.
print("NORTH")
for x in hubn:
        avgsum=0
        t=x-datetime.timedelta(days=7)
        if t in hubn:
                i=0
                d=x
                while(i<7):
                        d=d-datetime.timedelta(days=1)
                        avgsum+=hubn[d][0]
                        i+=1
                avgsum=int(avgsum/7)
                hubn[x][1]=avgsum
                print(x,hubn[x])

print("EAST")

for x in hube:
        avgsum=0
        t=x-datetime.timedelta(days=7)
        if t in hube:
                i=0
                d=x
                while(i<7):
                        d=d-datetime.timedelta(days=1)
                        avgsum+=hube[d][0]
                        i+=1
                        avg=avgsum
                avgsum=int(avgsum/7)
                hube[x][1]=avgsum
                print(avg,x,hube[x])

print("WEST")
for x in hubw:
        avgsum=0
        t=x-datetime.timedelta(days=7)
        if t in hubw:
                i=0
                d=x
                while(i<7):
                        d=d-datetime.timedelta(days=1)
                        avgsum+=hubw[d][0]
                        i+=1
                avgsum=int(avgsum/7)
                hubw[x][1]=avgsum

print("SOUTH")
for x in hubs:
        avgsum=0
        t=x-datetime.timedelta(days=7)
        if t in hubs:
                i=0
                d=x
                while(i<7):
                        d=d-datetime.timedelta(days=1)
                        avgsum+=hubs[d][0]
                        i+=1
                avgsum=int(avgsum/7)
                hubs[x][1]=avgsum



######ANOMALY######
#calculating anomaly for each date of each hub as the differene between current recharge and average recharge.
for x in hubw:
        anomaly=0
        if(hubw[x][1]!=0):
                anomaly=hubw[x][0]-hubw[x][1]
                hubw[x][2]=anomaly
                anomalyper=((anomaly*1.0)/hubw[x][1])*100
                hubw[x][3]=anomalyper

for x in hube:
        anomaly=0
        if(hube[x][1]!=0):
                anomaly=hube[x][0]-hube[x][1]
                hube[x][2]=anomaly
                anomalyper=((anomaly*1.0)/hube[x][1])*100
                hube[x][3]=anomalyper
for x in hubs:
        anomaly=0
        if(hubs[x][1]!=0):
                anomaly=hubs[x][0]-hubs[x][1]
                hubs[x][2]=anomaly
                anomalyper=((anomaly*1.0)/hubs[x][1])*100
                hubs[x][3]=anomalyper

for x in hubn:
        anomaly=0
        if(hubn[x][1]!=0):
                anomaly=hubn[x][0]-hubn[x][1]
                hubn[x][2]=anomaly
                anomalyper=((anomaly*1.0)/hubs[x][1])*100
                hubn[x][3]=anomalyper

#Loading data hubwise in influx db.
for x in hubn:
        hour=x.strftime("%H")
        time=x-datetime.timedelta(hours=5,minutes=30)
        if hubn[x][1]!=0:
                influx_metric = [{
                        'measurement': 'hubs_recharge_events_graphs_final',
                        "tags": {
                                "Hub":"North",
                                "Hour":hour,
                        },
                        'time':time ,
                        'fields': {
                                'current_recharge_count':hubn[x][0],
                                'average_recharge_count':hubn[x][1],
                                'anomaly_hubwise':hubn[x][2],
                                'anomaly_percentage_hubwise':int(round(hubn[x][3],0))
                        }
                }]
                influx_client.write_points(influx_metric)

for x in hubs:
        hour=x.strftime("%H")
        time=x-datetime.timedelta(hours=5,minutes=30)
        if hubs[x][1]!=0:
                influx_metric = [{
                        'measurement': 'hubs_recharge_events_graphs_final',
                        "tags": {
                                "Hub":"South",
                                "Hour":hour,
                        },
                        'time':time ,
                        'fields': {
                                'current_recharge_count':hubs[x][0],
                                'average_recharge_count':hubs[x][1],
                                 'anomaly_hubwise':hubs[x][2],
                                'anomaly_percentage_hubwise':int(round(hubs[x][3],0))
                        }
                }]
                influx_client.write_points(influx_metric)

for x in hube:
        hour=x.strftime("%H")
        time=x-datetime.timedelta(hours=5,minutes=30)
        if hube[x][1]!=0:
                influx_metric = [{
                        'measurement': 'hubs_recharge_events_graphs_final',
                        "tags": {
                                "Hub":"East",
                                "Hour":hour,
                        },
                        'time':time ,
                        'fields': {
                                'current_recharge_count':hube[x][0],
                                'average_recharge_count':hube[x][1],
                                'anomaly_hubwise':hube[x][2],
                                'anomaly_percentage_hubwise':int(round(hube[x][3],0))
                        }
                }]
                influx_client.write_points(influx_metric)

for x in hubw:
        hour=x.strftime("%H")
        time=x-datetime.timedelta(hours=5,minutes=30)
        if hubw[x][1]!=0:
                influx_metric = [{
                        'measurement': 'hubs_recharge_events_graphs_final',
                        "tags": {
                                "Hub":"West",
                                "Hour":hour,
                        },
                        'time':time ,
                        'fields': {
                                'current_recharge_count':hubw[x][0],
                                'average_recharge_count':hubw[x][1],
                                'anomaly_hubwise':hubw[x][2],
                                'anomaly_percentage_hubwise':int(round(hubw[x][3],0))
                        }
                }]
                influx_client.write_points(influx_metric)



cursor_sw.close()
cursor_ne.close()
print("process ending at %s"%datetime.datetime.now())

