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
ne_circles = ['HP', 'DL', 'WB', 'BH', 'OR', 'AS', 'NE', 'PB', 'HR', 'UW', 'KO', 'UE', 'JK']
sw_circles = ['CH', 'GJ', 'KE', 'KK', 'MP', 'MU', 'RJ', 'TN', 'AP', 'MH']
all_circles = ne_circles + sw_circles
n_circles = ['HP', 'DL', 'PB', 'HR', 'UW', 'UE', 'JK']
s_circles = ['CH','KE', 'KK', 'TN', 'AP']
e_circles=['WB','BH','OR','AS','NE','KO']
w_circles=['MH','MU','MP','GJ','RJ']
#past hour:current hour-1
print("process starting at %s"%datetime.datetime.now())

today_date = datetime.datetime.now()
today_date -= datetime.timedelta(hours=1)
today_date_ymd= today_date.strftime("%Y-%m-%d")
current_hour = today_date.strftime("%H")
old_today_date = datetime.datetime.now()
old_today_date -= datetime.timedelta(hours=7)
old_today_date_ymd= old_today_date.strftime("%Y-%m-%d")
old_current_hour = old_today_date.strftime("%H")

today_minus_1_date = datetime.datetime.now()
today_minus_1_date -= datetime.timedelta(days=1)
today_minus_1_date_ymd= today_minus_1_date.strftime("%Y-%m-%d")

print("Running Process for date: {0} and hour: {1}".format(today_date, current_hour))
#getting average count of the past hour of today from influx db to calculate anomaly of the past  hour.
influx_data = influx_client.query("SELECT * FROM hubs_recharge_events_graphs_final where time >= '{0} 18:30:00' and time <= '{1} 17:30:00'".format(today_minus_1_date_ymd,today_date_ymd))
modified_influx_data = {}
for resultset_value in influx_data:
        for each in resultset_value:
                print each
                if not modified_influx_data.get(each.get('Hub')):
                        modified_influx_data[each.get('Hub')] = {}
                if each.get('Hour') == current_hour:
                        modified_influx_data[each.get('Hub')] = each.get('average_recharge_count')

print "modified_influx_data: {0}".format(modified_influx_data)
#extracting entire data from oracle and getting hubwise data from the extracted data.
north_current=0
south_current=0
east_current=0
west_current=0
load_previous=datetime.datetime.now()

for each_circle in all_circles:

        sql="""
                SELECT
                        '{0}' AS CIRCLE,
                        COUNT(*) AS RECHARGE_COUNT
                FROM
                        DARTSOFR_{0}.RECHARGE_EVENTS
                PARTITION FOR (to_date('{1}','YYYY-MM-DD'))
                WHERE TO_CHAR(TRANSACTION_DATE, 'HH24') = '{2}'
            """.format(each_circle,today_date_ymd,current_hour)

        if each_circle in ne_circles:
                cursor_ne.execute(sql)
                recharge_events_data = cursor_ne.fetchall()
        elif each_circle in sw_circles:
                cursor_sw.execute(sql)
                recharge_events_data = cursor_sw.fetchall()
        else:
                print("Unknown circle: {0} received, Skipping it.".format(each_circle))
                continue
        print "recharge_events_data: {0}".format(recharge_events_data)

        if(recharge_events_data[0][0] in n_circles):
                north_current+=recharge_events_data[0][1]

        elif(recharge_events_data[0][0] in e_circles):
                east_current+=recharge_events_data[0][1]
        elif(recharge_events_data[0][0] in w_circles):
               west_current+=recharge_events_data[0][1]
        elif(recharge_events_data[0][0] in s_circles):
                south_current+=recharge_events_data[0][1]


#combinig the past hour  recharge count of each hub into a list of list.
hubwise_recharge=[["North",north_current],["South",south_current],["East",east_current],["West",west_current]]
print(hubwise_recharge)
#calculate anomaly and anomaly percentage for each hub and append the values in the respective hub's list.
for x in range(len(hubwise_recharge)):
        average_count = modified_influx_data.get(hubwise_recharge[x][0])
        print "average_count: {0}".format(average_count)
        anomaly = hubwise_recharge[x][1] - average_count
        print "anomaly: {0}".format(anomaly)
        anomaly_percentage = int((float(anomaly)/float(average_count))*100)
        print "anomaly_percentage: {0}".format(anomaly_percentage)
        hubwise_recharge[x].append(anomaly)
        hubwise_recharge[x].append(anomaly_percentage)
        print(recharge_events_data)
        print "\n"
#loading final data in influx db for the past hour.
for x in hubwise_recharge:
                influx_metric = [{
                        'measurement': 'hubs_recharge_events_graphs_final',
                        "tags": {
                                "Hub":x[0],
                               "Hour": current_hour
                        },
                        'time': old_today_date_ymd+"T{0}:30:00Z".format(old_current_hour),
                        'fields': {
                                'current_recharge_count': x[1],
                                'anomaly_hubwise':x[2],
                                'anomaly_percentage_hubwise':x[3]
                        }
                }]
                influx_client.write_points(influx_metric)

cursor_sw.close()
cursor_ne.close()
print("process ending at %s"%datetime.datetime.now())

