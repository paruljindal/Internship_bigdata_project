import datetime
from influxdb import InfluxDBClient
# influx db configuration
influx_client = InfluxDBClient('10.5.252.195', 8086, 'root', 'Mycl0ud@2016', 'telegraf')

print("process starting at %s"%datetime.datetime.now())

today_date = datetime.datetime.now()
today_date_ymd= today_date.strftime("%Y-%m-%d")

#loading data of previous 7 days to calculate average.
influx_data = influx_client.query("SELECT * FROM hubs_recharge_events_graphs_final  WHERE time >now()- 8d and time < now()- 1d")

modified_influx_data = {}
if influx_data:
        for resultset_value in influx_data:
                for each in resultset_value:
                        if not modified_influx_data.get(each.get('Hub')):
                                modified_influx_data[each.get('Hub')] = {}
                        if not modified_influx_data.get(each.get('Hub')).get(each.get('Hour')):
                                modified_influx_data[each.get('Hub')][each.get('Hour')] = []
                        modified_influx_data[each.get('Hub')][each.get('Hour')].append(each.get('current_recharge_count'))
else:
    print("Influx data in none")

#Making a list of data of current recharges of each hour of past 7 days for each hub to calculate the average of each hour of current date.
#Also we will load the data(average count of each hub of each hour) in influx db.
if modified_influx_data:
    for each_hub in modified_influx_data.keys():
        for each_hour in modified_influx_data.get(each_hub).keys():
                current_recharges = modified_influx_data.get(each_hub).get(each_hour)
                current_recharges1 = filter(lambda a: a != None, current_recharges)
                if not current_recharges1:
                        continue
                time_from_influx = today_date_ymd+"T{0}:00:00Z".format(each_hour)
                datetime_object = datetime.datetime.strptime(time_from_influx,'%Y-%m-%dT%H:%M:%SZ')
                datetime_object -= datetime.timedelta(hours=5)
                datetime_object -= datetime.timedelta(minutes=30)

                influx_metric = [{
                                'measurement': 'hubs_recharge_events_graphs_final',
                                "tags": {
                                        "Hub":each_hub,
                                        "Hour": each_hour
                                },
                                'time': datetime.datetime.strftime(datetime_object, '%Y-%m-%dT%H:%M:%SZ'),
                                'fields': {
                                        'average_recharge_count': sum(current_recharges1)/\
                                                                        len(current_recharges1)
                                }
                }]
                influx_client.write_points(influx_metric)
else:
    print("Modified influx data is none")

print("process ending at %s"%datetime.datetime.now())

