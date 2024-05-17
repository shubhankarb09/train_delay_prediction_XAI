import json
import csv
import datetime
import pytz
import os
import pandas as pd

# Function to read JSON file and return data
def read_json_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data
def returnCSV():
    df = pd.read_csv('static.csv')
    return df
def returnDelay(df, trip_id, stop_id, stop_sequence):
    # filterdCol = df[(df['trip_id'] == trip_id & df['stop_id'] == stop_id & df['stop_sequence'] == stop_sequence)]
    filterdCol = df[(df['trip_id'] == trip_id) & (df['stop_id'] == stop_id) & (df['stop_sequence'] == stop_sequence)]

    if not filterdCol.empty:
        return filterdCol.iloc[0]['arrival_time']

def write_to_csv(trip_updates):
    file_exists = os.path.isfile('realtime.csv')
    # df = returnCSV()
    with open('realtime_final.csv', 'a' if file_exists else 'w', newline='') as csvfile:
        fieldnames = ['trip_id', 'StartDate', 'stop_sequence', 'stop_id', 'arrival time',
                      'departure time', 'ScheduleRelationship', 'delay']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        # Writing trip updates to CSV
        for trip_update in trip_updates:
            trip_id = trip_update['tripUpdate']['trip']['tripId']
            start_date = trip_update['tripUpdate']['trip']['startDate']
            schedule_relationship = trip_update['tripUpdate']['trip']['scheduleRelationship']
            stop_time_updates = trip_update['tripUpdate']['stopTimeUpdate']

            for index, stop_time_update in enumerate(stop_time_updates):
                arrival_time = stop_time_update['arrival']['time'] if 'arrival' in stop_time_update else ''
                departure_time = stop_time_update['departure']['time'] if 'departure' in stop_time_update else ''
                timezone = pytz.timezone('Europe/Paris')
                # delay = returnDelay(df=df,trip_id=trip_id, stop_id=stop_time_update['stopId'], stop_sequence=stop_time_update['stopSequence'])

                if arrival_time != '':
                    arrival_time = datetime.datetime.utcfromtimestamp(int(arrival_time))
                    arrival_time = arrival_time.replace(tzinfo=pytz.utc).astimezone(timezone)
                    arrival_time = arrival_time.strftime('%H:%M:%S')
                if departure_time != '':
                    departure_time = datetime.datetime.utcfromtimestamp(int(departure_time))
                    departure_time = departure_time.replace(tzinfo=pytz.utc).astimezone(timezone)
                    departure_time = departure_time.strftime('%H:%M:%S')
                writer.writerow({
                    'trip_id': trip_id,
                    'StartDate': start_date,
                    'ScheduleRelationship': schedule_relationship,
                    'stop_sequence': stop_time_update['stopSequence'],
                    'stop_id': stop_time_update['stopId'],
                    'arrival time': arrival_time,
                    'departure time': departure_time,
                    'ScheduleRelationship': stop_time_update['scheduleRelationship'],
                    # 'delay': delay,
                })

        
    print("CSV file updated successfully.")
def merge():
    pd.merge()
# Example usage:
# write_to_csv(trip_updates_data, alerts_data)
def instruct():
    data = read_json_file('output.json')
    trip_updates = [entity for entity in data['entity'] if 'tripUpdate' in entity]
    write_to_csv(trip_updates)