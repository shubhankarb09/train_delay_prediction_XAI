import requests
# import my_proto_pb2  # Import your Protocol Buffer message type
import json
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from jsontocsv import instruct


def download_and_parse_protobuf(url):
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(url)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the Protocol Buffer content
            proto_message = gtfs_realtime_pb2.FeedMessage()
            proto_message.ParseFromString(response.content)
            feed_dict = MessageToDict(proto_message)
            with open('output.json', 'w') as json_file:
                json.dump(feed_dict, json_file, indent=2)

        else:
            print("Failed to download file. Status code:", response.status_code)
    except Exception as e:
        print("An error occurred:", str(e))

if __name__ == "__main__":
    # Example usage
    url = "https://realtime.gtfs.de/realtime-free.pb"  
    download_and_parse_protobuf(url)
    instruct()
