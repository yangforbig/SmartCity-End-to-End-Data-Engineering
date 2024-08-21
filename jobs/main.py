import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
import uuid


LONDON_COORDINATES = {
    'latitude' : 51.5074,
    'longtitude' : -0.1278
}

BIRMINGHAM_COORDINATES = {
    'latitude' : 52.4862,
    'longtitude' : -1.8904
}

# calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGTITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longtitude'] - LONDON_COORDINATES['longtitude']) / 100

# Environment Variables for configuration

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIS = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42)
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60))
    return start_time

def generate_gps_data(device_id, timestamp, vechicle_type='private'):
    return {
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction':'North-East',
        'vehicleType': vechicle_type

    }

def generate_traffic_camera_data(device_id, timestamp, location,camera_id):
    return {
        'id' : uuid.uuid4(),
        'deviceId' : device_id,
        'cameraId': camera_id,
        'location': location, 
        'timestamp':timestamp,
        'snapshot':'Base64EncodedString'
    }

def generate_weather_data(devcie_id, timestamp, location):
    return {
        'id' : uuid.uuid4(),
        'deviceId': devcie_id,
        'locatoin':location,
        'timestamp':timestamp,
        'temperature' : random.uniform(-5, 26),
        'weatherCondition':random.choice(['Sunny','Cloudy','Rainy','Snowy']),
        'precipitation':random.uniform(0, 25),
        'windSpeed' : random.uniform(0, 100),
        'humidity':random.randint(0,100),
        'airQualityIndex':random.uniform(0, 500)
    }

def generate_emergency_data(device_id, timestamp, location ):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'incident_id' : uuid.uuid4(),
        'type':random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'location':location,
        'status':random.choice(['Actice', 'Resolved']),
        'description': 'Description of the accident'
    }


def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longtitude'] += LONGTITUDE_INCREMENT

    #add some randomness for simulation
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longtitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location':(location['latitude'], location['longtitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make':'Audi',
        'mode':'R8',
        'year':'2023',
        'fuelType':'Hybrid'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key = str(data['id']),
        value = json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery = delivery_report 
    )

    producer.flush()

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],vehicle_data['location'],'Camera123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(weather_data)
        # print(emergency_incident_data)
        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longtitude'] ):
            print('Vehicle has reached Birmingham. Simulation ending...')
            break


        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIS, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers' : KAFKA_BOOTSTRAP_SERVERS,
        'error_cb' : lambda err: print(f'Kafka error:{err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer,'Vehcile-Noma')
    
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    
    except Exception as e:
        print(f'Unexpected Error occurred: {e}')

    