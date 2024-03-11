from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta
import random
import numpy
from threading import Timer

bootstrap_servers = ['localhost:9092']
topicName = 'machine_data'
num_machines = 1350
alarm_timestamps = []
check_interval = 60

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

process_parameters = [
    "ShotControl - IN", "ShotControl - T0", "ShotControl - C1", "ShotControl - V1",
    "ShotControl - T1", "ShotControl - GP", "ShotControl - C2", "ShotControl - V2",
    "ShotControl - T2", "ShotControl - VM", "ShotControl - CC", "ShotControl - T3",
    "ShotControl - TD", "ShotControl - PM", "ShotControl - PF", "ShotControl - VA",
    "ShotControl - PR", "ShotControl - PV", "ShotControl - PS", "ShotControl - FC",
    "ShotControl - SM", "ShotControl - TC", "ShotControl - YY", "ShotControl - MM",
    "ShotControl - DD", "ShotControl - h", "ShotControl - m", "ShotControl - s",
    "T - Alloy Temperature", "T3S - Start Time Multiply", "Lubrication Qty on Auto",
    "Lubrication Sensor Measure", "Lubrication Trigger", "Shot Trigger", "HeartBeat Signal"
]

parameter_ranges = {
    "ShotControl - C1": (173, 891),
    "ShotControl - V1": (2, 44),
    "ShotControl - T1": (1603, 9624),
    "ShotControl - GP": (12, 364),
    "ShotControl - C2": (105, 375),
    "ShotControl - V2": (33, 167),
    "ShotControl - T2": (119, 532),
    "ShotControl - VM": (69, 780),
    "ShotControl - CC": (0, 146),
    "ShotControl - T3": (4, 388),
    "ShotControl - TD": (0, 192),
    "ShotControl - PM": (1, 452),
    "ShotControl - PF": (0, 510),
    "ShotControl - VA": (1266, 3985),
    "ShotControl - PR": (44, 351),
    "ShotControl - PS": (776, 1274),
    "ShotControl - SM": (1, 87),
    "T - Alloy Temperature": (660, 700)
}

def check_alarms():
    current_time = datetime.now()
    recent_alarms = [timestamp for timestamp in alarm_timestamps if current_time - timestamp <= timedelta(seconds=check_interval)]
    print(f"Alarms generated in the last {check_interval} seconds: {len(recent_alarms)}")
    Timer(check_interval, check_alarms).start()

def generate_data(machine_id):
    data = {'timestamp': 0, 'machine_id': f'machine_{machine_id}', 'parameters': {}}
    for parameter in process_parameters:
        if parameter in parameter_ranges:
            min_val, max_val = parameter_ranges[parameter]
            if parameter == "T - Alloy Temperature" and random.random() < 0.00008:
                value = round(random.uniform(900, 950), 2)
                alarm_timestamps.append(datetime.now())
            else:
                value = round(random.uniform(min_val, max_val), 2)
            data['parameters'][parameter] = value
        else:
            data['parameters'][parameter] = round(numpy.random.uniform(10, 100.0), 2)
    data['timestamp'] = datetime.now().isoformat()
    return data

try:
    Timer(check_interval, check_alarms).start()
    while True:
        start_time = time.time()
        for i in range(num_machines):
            data = generate_data(i)
            producer.send(topicName, value=data)
        loop_time = time.time() - start_time
        print(f"Generation time for {num_machines} machines : {loop_time:.2f} seconds")
            
except KeyboardInterrupt:
    current_time = datetime.now()
    alarms_in_x_time = [timestamp for timestamp in alarm_timestamps if current_time - timestamp <= timedelta(seconds=check_interval)]
    print(f"Data generation stopped. Alarms generated in the last {check_interval} seconds: {len(alarms_in_x_time)}")

    total_runtime_seconds = (alarm_timestamps[-1] - alarm_timestamps[0]).total_seconds()
    total_alarms = len(alarm_timestamps)
    average_alarms_per_second = total_alarms / total_runtime_seconds

    print(f"Data generation stopped. Total runtime: {total_runtime_seconds:.2f} seconds")
    print(f"Total alarms: {total_alarms}")
    print(f"Average number of alarms per second: {average_alarms_per_second:.4f}")
    
