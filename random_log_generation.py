import random
import datetime

def generate_log(num_entries, filename):
    operator_types = [
        ("Source: Custom Source", 1),
        ("Splitter FlatMap", 2),
        ("Count -> Latency Sink", 1)
    ]
    
    start_time = datetime.datetime.now()
    
    with open(filename, 'w') as file:
        file.write("# operator_id,operator_instance_id,total_number_of_operator_instances,epoch_timestamp,true_processing_rate,true_output_rate,observed_processing_rate,observed_output_rate\n")
        
        for _ in range(num_entries):
            for operator_id, num_instances in operator_types:
                for instance_id in range(1, num_instances + 1):
                    epoch_timestamp = int(start_time.timestamp() * 1000)
                    if operator_id == "Source: Custom Source":
                        true_processing_rate = 0
                        true_output_rate = random.uniform(50000, 200000)
                    elif operator_id == "Splitter FlatMap":
                        true_processing_rate = random.uniform(40000, 150000)
                        true_output_rate = random.uniform(200000, 1600000)
                    else:  # Count -> Latency Sink
                        true_processing_rate = random.uniform(1000000, 2500000)
                        true_output_rate = 0
                    
                    observed_processing_rate = max(0, true_processing_rate * random.uniform(0.5, 1.1))
                    observed_output_rate = max(0, true_output_rate * random.uniform(0.5, 1.1))
                    
                    line = f"{operator_id},{instance_id},{num_instances},{epoch_timestamp},{true_processing_rate},{true_output_rate},{observed_processing_rate},{observed_output_rate}\n"
                    file.write(line)
                    
                start_time += datetime.timedelta(seconds=random.randint(1, 10))

generate_log(10, 'flink_rates.log')