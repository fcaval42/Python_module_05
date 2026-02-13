# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  data_stream.py                                    :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: fcaval <fcaval@student.42.fr>             +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/02/06 11:39:25 by fcaval          #+#    #+#               #
#  Updated: 2026/02/09 16:55:51 by fcaval          ###   ########.fr        #
#                                                                           #
# ************************************************************************* #

from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str]
                    = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):

    def __init__(self, stream_id: Union[str, float, int]):
        super().__init__(stream_id)
        self.categorie = "Sensor data"
        self.operation = 0
        self.type = 'readings'
        self.criteria_sensor = 0

    def filter_data(self, data_batch: List[Any], criteria: Optional[str]
                    = None) -> List[Any]:

        filtered_data = []
        if criteria == "High-priority":
            for item in data_batch:
                if isinstance(item, str) and ':' in item:
                    key, value = item.split(':')

                    if key == 'temp' and float(value) > 25:
                        self.criteria_sensor += 1

                    if key in ['temp', 'humidity', 'pressure']:
                        filtered_data.append({key: float(value)})

            return filtered_data
        else:
            for item in data_batch:
                if isinstance(item, str) and ':' in item:
                    key, value = item.split(':')

                    if key in ['temp', 'humidity', 'pressure']:
                        filtered_data.append({key: float(value)})

            return filtered_data

    def process_batch(self, data_batch: List[Any]) -> str:

        temps = [item['temp'] for item in data_batch if 'temp' in item]

        if len(temps) == 0:
            self.operation = len(data_batch)
            return f"{self.operation} reading processed, no temp data"

        sum_data = sum(temps)
        len_data = len(temps)
        avg = sum_data / len_data
        self.operation = len(data_batch)

        return f"{self.operation} readings processed, avg temp: {avg}°C"

    def get_stats(self):
        return dict(operation=self.operation,
                    type=self.type,
                    categorie=self.categorie,
                    criteria_sensor=self.criteria_sensor)


class TransactionStream(DataStream):
    def __init__(self, stream_id: Union[str, float, int]):
        super().__init__(stream_id)
        self.categorie = 'Transaction data'
        self.operation = 0
        self.type = 'operations'
        self.large_transaction = 0

    def filter_data(self, data_batch: List[Any], criteria: Optional[str]
                    = None) -> List[Any]:

        filtered_data = []
        if criteria == "High-priority":
            for item in data_batch:
                if isinstance(item, str) and ':' in item:
                    key, value = item.split(':')

                    if key == 'sell' and float(value) >= 200:
                        self.large_transaction += 1

                    if key in ['buy', 'sell']:
                        filtered_data.append({key: int(value)})

            return filtered_data
        else:
            for item in data_batch:
                if isinstance(item, str) and ':' in item:
                    key, value = item.split(':')

                    if key in ['buy', 'sell']:
                        filtered_data.append({key: int(value)})

            return filtered_data

    def process_batch(self, data_batch: List[Any]) -> str:

        buy = [item['buy'] for item in data_batch if 'buy' in item]
        sell = [item['sell'] for item in data_batch if 'sell' in item]

        total = sum(buy) - sum(sell)

        operator = "+" if total > 0 else "-"
        self.operation = len(data_batch)

        return f"{self.operation} operations, net flow: {operator}{total} "
    "units"

    def get_stats(self):
        return dict(categorie=self.categorie,
                    operation=self.operation,
                    type=self.type,
                    large=self.large_transaction)


class EventStream(DataStream):
    def __init__(self, stream_id: Union[str, float, int]):
        super().__init__(stream_id)
        self.categorie = 'Event data'
        self.operation = 0
        self.type = 'events'

    def filter_data(self, data_batch: List[Any], criteria: Optional[str]
                    = None) -> List[Any]:

        filtered_data = []

        for item in data_batch:
            if isinstance(item, str) and ':' not in item:
                if item in ['login', 'error', 'logout']:
                    filtered_data.append(item)

        return filtered_data

    def process_batch(self, data_batch: List[Any]) -> str:

        error = [item for item in data_batch if item == 'error']
        self.operation = len(data_batch)

        return f"{self.operation} events, {len(error)} error detected"

    def get_stats(self):
        return dict(categorie=self.categorie,
                    operation=self.operation,
                    type=self.type)


class StreamProcessor():
    def process_streams(self, streams: List[DataStream], data_batch: List[Any],
                        criteria: Optional[str] = None) -> List[Dict]:
        results = []
        for stream in streams:
            filtered = stream.filter_data(data_batch, criteria=criteria)
            stream.process_batch(filtered)
            results.append(stream.get_stats())
        return results


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    databatch = ['temp:22.5', 'humidity:65', 'pressure:1013', 'buy:100',
                 'sell:150', 'buy:75', 'login', 'error', 'logout']

# ************************************************************************* #
#                              SensorStream                                 #
# ************************************************************************* #

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: Environnemental Data")
    sensor_data = sensor.filter_data(databatch)
    sensor_analysis = sensor.process_batch(sensor_data)
    print(f"Processing sensor batch: {sensor_data}")
    print(f"Sensor analysis: {sensor_analysis}")
    print()

# ************************************************************************* #
#                           TransactionStream                               #
# ************************************************************************* #

    print("Initializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print(f"Stream ID: {transaction.stream_id}, Type: Financial Data")
    transaction_data = transaction.filter_data(databatch)
    transaction_analysis = transaction.process_batch(transaction_data)
    print(f"Processing transaction batch: {transaction_data}")
    print(f"Transaction analysis: {transaction_analysis}")
    print()

# ************************************************************************* #
#                             EventStream                                   #
# ************************************************************************* #

    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: System Events")
    event_data = event.filter_data(databatch)
    event_analysis = event.process_batch(event_data)
    print(f"Processing event batch: {event_data}")
    print(f"Event analysis: {event_analysis}")
    print()

# ************************************************************************* #
#                              Polymorphic                                  #
# ************************************************************************* #

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    data_batch2 = ['temp:30', 'temp:35', 'buy:100', 'sell:150',
                   'buy:75', 'sell:200', 'login', 'error', 'logout']

    streams = [sensor, transaction, event]

    processor = StreamProcessor()
    stats = processor.process_streams(streams, data_batch2,
                                      criteria="High-priority")

    print("Batch 1 Results:")
    for stat in stats:
        print(f"- {stat['categorie']}: {stat['operation']} {stat['type']} "
              "processed")

    print("\nStream filtering active: High-priority data only")
    print(f"Filtered results: {stats[0]['criteria_sensor']} critical sensor "
          f"alerts, {stats[1]['large']} large transaction\n")

    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
