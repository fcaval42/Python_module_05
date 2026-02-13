# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  stream_processor.py                               :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: fcaval <fcaval@student.42.fr>             +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/02/05 11:07:30 by fcaval          #+#    #+#               #
#  Updated: 2026/02/10 10:55:49 by fcaval          ###   ########.fr        #
#                                                                           #
# ************************************************************************* #

from typing import Any, List
from abc import ABC, abstractmethod


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):

    def process(self, data: List) -> str:
        count = 0
        sum = 0

        if self.validate(data) is False:
            return

        for number in data:
            count += 1
            sum += number
        avg = sum / count

        return f"Processed {count} numeric values, sum={sum}, avg={avg}"

    def validate(self, data: List) -> bool:
        try:
            for number in data:
                if type(number) is not int:
                    raise TypeError(f"{number} is not int")
            return True
        except TypeError as e:
            print(f"Error: {e}")
            return False


class TextProcessor(DataProcessor):

    def process(self, data: str) -> str:

        if self.validate(data) is False:
            return

        return (f"Processed text: {len(data)} characters, "
                f"{len(data.split())} words")

    def validate(self, data: str) -> bool:
        try:
            if type(data) is not str:
                raise TypeError(f"{data} is not str")
            return True
        except TypeError as e:
            print(f"Error: {e}")
            return False


class LogProcessor(DataProcessor):

    def process(self, data: Any) -> str:

        if self.validate(data) is False:
            return

        if "ERROR" in data:
            return ("[ALERT] ERROR level detected: Connection timeout")
        else:
            return ("[INFO] INFO level detected: System ready")

    def validate(self, data: Any) -> bool:
        try:
            if data == 0:
                raise TypeError("No connection")
            return True
        except TypeError as e:
            print(f"Error: {e}")
            return False


def main() -> None:

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    num = NumericProcessor()
    data_num = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_num}")
    numeric_result = num.process(data_num)
    if num.validate(data_num) is True:
        print("Validation: Numeric data verified")
        print(num.format_output(numeric_result))
    print()

# ************************************************************************* #
#                                 Text                                      #
# ************************************************************************* #

    print("Initializing Text Processor...")
    text = TextProcessor()
    text_data = "Hello Nexus World"
    print(f'Processing data: "{text_data}"')
    text_result = text.process(text_data)
    if text.validate(text_data) is True:
        print("Validation: Text data verified")
        print(text.format_output(text_result))
    print()

# ************************************************************************* #
#                                  Log                                      #
# ************************************************************************* #

    print("Initializing Log Processor...")
    log = LogProcessor()
    log_data = "ERROR: Connection timeout"
    print(f'Processing data: "{log_data}"')
    log_result = log.process(log_data)
    if log.validate(log_data) is True:
        print("Validation: Log entry verified")
        print(log.format_output(log_result))
    print()

# ************************************************************************* #
#                               Polymorphic                                 #
# ************************************************************************* #

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    types = [NumericProcessor(), TextProcessor(), LogProcessor()]
    datas = [[2, 2, 2], "Hello world!", "INFO: connexion ok"]
    i = 1
    for type, data in zip(types, datas):
        print(f"Result {i}: {type.process(data)}")
        i += 1
    print()

    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
