import json
import re
import os


class DataLoader:

    @staticmethod
    def load_from_file(db, filepath):
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "workload", filepath)
        with open(filepath, "r") as file:
            lines = file.readlines()

        insert_mode = False

        for line in lines:
            line = line.strip()

            if line == "INSERT":
                insert_mode = True
                continue

            if line == "END" and insert_mode:
                break

            if insert_mode and line.startswith("KEY:"):

                # Example line:
                # KEY: A_1, VALUE: {name: "Account-1", balance: 153}

                key_part, value_part = line.split("VALUE:")
                key = key_part.replace("KEY:", "").replace(",", "").strip()

                # Extract dictionary part
                value_str = value_part.strip()

                # Convert workload-style dict to valid JSON
                # Add quotes around field names
                value_str = re.sub(r'(\w+):', r'"\1":', value_str)

                # Now it becomes valid JSON
                value = json.loads(value_str)

                db.put(key, value)

        print("Initial data loaded successfully.")
