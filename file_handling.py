from io import TextIOWrapper
import json
import subprocess as sp
import errors


class FileHandling:
    def __init__(self) -> None:
        self.json_file_name = "local_db.json"
        self.json_file = self.initialise_json()

    def get_json(self):
        f = open(self.json_file, mode="r")
        f_json = json.loads(f)
        f.close()
        return f_json

    def initialise_json(self):
        f = open(self.json_file_name, mode="w")
        f.write("{}")
        f.close()
        return sp.getoutput(["pwd"]) + "/" + self.json_file_name

    def write_property(self, key: str, value):
        f_in = open(self.json_file, "r")
        f_json = json.load(f_in)
        f_json[key] = value
        f_out_json = json.dumps(f_json)
        f_out = open(self.json_file, "w")
        f_out.write(f_out_json)
        f_in.close()
        f_out.close()

    def write_slave_property(self, slave_ip: str, key: str, value):
        f_in = open(self.json_file, "r")
        f_json = json.load(f_in)
        f_json[slave_ip][key] = value
        f_out_json = json.dumps(f_json)
        f_out = open(self.json_file, "w")
        f_out.write(f_out_json)
        f_in.close()
        f_out.close()

    def read_property(self, key: str):
        f = open(self.json_file, mode="r")
        f_json = json.load(f)
        f.close()
        return f_json[key]
