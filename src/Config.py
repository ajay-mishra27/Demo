import os
import json

class Config(object):
    def __init__(self):
        config_path = get_config_file()
        prop_config = None

        with open(config_path, 'r') as file:
            prop_config = json.load(file)
        
        for eachKey in prop_config:
            self.__dict__[eachKey] = prop_config[eachKey]
    
def get_config_file():
    localFile = "./config/main/config.json"

    if os.path.exists(localFile):
        return localFile
    else:
        raise("No Config file found please check it")

properties = Config()



