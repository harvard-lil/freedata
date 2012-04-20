#!/usr/bin/python

"""
This is the access point for the Free Data logic. We keep things simple 
here. Setup the config and call the methods in FreeData
"""

from FreeData import FreeData
from ConfigParser import SafeConfigParser

# Load our config
config = SafeConfigParser()
config.read('../etc/config.ini')

# Get one of our heavy-lifting objects
free_data = FreeData(config)

# Start doing some work
# TODO: this business can probably be handled by the FreeData init method
free_data.generate_list_of_files()
free_data.get_total_record_count()
free_data.create_tar()
free_data.get_md5_and_size()
free_data.package_stats()
free_data.send_to_aws()
