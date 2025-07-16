import os
import sys
import json
import shutil

##
## check_path():
## Check that a directory exists and create it if it doesn't
##
def check_path(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)            
        except OSError as e:
            print(f"Error creating directory: {e}")
            sys.exit(1)

## get_config()
## get the json config file for this script
## return: dict containing the contents of the config of the specifc section based on the keys

def get_config(filepath,key=None):
    with open(filepath) as f:
        config = json.load(f)
    if key != None:
        return config[key]        
    return config

##
## split node path: Split up the node_modules path to the IG name and file
##
def split_node_path(filepath):
    """
    Splits the given file path into '<module>: <filename>', where <module> is the part of the path
    immediately after 'node_modules' and <filename> is the file name.
    """
    # Extract the filename
    filename = os.path.basename(filepath)
    
    # Split the path by directory separator
    parts = filepath.split(os.sep)
    
    # Find the index of 'node_modules'
    try:
        node_modules_index = parts.index('node_modules')
        # Get the part of the path immediately after 'node_modules'
        module_part = parts[node_modules_index + 1]
    except ValueError:
        # Handle cases where 'node_modules' is not in the path
        module_part = "unknown_module"
    
    # Format the result
    return f'{filename}'