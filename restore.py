import boto3
from botocore.client import ClientError
import os
import sys

print("Welcome to the Restore program!")

s3 = boto3.resource("s3")
client = boto3.client("s3")

buckets = s3.buckets.all()

if len(sys.argv) != 3:
    print("Error: Only two args allowed, each surrounded by double quotes!")
    quit()

if len(sys.argv[1]) == 0 or len(sys.argv[2]) == 0:
    print("Error: Length of args must be greater than zero!")
    quit()

direc = [sys.argv[2], sys.argv[1]]

if (s3.Bucket(direc[1]) not in buckets):
    print("Error: Bucket does not exist!")
    quit()

direc[0] = direc[0].replace("\\", "/")
if direc[0][len(direc[0]) - 1] != '/':
    direc[0] += "/"
    pass
directory = direc[0]

try:
    os.makedirs(directory, exist_ok = True)
except OSError as e:
    print("Error: Directory",directory,"cannot be created!")
    quit()

os.chdir(directory)

contents = client.list_objects_v2(Bucket=direc[1])

if len(contents) == 0:
    print("Bucket is empty!")
    quit()

keys = []
resp = client.list_objects_v2(Bucket=direc[1])
for obj in resp['Contents']:
    keys.append(obj['Key'])

def compare(folder, file, orig_key):
    if os.path.exists(directory+folder+file):
        object = s3.Object(direc[1],folder+orig_key)
        file_size = object.content_length
        return file_size != os.path.getsize(directory+folder+file)
    return True

def restore(curr_folder):
    try:
        os.makedirs(directory+curr_folder, exist_ok = True)
    except OSError as e:
        print("Error: Directory",directory+curr_folder,"cannot be created!")
        return
    os.chdir(directory + curr_folder)
    keys.sort(key=lambda x: x.count('/'), reverse = True)
    new_keys = []
    if (curr_folder != ""):
        for i in range(0, len(keys)):
            if keys[i].find(curr_folder) == 0:
                keys[i] = keys[i][keys[i].find(curr_folder) + len(curr_folder):]
    matching = [s for s in keys if '/' not in s]
    for key in matching:
        temp = 1
        tempkey = key
        extension = ""
        if tempkey.rfind(".") != -1:
            tempkey = key[:key.rfind(".")]
            extension = key[key.rfind("."):]
            pass
        if compare(curr_folder, tempkey+extension, key):
            while os.path.exists(directory+curr_folder+tempkey+extension):
                if (temp != 1):
                    tempkey = tempkey[:-str(temp) - 1]
                tempkey += " " + str(temp)
            tempkey += extension
            try:
                client.download_file(direc[1], curr_folder+key, directory+curr_folder+tempkey)
                print("Downloading file '"+key+"' to '"+directory+curr_folder+"' as '"+tempkey+"'..")
            except ClientError as e:
                print("Error while downloading file '"+tempkey+"' to '"+directory+curr_folder+"'!")
            keys.remove(key)
            pass
        else:
            print("File at "+curr_folder+key+" already exists in directory!")
            keys.remove(key)
    while len(keys) != 0:
        for i in range(0, len(keys)):
            if i < len(keys):
                key = keys[i]
                if key.find('/') != -1:
                    restore(curr_folder + key[:key.find('/')] + "/")
                    os.chdir(directory + curr_folder)
                    pass
    pass

restore("")

print("Finished downloading from bucket "+direc[1]+" to "+directory)