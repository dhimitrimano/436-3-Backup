import boto3
from botocore.client import ClientError
import datetime
import os
import pytz
import sys
import zoneinfo

print("Welcome to the Backup program!")

s3 = boto3.resource("s3")
client = boto3.client("s3")

buckets = s3.buckets.all()

if len(sys.argv) != 3:
    print("Error: Only two args allowed, each surrounded by double quotes!")
    print("If you did the above, remember not to finish off any arg by using the character '\\'!")
    quit()

if len(sys.argv[1]) == 0 or len(sys.argv[2]) == 0:
    print("Error: Length of args must be greater than zero!")
    quit()

direc = [sys.argv[1], sys.argv[2]]

if not os.path.isdir(direc[0]):
    print("Error: Directory",direc[0],"does not exist!")
    quit()

os.chdir(direc[0])

if (s3.Bucket(direc[1]) not in buckets):
    try:
        print("Creating Bucket",direc[1]+"..")
        s3.create_bucket(Bucket=direc[1])
        print("Bucket",direc[1],"created.")
    except:
        print("Error: Something went wrong in the AWS cloud!")
        print("Remember! Bucket names must be between 3 and 63 characters (inclusive),")
        print("consist only of lowercase letters, numbers, dots, and hyphens,")
        print("and must start and end with a letter or number!")
        quit()
else:
    print("Bucket",direc[1],"already exists!")

direc[0] = direc[0].replace("\\", "/")
if direc[0][len(direc[0]) - 1] == '/':
    direc[0] = direc[0][:-1]
    pass
directory = direc[0]

def compare(directory, file):
    try:
        client.head_object(Bucket=direc[1], Key=file)
    except ClientError:
        # Not found
        return True
    object = s3.Object(direc[1],file)
    modified = object.last_modified.replace(tzinfo=pytz.UTC)
    cloud_mod = datetime.datetime.fromtimestamp(os.path.getmtime(directory+"/"+file)).replace(tzinfo=pytz.UTC) + datetime.timedelta(hours=-8)
    print(modified , cloud_mod)
    return modified <= cloud_mod

def to_cloud(prefolder, folder, file):
    name = ""
    if (folder != ""):
        name = folder+"/"+file
    else:
        name = "/"+file
    name = name[1:]
    if compare(prefolder, name):
        try:
            client.upload_file(prefolder+"/"+name, direc[1], name)
            print("Uploading file at "+prefolder+"/"+name+" as '"+name+"'..")
        except ClientError as e:
            print("Error while uploading file at '"+prefolder+"/"+name+"'!")
        pass
    else:
        print("File at "+prefolder+"/"+name+" already exists in the bucket!")
    pass

def upload(prefolder, folder):
    for file in os.listdir(prefolder+folder):
        if os.path.isfile(os.path.join(prefolder+folder, file)):
            to_cloud(prefolder, folder, file)
        elif os.path.isdir(os.path.join(prefolder+folder, file)):
            upload(prefolder, folder+"/"+file)
    pass

for bucket in buckets:
    pass

upload(directory, "")

print("Finished uploading "+directory+"/ and subdirectories to "+direc[1])