#
# Main program for photoapp program using AWS S3 and RDS to
# implement a simple photo application for photo storage and
# viewing.
#
# Authors:
#   SATYA BHARADWAJ PAKKI (he/him)
#   Prof. Joe Hummel (initial template)
#   Northwestern University
#

import datatier  # MySQL database access
import awsutil  # helper functions for AWS
import boto3  # Amazon AWS

import uuid
import pathlib
import logging
import sys
import os
import sqlite3

from configparser import ConfigParser

import matplotlib.pyplot as plt
import matplotlib.image as img


###################################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number
  
  Parameters
  ----------
  None
  
  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """

  try:
    print()
    print(">> Enter a command:")
    print("   0 => end")
    print("   1 => stats")
    print("   2 => users")
    print("   3 => assets")
    print("   4 => download")
    print("   5 => download and display")
    print("   6 => upload")
    print("   7 => add user")

    cmd = int(input())
    return cmd

  except Exception as e:
    print("ERROR")
    print("ERROR: invalid input")
    print("ERROR")
    return -1


###################################################################
#
# stats
#
def stats(bucketname, bucket, endpoint, dbConn):
  """
  Prints out S3 and RDS info: bucket name, # of assets, RDS 
  endpoint, and # of users and assets in the database
  
  Parameters
  ----------
  bucketname: S3 bucket name,
  bucket: S3 boto bucket object,
  endpoint: RDS machine name,
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 
    print("S3 bucket name:", bucketname)

    assets = bucket.objects.all()
    print("S3 assets:", len(list(assets)))

    #
    # MySQL info:
    #
    print("RDS MySQL endpoint:", endpoint)

    # sql = """
    # select now();
    # """

    # row = datatier.retrieve_one_row(dbConn, sql)
    # if row is None:
    #   print("Database operation failed...")
    # elif row == ():
    #   print("Unexpected query failure...")
    # else:
    #   print(row[0])

  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))
  
  try:
    # Query 1: Retrieve number of users from the users table
    sql_users = "SELECT COUNT(*) FROM users;"
    row_users = datatier.retrieve_one_row(dbConn, sql_users)
    if row_users is None:
        print("Database operation failed (users)...")
    else:
        print(f"# of users: {row_users[0]}")

    # Query 2: Retrieve number of assets from the assets table
    sql_assets = "SELECT COUNT(*) FROM assets;"
    row_assets = datatier.retrieve_one_row(dbConn, sql_assets)
    if row_assets is None:
        print("Database operation failed (assets)...")
    else:
        print(f"# of assets: {row_assets[0]}")
  
  except Exception as e:
    logging.error(f"Failed to execute database queries: {str(e)}")
    print("**ERROR: Unable to retrieve data from the database.")

# users
#
def users(dbConn):
  """
  Retrieves and outputs the users from user table in descending order
  by user id. The output is indented for grading purposes.
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  try:
      # SQL query to fetch users sorted by user_id in descending order:
      sql = """
            SELECT userid, email, lastname, firstname, bucketfolder
            FROM users ORDER BY userid DESC; 
            """
      
      # Retrieve all rows:
      rows = datatier.retrieve_all_rows(dbConn, sql)
      
      if rows is None:
          print("ERROR: Database query failed for retrieving users.")
          return
      
      # Output each user, ensuring the format is correct:
      #print("Users in the database (sorted by user id):")
      for row in rows:
        print(f"""User id: {row[0]}\n  Email: {row[1]}\n  Name: {row[2]} , {row[3]}\n  Folder: {row[4]}""")
  
  except Exception as e:
      # Error handling:
      logging.error(f"An error occurred in the users function: {str(e)}")
      print("ERROR: Unable to retrieve users from the database.")
      print("ERROR DETAILS:", str(e))

def assets(dbConn):
  """
  Retrieves and outputs the assets from assets table in descending order
  by asset id. The output is indented for grading purposes.
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  try:
    # fetch assets
    sql = """
          SELECT assetid, userid, assetname, bucketkey
          FROM assets ORDER BY assetid DESC
          """
    
    #Retreive all rows
    rows = datatier.retrieve_all_rows(dbConn, sql)

    if rows is None:
        print("ERROR: Database query failed for retrieving assets.")
        return
    
    # Output each asset, ensuring the format is correct:
      
    for row in rows:
        print(f"""Asset id: {row[0]}\n  User id: {row[1]}\n  Original name: {row[2]}\n  Key name: {row[3]}""")

  except Exception as e:
    # Error handling:
      logging.error(f"An error occurred in the assets function: {str(e)}")
      print("ERROR: Unable to retrieve assets from the database.")
      print("ERROR DETAILS:", str(e))



def download(bucketname, bucket, dbConn, display):
  """
   Inputs an asset ID, looks up that asset in the database, downloads the file,
   and renames it based on the original filename. Also displays if required.
  
   Parameters
   ----------
 
   bucketname: S3 bucket name
   bucket: S3 boto bucket object
   dbConn: open connection to MySQL server
   display: displays image if set to 'TRUE'
  
   Returns
   -------
   nothing
  """
  try:
     
    a = input("Enter asset id>\n")
    sql = """
    SELECT assetname,bucketkey,bucketfolder 
    FROM assets 
    inner join users WHERE assets.userid = users.userid and  assetid = %s;
    """
    row = datatier.retrieve_all_rows(dbConn, sql, [a])
    if not row:
      print("No such asset...")
    else:
      bname = awsutil.download_file(bucket, row[0][1])
      os.rename(bname, row[0][0])
      print(f"Downloaded from S3 and saved as ' {row[0][0]} '")

      if display:
        image = img.imread(row[0][0])
        plt.imshow(image)
        plt.show()
  except Exception as e:
    logging.error(f"An error occurred while downloading asset: {str(e)}")
    print("ERROR: Unable to download asset.")
    print("ERROR DETAILS:", str(e))
     


def upload(bucketname, bucket, dbConn):
  """
   Inputs local filename and userid; uploads it to the database mapping it to corresponding
   userid. 
  
   Parameters
   ----------
   bucketname: S3 bucket name
   bucket: S3 boto bucket object
   dbConn: open connection to MySQL server

   Returns
   -------
   nothing
  """
  try: 
     
    local_filename = input("Enter the local filename>\n")
    if not os.path.exists(local_filename):
      print(f"Local file {local_filename} does not exist...")
      return

    user_id = input("Enter the user id>\n")

    sql = """
    SELECT bucketfolder 
    FROM users 
    WHERE userid = %s
    """
    row = datatier.retrieve_all_rows(dbConn, sql, [user_id])
    #print(row)
    
    if row is None:
      print("No such user...")
      return
    
    uid= str(uuid.uuid4()) + ".jpg"
    keyname = row[0][0] + '/' + uid

    #print(keyname)

    e= awsutil.upload_file(local_filename, bucket, keyname)
    if (e != -1):
        sql = """
        INSERT into assets (userid,assetname,bucketkey) 
        values( %s, %s, %s)
        """
        upload_result=datatier.perform_action(dbConn, sql,[user_id,local_filename,keyname ])

        if upload_result != -1:
            sql = """
            SELECT LAST_INSERT_ID()
            """
            row = datatier.retrieve_all_rows(dbConn, sql)
            print(f"Uploaded and stored in S3 as ' {keyname} '\nRecorded in RDS under asset id {row[0][0]}")
            #assets(dbConn)
  except Exception as e:
    logging.error(f"An error occurred while uploading asset: {str(e)}")
    print("ERROR: Unable to upload asset.")
    print("ERROR DETAILS:", str(e))

def add_user(dbConn):
  """
   Inputs a new user's email, lastname, firstname, and adds to the database mapping the new user to 
   corresponding bucketfolder
  
   Parameters
   ----------
   dbConn: open connection to MySQL server

   Returns
   -------
   nothing
  """
   
  try:

    email = input("Enter user's email>\n")
    lname = input("Enter user's last (family) name>\n")
    fname = input("Enter user's first (given) name>\n")
    uid = str(uuid.uuid4())

    sql = """INSERT into users (email, lastname, firstname, bucketfolder)
    values(%s,%s,%s,%s)
    """

    insert_result = datatier.perform_action(dbConn, sql,[email,lname,fname, uid ])
    if insert_result != -1:
      sql = """
      select LAST_INSERT_ID()
      """
      row = datatier.retrieve_all_rows(dbConn, sql)
      print(f" Recorded in RDS under user id {row[0][0]}")

  except Exception as e:
    logging.error(f"An error occurred while adding user: {str(e)}")
    print("ERROR: Unable to add user.")
    print("ERROR DETAILS:", str(e))

#########################################################################
# main
#
print('** Welcome to PhotoApp **')
print()

# eliminate traceback so we just get error message:
sys.tracebacklimit = 0

#
# what config file should we use for this session?
#
config_file = 'photoapp-config.ini'

print("What config file to use for this session?")
print("Press ENTER to use default (photoapp-config.ini),")
print("otherwise enter name of config file>")
s = input()

if s == "":  # use default
  pass  # already set
else:
  config_file = s

#
# does config file exist?
#
if not pathlib.Path(config_file).is_file():
  print("**ERROR: config file '", config_file, "' does not exist, exiting")
  sys.exit(0)

#
# gain access to our S3 bucket:
#
s3_profile = 's3readwrite'

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

boto3.setup_default_session(profile_name=s3_profile)

configur = ConfigParser()
configur.read(config_file)
bucketname = configur.get('s3', 'bucket_name')

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucketname)

#
# now let's connect to our RDS MySQL server:
#
endpoint = configur.get('rds', 'endpoint')
portnum = int(configur.get('rds', 'port_number'))
username = configur.get('rds', 'user_name')
pwd = configur.get('rds', 'user_pwd')
dbname = configur.get('rds', 'db_name')

dbConn = datatier.get_dbConn(endpoint, portnum, username, pwd, dbname)

if dbConn is None:
  print('**ERROR: unable to connect to database, exiting')
  sys.exit(0)

#
# main processing loop:
#
cmd = prompt()

while cmd != 0:
  #
  if cmd == 1:
    stats(bucketname, bucket, endpoint, dbConn)
  
  elif cmd == 2:
    users(dbConn)
  elif cmd ==3:
    assets(dbConn)
  elif cmd == 4:
     download(bucketname, bucket, dbConn, display = False)
     #download(dbConn, bucket, bucketname)
        
  elif cmd == 5:
     download(bucketname, bucket, dbConn, display = True)
  elif cmd == 6:
     upload(bucketname, bucket, dbConn)
  elif cmd == 7:
    add_user(dbConn)
  else:
   print("** Unknown command, try again...")
  #
  #
  # TODO
  #
  #

  #
  cmd = prompt()

#
# done
#
print()
print('** done **')
