
from flask import Flask, render_template, request, redirect, url_for
import os
from os.path import join, dirname, realpath

import pandas as pd
import numpy as np
import math
import mysql.connector

app = Flask(__name__)

# enable debugging mode
app.config["DEBUG"] = True

# Upload folder
UPLOAD_FOLDER = 'C:\\Users\\Nicolas\\Documents\\GitHub\\restapi-PoC'
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER


# Database
mydb = mysql.connector.connect(
  host="host.docker.internal",
  user="test",
  password="asfTTsCVGs&%$23a",
  database="testdb"
)

mycursor = mydb.cursor()

mycursor.execute("SHOW DATABASES")

# View All Database
for x in mycursor:
  print(x)



# Root URL
@app.route('/')
def index():
     # Set The upload HTML template '\templates\index.html'
    return render_template('upload.html')


# Get the uploaded files
@app.route("/", methods=['POST'])
def uploadFiles():
      # get the uploaded file
      uploaded_file = request.files['file']
      table_id = request.form.get('tablename')
      if uploaded_file.filename != '':
           file_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename).replace("/","\\")
          # set the file path
           uploaded_file.save(file_path)
           parseCSV(file_path,table_id)
          # save the file
      return redirect(url_for('index'))

def parseCSV(filePath,table_id):
  
    # Use Pandas to parse the CSV file
    csvData = pd.read_csv(filePath, header=None)
    # Loop through the Rows
    for i,row in csvData.iterrows():
      if pd.isna(row.values).any(): #check if any null exists in row
        continue  
      if table_id == "hired_employees":
        sql = "INSERT INTO hired_employees (id,name,datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s)"
        value = (int(row[0]),str(row[1]),str(row[2]),int(row[3]),int(row[4]))
      elif table_id == "departments":
        sql = "INSERT INTO departments (id,department) VALUES (%s, %s)"
        value = (int(row[0]),str(row[1]))
      elif table_id == "jobs":
        sql = "INSERT INTO jobs (id,job) VALUES (%s, %s)"
        value = (int(row[0]),str(row[1]))
      mycursor.execute(sql, value)
      mydb.commit()
  
if (__name__ == "__main__"):
     app.run(port = 5002)

