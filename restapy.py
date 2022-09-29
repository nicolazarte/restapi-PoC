from flask import Flask, render_template, request, redirect, url_for
import os
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)

# enable debugging mode
app.config["DEBUG"] = True

# Upload folder
UPLOAD_FOLDER = os.getcwd() + "\\uploads"
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER

# Database

mydb2 = create_engine("mysql+pymysql://test:asfTTsCVGs&%$23a@host.docker.internal:3306/testdb")

# Root URL
@app.route('/')
def index():
     # Set The upload HTML template '\templates\upload.html'
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

@app.route('/test_api', methods = ['POST'])
def test_api():
  #try:
    #get the tables json data and check for existing ids
    data = request.get_json()
    rows=0
    if 'departments' in data.keys():
      dfdep = pd.DataFrame(data.get('departments'))
      rows += len(dfdep.index)
    if 'jobs' in data.keys():
      dfjobs = pd.DataFrame(data.get('jobs'))
      rows += len(dfjobs.index)
    if 'hired_employees' in data.keys():
      dfemp = pd.DataFrame(data.get('hired_employees'))
      rows += len(dfemp.index)
    if rows <= 1000:
      if 'departments' in data.keys():
        depids = dfdep['id'].to_list()
        depidsstr = "' or id = '".join([str(x) for x in depids]) 
        dfdepkey = pd.read_sql_query('SELECT id FROM departments where id = \'' +  depidsstr + '\';',mydb2)
        dfdepuniq = dfdep[~dfdep.id.isin(dfdepkey['id'].to_list())]
        if len(dfdepuniq) > 0:
          dfdepuniq.to_sql ('departments',mydb2, if_exists='append',index=False)
      if 'jobs' in data.keys():
        jobids = dfjobs['id'].to_list()
        jobsidsstr = "' or id = '".join([str(x) for x in jobids]) 
        dfjobkey = pd.read_sql_query('SELECT id FROM jobs where id = \'' + jobsidsstr + '\';',mydb2)
        dfjobuniq = dfjobs[~dfjobs.id.isin(dfjobkey['id'].to_list())]
        if len(dfjobuniq) > 0:
          dfjobuniq.to_sql ('jobs',mydb2, if_exists='append',index=False)
      if 'hired_employees' in data.keys():
        empids = dfemp['id'].to_list()
        empidsstr = "' or id = '".join([str(x) for x in empids]) 
        dfempkey = pd.read_sql_query('SELECT id FROM hired_employees where id = \'' + empidsstr + '\';',mydb2)
        dfempuniq = dfemp[~dfemp.id.isin(dfempkey['id'].to_list())]
        if len(dfempuniq) > 0:
          dfempuniq.to_sql ('hired_employees',mydb2, if_exists='append',index=False)
    print(dfdep)
    print(dfjobs)
    print(dfemp)
    print("rows: " + str(rows)) 
    return 'success'
  #except:
    #print("Error")

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
      mydb2.execute(sql, value)



if (__name__ == "__main__"):
     app.run(port = 5002, debug = True)

