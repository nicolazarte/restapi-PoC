from flask import Flask, render_template, request, redirect, url_for, flash
import os
import pandas as pd
import pandavro as pdx
from sqlalchemy import create_engine
from datetime import datetime  
import time 

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

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
           message = parseCSV(file_path,table_id)
           flash(message)
          # save the file
      return redirect(url_for('index'))

@app.route('/submit_json', methods = ['POST'])
def submit_json():
  #try:
    #get the tables json data and check for existing ids
    data = request.get_json()
    rows=0
    if 'departments' in data.keys():
      dfdep = pd.DataFrame(data.get('departments'))
      column_names = list(dfdep.columns)
      if column_names == ['id','department']:
        rows += len(dfdep.index)
      else:
        dfdep = None
    if 'jobs' in data.keys():
      dfjobs = pd.DataFrame(data.get('jobs'))
      column_names = list(dfjobs.columns)
      if column_names == ['id','job']:
        rows += len(dfjobs.index)
      else:
        dfjobs = None
      rows += len(dfjobs.index)
    if 'hired_employees' in data.keys():
      dfemp = pd.DataFrame(data.get('hired_employees'))
      column_names = list(dfemp.columns)
      if column_names == ['id','name','datetime','department_id','job_id']:
        rows += len(dfemp.index)
      else:
        dfemp = None
      
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
    flash("Successfully inserted " +str(rows) + " rows.")
    return 'success'
  #except:
    #print("Error")

def parseCSV(filePath,table_id):
    # Use Pandas to parse the CSV file
    csvData = pd.read_csv(filePath, header=None)
    #Open file for logging errors
    if table_id == "hired_employees":
      mydb2.execute('DROP TABLE `hired_employees`;')
      df = pd.DataFrame({'id': pd.Series(dtype='int'),
                   'name': pd.Series(dtype='str'),
                   'datetime': pd.Series(dtype='str'),
                   'department_id': pd.Series(dtype='int'),
                   'job_id': pd.Series(dtype='int')})
      df.to_sql('hired_employees',mydb2,if_exists='replace', index=False)
      mydb2.execute('ALTER TABLE `hired_employees` ADD PRIMARY KEY (`id`);')
    elif table_id == "departments":
      mydb2.execute('DROP TABLE `departments`;')
      df = pd.DataFrame({'id': pd.Series(dtype='int'),
                   'department': pd.Series(dtype='str')})
      df.to_sql('departments',mydb2,if_exists='replace', index=False)
      mydb2.execute('ALTER TABLE `departments` ADD PRIMARY KEY (`id`);')
    elif table_id == "jobs":
      mydb2.execute('DROP TABLE `jobs`;')
      df = pd.DataFrame({'id': pd.Series(dtype='int'),
                   'job': pd.Series(dtype='str')})
      df.to_sql('jobs',mydb2,if_exists='replace', index=False)
      mydb2.execute('ALTER TABLE `jobs` ADD PRIMARY KEY (`id`);')
    with open('error_log.txt', 'a') as f:
      # Loop through the Rows
      for i,row in csvData.iterrows():
        if table_id == "hired_employees":
          #Check null values
          if ~pd.isna(row.values).any():
            #Check datetime in iso format
            try:
              iso_format = "%Y-%m-%dT%H:%M:%SZ"
              iso_date = str(row[2])
              datetime.strptime(iso_date, iso_format)
            except ValueError:
              time_stamp = time.time()
              log = "[Time: "+ str(datetime.fromtimestamp(time_stamp)) + ", file:" + filePath +", table_id: " + table_id +", row: " + str(i+1) + "]: Row with invalid date format not inserted into db."
              f.writelines(log)
              flash(log)
              continue  
            sql = "INSERT INTO hired_employees (id,name,datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s)"
            value = (int(row[0]),str(row[1]),str(row[2]),int(row[3]),int(row[4]))
            mydb2.execute(sql, value)
          else:
            time_stamp = time.time()
            log = "[Time: "+ str(datetime.fromtimestamp(time_stamp)) + ", file:" + filePath +", table_id: " + table_id +", row: " + str(i+1) + "]: Row with null value not inserted into db."
            f.writelines(log)
            flash(log)
        elif table_id == "departments":
          if ~pd.isna(row.values).any():
            sql = "INSERT INTO departments (id,department) VALUES (%s, %s)"
            value = (int(row[0]),str(row[1]))
            mydb2.execute(sql, value)
          else:
            time_stamp = time.time()
            log = "[Time: "+ str(datetime.fromtimestamp(time_stamp)) + ", file:" + filePath +", table_id: " + table_id +", row: " + str(i+1) + "]: Row with null value not inserted into db."
            f.writelines(log)
            flash(log)
        elif table_id == "jobs":
          if ~pd.isna(row.values).any():
            sql = "INSERT INTO jobs (id,job) VALUES (%s, %s)"
            value = (int(row[0]),str(row[1]))
            mydb2.execute(sql, value)
          else:
            time_stamp = time.time()
            log = "[Time: "+ str(datetime.fromtimestamp(time_stamp)) + ", file:" + filePath +", table_id: " + table_id +", row: " + str(i+1) + "]: Row with null value not inserted into db."
            f.writelines(log)
            flash(log)
    return "Succesfully performed the operation"

@app.route('/backups')
def backups():
     # Set The backups HTML template '\templates\backups.html'
    return render_template('backups.html')

# Perform backups and restoration
@app.route("/backups", methods=['POST'])
def manage_backups():
      # get the uploaded file
      optionbck = request.form['operation']
      if optionbck == 'backup':
        print('backup')
        dfemp = pd.read_sql_table('hired_employees', mydb2)
        pdx.to_avro("\\backups\\hired_employees.avro", dfemp)
        dfjob = pd.read_sql_table('jobs', mydb2)
        pdx.to_avro("\\backups\\jobs.avro", dfjob)
        dfdep = pd.read_sql_table('departments', mydb2)
        pdx.to_avro("\\backups\\departments.avro", dfdep)
        flash("Backup performed succesfully")
      if optionbck == 'restore':
        print('restore')
        dfdep = pdx.read_avro("\\backups\\departments.avro")
        dfdep.to_sql('departments',mydb2, if_exists='replace',index=False)
        dfjob = pdx.read_avro("\\backups\\jobs.avro")
        dfjob.to_sql('jobs',mydb2, if_exists='replace',index=False)
        dfemp = pdx.read_avro("\\backups\\hired_employees.avro")
        dfemp.to_sql ('hired_employees',mydb2, if_exists='replace',index=False)
        flash("Backup succesfully restored")
      return redirect(url_for('backups'))


if (__name__ == "__main__"):
     app.run(port = 5002, debug = True)

