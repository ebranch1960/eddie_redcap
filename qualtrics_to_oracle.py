# Python 3

import requests
import zipfile
import json
import io, os
import sys
import re
import json
import cx_Oracle
import pandas as pd
import numpy as np

def oracle_import_file(dbuser,dbpass,dbhost,dbport,dbsrvc,filename,processid):

  try:
     ora = cx_Oracle.connect(dbuser, dbpass, cx_Oracle.makedsn(dbhost, dbport, service_name=dbsrvc))
     print("Connected to ", dbsrvc, ora.version)
   
     df = pd.read_csv(filename,usecols=['StartDate', 'EndDate', 'Status', 'IPAddress', 'Progress', 'Duration (in seconds)', 'Finished', 'RecordedDate', 'ResponseId', 'RecipientLastName', 'RecipientFirstName', 'RecipientEmail', 'ExternalReference', 'LocationLatitude', 'LocationLongitude', 'DistributionChannel', 'UserLanguage', 'GPAQ_Q1', 'GPAQ_Q2_1', 'GPAQ_Q3_1', 'GPAQ_Q3_2', 'GPAQ_Q4', 'GPAQ_Q5_1', 'GPAQ_Q6_1', 'GPAQ_Q6_2', 'GPAQ_Q7', 'GPAQ_Q8_1', 'GPAQ_Q9_1', 'GPAQ_Q9_2', 'GPAQ_Q10', 'GPAQ_Q11_1', 'GPAQ_Q12_1', 'GPAQ_Q12_2', 'GPAQ_Q13', 'GPAQ_Q14_1', 'GPAQ_Q15_1', 'GPAQ_Q15_2', 'GPAQ_Q16', 'GPAQ_Q17_1', 'GPAQ_Q18_1', 'GPAQ_Q18_2', 'GPAQ_Q19', 'GPAQ_Q20_1', 'GPAQ_Q21_1', 'GPAQ_Q21_2', 'GPAQ_Q22_1', 'GPAQ_Q22_2', 'BRFSS_Q1', 'BRFSS_Q2', 'BRFSS_Q3', 'BRFSS_Q4', 'BRFSS_Q5', 'BRFSS_Q6', 'BRFSS_Q7', 'BRFSS_Q8', 'BRFSS_Q9', 'BRFSS_Q10', 'BRFSS_Q11', 'SNST_Q1', 'SNST_Q2_1', 'SNST_Q2_2', 'SNST_Q2_3', 'SNST_Q2_4', 'SNST_Q2_5', 'SNST_Q2_6', 'SNST_Q2_7', 'SNST_Q2_8', 'SNST_Q3', 'SNST_Q4', 'SNST_Q5_1', 'SNST_Q5_2', 'SNST_Q5_3', 'SNST_Q6', 'SNST_Q7', 'SNST_Q8', 'SNST_Q9', 'SNST_Q10', 'SNST_Q11', 'PAM_1', 'PAM_2', 'PAM_3', 'PAM_4', 'PAM_5', 'PAM_6', 'PAM_7', 'PAM_8', 'PAM_9', 'PAM_10', 'PAM_11', 'PAM_12', 'PAM_13', 'EHLS_Q1', 'EHLS_Q2', 'EHLS_Q3', 'EHLS_Q4', 'EHLS_Q5', 'EHLS_Q6', 'EHLS_Q7', 'EHLS_Q8', 'ATAQ_Q1_1', 'ATAQ_Q1_2', 'ATAQ_Q1_3', 'ATAQ_Q1_4', 'ATAQ_Q2_1', 'ATAQ_Q2_2', 'ATAQ_Q2_3', 'ATAQ_Q2_4', 'ATAQ_Q3_1', 'ATAQ_Q3_2', 'ATAQ_Q3_3', 'ATAQ_Q3_4', 'ATAQ_Q4_1', 'ATAQ_Q4_2', 'ATAQ_Q4_3', 'ATAQ_Q4_4', 'ATAQ_Q5_1', 'ATAQ_Q5_2', 'ATAQ_Q5_3', 'ATAQ_Q5_4', 'ATAQ_Q6_1', 'ATAQ_Q6_2', 'ATAQ_Q6_3', 'ATAQ_Q6_4', 'ATAQ_Q7_1', 'ATAQ_Q7_2', 'ATAQ_Q7_3', 'ATAQ_Q7_4', 'ATAQ_Q8_1', 'ATAQ_Q8_2', 'ATAQ_Q8_3', 'NEWS_Q1', 'NEWS_Q2', 'NEWS_Q3', 'NEWS_Q4', 'NEWS_Q5', 'NEWS_Q6', 'NEWS_Q7', 'NEWS_Q8', 'NEWS_Q9', 'GHQOL_1', 'GHQOL_2', 'GHQOL_3', 'GHQOL_4', 'GHQOL_5', 'GHQOL_6', 'GHQOL_7', 'GHQOL_8', 'GHQOL_9', 'GHQOL_10', 'SD_1', 'SD_2', 'SD_3', 'SD_4', 'SD_5', 'SD_6', 'SD_7', 'SD_8', 'WELSF_Q1_NPS_GROUP', 'WELSF_Q1', 'WELSF_Q2_NPS_GROUP', 'WELSF_Q2', 'WELSF_Q3_NPS_GROUP', 'WELSF_Q3', 'WELSF_Q4_NPS_GROUP', 'WELSF_Q4', 'WELSF_Q5_NPS_GROUP', 'WELSF_Q5', 'WELSF_Q6_NPS_GROUP', 'WELSF_Q6', 'WELSF_Q7_NPS_GROUP', 'WELSF_Q7', 'WELSF_Q8_NPS_GROUP', 'WELSF_Q8', 'CARDIA_Weekday_1', 'CARDIA_Weekday_2', 'CARDIA_Weekday_3', 'CARDIA_Weekday_4', 'CARDIA_Weekday_5', 'CARDIA_Weekday_6', 'CARDIA_Weekday_7', 'CARDIA_Weekday_8', 'CARDIA_Weekend_1', 'CARDIA_Weekend_2', 'CARDIA_Weekend_3', 'CARDIA_Weekend_4', 'CARDIA_Weekend_5', 'CARDIA_Weekend_6', 'CARDIA_Weekend_7', 'CARDIA_Weekend_8', 'Score', 'ScaledScore']) # read the csv into a datafram
     # print(df.columns.tolist()) #
     df = df.replace(np.nan, '', regex=True) # replace nan's with blanks
     df_list = df.values.tolist() # convert the dataframe to list
     
     list_of_column_names = list(df.columns)
     print('List of column names : ',
          list_of_column_names)
     for index, element in enumerate(list_of_column_names):
          print(index, ":", element)
     for index, element in enumerate(list_of_column_names):
          print(element)
     cursor=ora.cursor()
     #
     # Truncate table before loading
     # 
     cursor.execute("truncate table QUALTRICS_QUESTIONNAIRES")
   
     #
     # Customize the insert with table_name and number of variables
     #
     sql="INSERT INTO QUALTRICS_QUESTIONNAIRES values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55,:56,:57,:58,:59,:60,:61,:62,:63,:64,:65,:66,:67,:68,:69,:70,:71,:72,:73,:74,:75,:76,:77,:78,:79,:80,:81,:82,:83,:84,:85,:86,:87,:88,:89,:90,:91,:92,:93,:94,:95,:96,:97,:98,:99,:100,:101,:102,:103,:104,:105,:106,:107,:108,:109,:110,:111,:112,:113,:114,:115,:116,:117,:118,:119,:120,:121,:122,:123,:124,:125,:126,:127,:128,:129,:130,:131,:132,:133,:134,:135,:136,:137,:138,:139,:140,:141,:142,:143,:144,:145,:146,:147,:148,:149,:150,:151,:152,:153,:154,:155,:156,:157,:158,:159,:160,:161,:162,:163,:164,:165,:166,:167,:168,:169,:170,:171,:172,:173,:174,:175,:176,:177,:178,:179,:180,:181,:182,:183,:184,:185,:186,:187,:188,:189,:190,:191)"
    
     for index,elem in enumerate(df_list[2:],start=2): # iterating the list using index(int)
          print("********************************************************************")
          print(df_list[index]) 
          cursor.execute(sql, df_list[index]) # execute the cursor passing sql and array

     print("{} records inserted.".format(index))

#
# Oracle error trapping
#
  except cx_Oracle.DatabaseError as exc:
     err, = exc.args
     print("Oracle-Error-Code:", err.code)
     print("Oracle-Error-Message:", err.message)

#
# Cleanup
#
  finally:
     cursor.execute("update qualtrics_download_process set status=1,finished=sysdate where process_id=:id",id=processid)
     cursor.execute("update qualtrics_download_process set status=1")
     ora.commit() # commit the changes to the table
     cursor.close()
     ora.close()
     print("Oracle load complete")


def exportSurvey(apiToken, surveyId, dataCenter, fileFormat):

    surveyId = surveyId
    fileFormat = fileFormat
    dataCenter = dataCenter 

    # Setting static parameters
    requestCheckProgress = 0.0
    progressStatus = "inProgress"
    newlineReplacement = " "
    baseUrl = "https://{0}.qualtrics.com/API/v3/surveys/{1}/export-responses/".format(dataCenter, surveyId, newlineReplacement, )
    headers = {
    "content-type": "application/json",
    "x-api-token": apiToken,
    }

    # Step 1: Creating Data Export
    downloadRequestUrl = baseUrl
    downloadRequestPayload = '{"format":"' + fileFormat + '"}'
    downloadRequestResponse = requests.request("POST", downloadRequestUrl, data=downloadRequestPayload, headers=headers)
    progressId = downloadRequestResponse.json()["result"]["progressId"]
    print(downloadRequestResponse.text)

    # Step 2: Checking on Data Export Progress and waiting until export is ready
    while progressStatus != "complete" and progressStatus != "failed":
        print ("progressStatus=", progressStatus)
        requestCheckUrl = baseUrl + progressId
        requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers)
        requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
        print("Download is " + str(requestCheckProgress) + " complete")
        progressStatus = requestCheckResponse.json()["result"]["status"]

    #step 2.1: Check for error
    if progressStatus == "failed":
        raise Exception("export failed")

    fileId = requestCheckResponse.json()["result"]["fileId"]

    # Step 3: Downloading file
    requestDownloadUrl = baseUrl + fileId + '/file'
    requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)

    # Step 4: Unzipping the file
    zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall("MyQualtricsDownload")
    print('exportSurvey complete')


def main():
    
    try:
      apiToken = os.environ['APIKEY']
      dataCenter = os.environ['DATACENTER']
      dbUser = os.environ['ORACLE_USERNAME']
      dbPass = os.environ['ORACLE_PASSWORD']
      dbHost = os.environ['ORACLE_HOST']
      dbPort = os.environ['ORACLE_PORT']
      dbSrvc = os.environ['ORACLE_SERVICE']
    except KeyError:
      print("set environment variables")
      sys.exit(2)

    try:
        surveyId=sys.argv[1]
        processid=sys.argv[2]
        fileFormat=sys.argv[3]
    except IndexError:
        print ("usage: surveyId fileFormat")
        sys.exit(2)

    if fileFormat not in ["csv", "tsv", "spss", "json"]:
        print ('fileFormat must be either csv, tsv, or spss')
        sys.exit(2)
 
    r = re.compile('^SV_.*')
    m = r.match(surveyId)
    if not m:
       print ("survey Id must match ^SV_.*")
       sys.exit(2)

    print ("Processing request ", surveyId, processid)
    exportSurvey(apiToken, surveyId,dataCenter, fileFormat)
    oracle_import_file(dbUser,dbPass,dbHost,dbPort,dbSrvc,"MyQualtricsDownload/CHAMPS_Questionnaires.csv",processid)

if __name__== "__main__":
    main()
