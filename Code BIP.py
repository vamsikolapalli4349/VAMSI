import base64, re, json, requests, xml.dom.minidom, numpy as np, pandas as pd, xml.etree.ElementTree as ET, boto3, time, lxml
from datetime import date
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pytz import timezone
from bs4 import BeautifulSoup
import Redshift_Target
import BIP_Main
import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark import *
from pyspark.sql.functions import lit 

import os
import configparser
from pyspark.sql import *
from pyspark import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import from_unixtime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
print('at start')

###### boto3 services definitions ######
ssm = boto3.client('ssm')
param_region = ssm.get_parameter(Name='/veritiv/aws/region', WithDecryption=True)['Parameter']['Value']
sts = boto3.client("sts", region_name=param_region)
s3 = boto3.resource('s3')
s3_client = boto3.client('s3', region_name=param_region)
sns_client = boto3.client('sns', region_name=param_region)
ddbconn = boto3.client('dynamodb', region_name=param_region)
ddb = boto3.resource('dynamodb', region_name=param_region)
##################################################################################

#### values for account ids in different aws instances #####
devaccountid = ssm.get_parameter(Name='/veritiv/aws/devaccountid', WithDecryption=True)['Parameter']['Value']
testaccountid = ssm.get_parameter(Name='/veritiv/aws/testaccountid', WithDecryption=True)['Parameter']['Value']
prodaccountid = ssm.get_parameter(Name='/veritiv/aws/prodaccountid', WithDecryption=True)['Parameter']['Value']
sharedservicesaccountid = \
ssm.get_parameter(Name='/veritiv/aws/sharedservicesaccountid', WithDecryption=True)['Parameter']['Value']
###################################################################################################################################

#### oracle fusion (BIP) parameter store values used in lambdas currently  ####
Security_Service_WSDL = ssm.get_parameter(Name='/BIP/WSDL/prod/SecurityService', WithDecryption=True)['Parameter'][
    'Value']
Report_Service_WSDL = ssm.get_parameter(Name='/BIP/WSDL/prod/ReportService', WithDecryption=True)['Parameter']['Value']
WSDL_List = ssm.get_parameter(Name='/BIP/WSDL/prod/List', WithDecryption=True)['Parameter']['Value']
folderPath = ssm.get_parameter(Name='/BIP/WSDL/prod/BIPDynamicExtractFolder', WithDecryption=True)['Parameter']['Value']
### prod ####
prodInstanceURL = ssm.get_parameter(Name='/BIP/WSDL/prod/instanceURL', WithDecryption=True)['Parameter']['Value']
prodFusionUserName = ssm.get_parameter(Name='/BIP/WSDL/prod/userid', WithDecryption=True)['Parameter']['Value']
prodFusionPassword = ssm.get_parameter(Name='/BIP/WSDL/prod/password', WithDecryption=True)['Parameter']['Value']
### test ####
testInstanceURL = ssm.get_parameter(Name='/BIP/WSDL/test/instanceURL', WithDecryption=True)['Parameter']['Value']
testFusionUserName = ssm.get_parameter(Name='/BIP/WSDL/test/userid', WithDecryption=True)['Parameter']['Value']
testFusionPassword = ssm.get_parameter(Name='/BIP/WSDL/test/password', WithDecryption=True)['Parameter']['Value']
############
### dev1 ###
dev1InstanceURL = ssm.get_parameter(Name='/BIP/WSDL/dev1/instanceURL', WithDecryption=True)['Parameter']['Value']
dev1FusionUserName = ssm.get_parameter(Name='/BIP/WSDL/dev1/userid', WithDecryption=True)['Parameter']['Value']
dev1FusionPassword = ssm.get_parameter(Name='/BIP/WSDL/dev1/password', WithDecryption=True)['Parameter']['Value']
############
### dev2 ###
dev2InstanceURL = ssm.get_parameter(Name='/BIP/WSDL/dev2/instanceURL', WithDecryption=True)['Parameter']['Value']
dev2FusionUserName = ssm.get_parameter(Name='/BIP/WSDL/dev2/userid', WithDecryption=True)['Parameter']['Value']
dev2FusionPassword = ssm.get_parameter(Name='/BIP/WSDL/dev2/password', WithDecryption=True)['Parameter']['Value']
############
### dev3 ###
dev3InstanceURL = ssm.get_parameter(Name='/BIP/WSDL/dev3/instanceURL', WithDecryption=True)['Parameter']['Value']
dev3FusionUserName = ssm.get_parameter(Name='/BIP/WSDL/dev3/userid', WithDecryption=True)['Parameter']['Value']
dev3FusionPassword = ssm.get_parameter(Name='/BIP/WSDL/dev3/password', WithDecryption=True)['Parameter']['Value']
############
### dev4 ###
dev4InstanceURL = ssm.get_parameter(Name='/BIP/WSDL/dev4/instanceURL', WithDecryption=True)['Parameter']['Value']
dev4FusionUserName = ssm.get_parameter(Name='/BIP/WSDL/dev4/userid', WithDecryption=True)['Parameter']['Value']
dev4FusionPassword = ssm.get_parameter(Name='/BIP/WSDL/dev4/password', WithDecryption=True)['Parameter']['Value']
############
### dev5 ###
dev5InstanceURL = ssm.get_parameter(Name='/BIP/WSDL/dev5/instanceURL', WithDecryption=True)['Parameter']['Value']
dev5FusionUserName = ssm.get_parameter(Name='/BIP/WSDL/dev5/userid', WithDecryption=True)['Parameter']['Value']
dev5FusionPassword = ssm.get_parameter(Name='/BIP/WSDL/dev5/password', WithDecryption=True)['Parameter']['Value']
############
### dev6 ###
dev6InstanceURL = ssm.get_parameter(Name='/BIP/WSDL/dev6/instanceURL', WithDecryption=True)['Parameter']['Value']
dev6FusionUserName = ssm.get_parameter(Name='/BIP/WSDL/dev6/userid', WithDecryption=True)['Parameter']['Value']
dev6FusionPassword = ssm.get_parameter(Name='/BIP/WSDL/dev6/password', WithDecryption=True)['Parameter']['Value']
############
### dev7 ###
dev7InstanceURL = ssm.get_parameter(Name='/BIP/WSDL/dev7/instanceURL', WithDecryption=True)['Parameter']['Value']
dev7FusionUserName = ssm.get_parameter(Name='/BIP/WSDL/dev7/userid', WithDecryption=True)['Parameter']['Value']
dev7FusionPassword = ssm.get_parameter(Name='/BIP/WSDL/dev7/password', WithDecryption=True)['Parameter']['Value']
######################################################################################################################
##### SNS standard load email Topic ##### 
snsload = ssm.get_parameter(Name='/veritiv/aws/sns/standload', WithDecryption=True)['Parameter']['Value']

evoCurrentRelease = ssm.get_parameter(Name='/BIP/EVO/CurrentRelease', WithDecryption=True)['Parameter']['Value']
evoPriorRelease = ssm.get_parameter(Name='/BIP/EVO/PriorRelease', WithDecryption=True)['Parameter']['Value']

#############################  get instance / bucket definitions ########
extra_args = {'ACL': 'bucket-owner-full-control'}

account_id = sts.get_caller_identity()["Account"]
print(account_id)

if account_id == prodaccountid:
    # '556567008402':
    instance = 'vprod'
elif account_id == devaccountid:
    # '107479248385':
    instance = 'vdev'
elif account_id == testaccountid:
    # '226074724429':
    instance = 'vtst'
# shared services id
elif account_id == sharedservicesaccountid:
    # '353328443169':
    instance = 'vdev'
else:
    instance = 'vdev'

print('Instance: ', instance)

rawbucket = instance + '-raw'
bucket = s3.Bucket(rawbucket)  # Already in Parameter Store


ddb_tablename = 'EVO_BaseTables_Data_Controller'

ddb_stats_table = 'EVO_BaseTables_Load_Stats'

messageSNS = ''

###### dates and times are using UTC to match Oracle Fusion BI Publisher dates and times  ########
##################################################################################################
# est = timezone('US/Eastern')
# now = datetime.now(est)
now = datetime.now()
filedatetime = now.strftime("%Y%m%d%H%M")
today = now.strftime("%Y-%m-%d")
mm = now.strftime("%m")
yy = now.strftime("%Y")
dd = now.strftime("%d")

print(today)


try:
    args = getResolvedOptions(sys.argv,
                               ['JOB_NAME',
                               'priority',
                               'EVOInstance'])
                               
    job.init(args['JOB_NAME'], args)
    priority = args['priority']
    EVOInstance = args['EVOInstance']
except Exception as e:
    print("No arguments passed to the Glue Job so we'll use the defaults")
    priority = 'All'
    EVOInstance = 'All'


start_date_days = 1

############ create dynamodb stats table if not already present ###########################
try:
    response2 = ddbconn.describe_table(TableName=ddb_stats_table)
except Exception as e:
    ddbconn.create_table(
        TableName=ddb_stats_table,
        KeySchema=[{'AttributeName': 'filename', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'filename', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1})
    print('Stats Table created')
    time.sleep(5)
###########################################################################################

print('priority: ',priority)
print('EVOInstance: ', EVOInstance)


############################################################################################################################################################################
########################################## get total record counts through yesterday's date Function #######################################################################
############################################################################################################################################################################
def recordCnts(tbl, loadType):
    import base64

    sourceRowCount = 0
    recordcount = 0
    minLUD = ''
    maxLUD = ''
    soup = ''
    reportBytes = ''
    inputSQLtxt = ''

    ###############################################################################################################################
    recordcount = 0

    print("Extract total record counts through yesterday from EVO SRC for Base Table: " + tbl)

    if row['LUD_InTable_Flag'] == 'false':
        if loadType == 'MetaData':
            inputSQLtxt = "SELECT count(1) as cnt, null AS MIN_LUD, null AS MAX_LUD FROM " + tbl + " WHERE OWNER = ''FUSION''"
        else:
            inputSQLtxt = "SELECT count(1) as cnt, null AS MIN_LUD, null AS MAX_LUD FROM " + tbl
    else:
        inputSQLtxt = "SELECT count(1) as cnt, MIN(LAST_UPDATE_DATE) AS MIN_LUD, MAX(LAST_UPDATE_DATE) AS MAX_LUD FROM " + tbl
        if loadType == 'CDC':
            inputSQLtxt += " WHERE ( TRUNC(LAST_UPDATE_DATE) <= TO_DATE(CURRENT_DATE - " + str(start_date_days) +") OR TRUNC(CREATION_DATE) <= TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") )"
    print(inputSQLtxt)

    folderPath = '/Custom/CloudTools/V4'
    baseName = "FSTReportCSV"
    dm_baseName = "FusionSQLToolDM"
    reportName = baseName + ".xdo"
    reportPath = folderPath + "/" + reportName
    xdmName = dm_baseName
    dmPath = folderPath + "/" + xdmName + '.xdm'

    # create DM
    result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
    result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
    
    # run report to return csv file
    result = BIP_Main.CSVrunReport(instanceURL, folderPath, reportPath,fusionUserName, fusionPassword)
    print(result)

    # soup = BeautifulSoup(result3.text.encode('utf8'), 'xml')
    # reportBytes = soup.find('reportBytes').string
    # result = base64.b64decode(reportBytes).decode('UTF-8')
    status = result[0]
    if status != 'Success':
        errormsg = result[1]
        print(errormsg)
        
    else:
        errormsg = ''
   
    return(result[1])

############################################################################################################################################################################
########################################## END of get total record counts through yesterday's date #########################################################################
############################################################################################################################################################################

############################################################################################################################################################################
################################################# Main Processing Loop #####################################################################################################
############################################################################################################################################################################

table = ddb.Table(ddb_tablename)
response = table.scan()
data = response['Items']
listdf = pd.DataFrame(data)

fileprefix = 'EVO'+ '/'+ 'TblSQL'+ '/'

listdf = listdf.loc[listdf['Frequency'].isin(['Daily'])]
listdf = listdf.loc[listdf['ActiveDownloadFlag'].isin(['true'])]
listdf = listdf.loc[listdf['TableCreatedFlag'].isin(['true'])]
listdf = listdf.loc[listdf['DailyLoadType'].isin(['CDC', 'Full','Validation','Special_Full','Special_CDC','MetaData'])]
listdf = listdf.loc[listdf['Schedule'].isin(['false'])]
if priority != 'All':
    listdf = listdf.loc[listdf['priority'].isin([int(priority)])]
if EVOInstance != 'All':
    listdf = listdf.loc[listdf['EVOInstance'].isin([EVOInstance])]
listdf = listdf.sort_values(['priority','sortOrder'])

redshift_error = 'false'
redshift_error_message = ''     

dmPath = folderPath + "/FusionSQLToolDM.xdm"
reportPath = folderPath + "/FSTreport.xdo"
tablenamelist = listdf['TableName1'].tolist()
for index, row in listdf.iterrows():
    ######  determine which EVO Instance to create and run the reports based upon entry in dynamodb item ###
    ###### In prod this should always be prod more than likely ##########
    if row['EVOInstance'] == 'prod':
        instanceURL = prodInstanceURL
        fusionUserName = prodFusionUserName
        fusionPassword = prodFusionPassword
    if row['EVOInstance'] == 'test':
        instanceURL = testInstanceURL
        fusionUserName = testFusionUserName
        fusionPassword = testFusionPassword
    elif row['EVOInstance'] == 'dev1':
        instanceURL    = dev1InstanceURL
        fusionUserName = dev1FusionUserName
        fusionPassword = dev1FusionPassword    
    elif row['EVOInstance'] == 'dev2':
        instanceURL    = dev2InstanceURL
        fusionUserName = dev2FusionUserName
        fusionPassword = dev2FusionPassword    
    elif row['EVOInstance'] == 'dev3':
        instanceURL    = dev3InstanceURL
        fusionUserName = dev3FusionUserName
        fusionPassword = dev3FusionPassword    
    elif row['EVOInstance'] == 'dev4':
        instanceURL    = dev4InstanceURL
        fusionUserName = dev4FusionUserName
        fusionPassword = dev4FusionPassword
    elif row['EVOInstance'] == 'dev5':
        instanceURL    = dev5InstanceURL
        fusionUserName = dev5FusionUserName
        fusionPassword = dev5FusionPassword
    elif row['EVOInstance'] == 'dev6':
        instanceURL    = dev6InstanceURL
        fusionUserName = dev6FusionUserName
        fusionPassword = dev6FusionPassword
    elif row['EVOInstance'] == 'dev7':
        instanceURL    = dev7InstanceURL
        fusionUserName = dev7FusionUserName
        fusionPassword = dev7FusionPassword


    # print(instanceURL)
    # print(testInstanceURL)
    # print(fusionUserName)
    # print(testFusionUserName)
    # print(fusionPassword)
    # print(testFusionPassword)


    url = instanceURL + "/xmlpserver/services/v2/CatalogService"
    
    SchemaName = row['SchemaName']
    loadType = row['DailyLoadType']
    upsertkey = row['primarykey']
    tbl = row['TableName1']
    tablename = tbl
    redshift_tablename = tbl
    if loadType == 'Validation':
        redshift_tablename = row['ValidationRedshiftTableName']
    elif loadType == 'MetaData':
        redshift_tablename = tbl + '_' + EVOInstance + '_' + evoCurrentRelease
        emptyDF = pd.DataFrame()
        tableExistsFlag = Redshift_Target.tbl_Exists(emptyDF,SchemaName, redshift_tablename, 'false')
        if tableExistsFlag[0] != 'True':
            createFlag = Redshift_Target.tbl_CreateTbl_MetaDataReleaseTable(SchemaName, redshift_tablename)
            if createFlag == 'True':
                print('Successful creation of Redshift metadata release table: ', redshift_tablename)
            else:
                print('Faled to create Redshift metadata release table: ', redshift_tablename)    
            
        
    tableUID = row['TableName']
    
    returnfields = row['returnFields']
    
    fullToCDCFlag = row['FullToCDCFlag']
    
    errormsg = ''
    sourceRowCount = 0
    redshift_row_count = 0
    redshift_inserts = 0
    redshift_updates = 0
    redshift_error = 'false'
    redshift_error_message = ''
    newloadType = loadType
    redshiftbeginrowscount = 0
    
    recLastUpdateDate = row['LastUpdateDate'][0:10]
    #print(recLastUpdateDate)
    
    if recLastUpdateDate != today:

        if row['BatchesFlag'] != 'true' or loadType == 'CDC' or loadType == 'Validation' or loadType == 'MetaData' or loadType == 'Special_Full' or loadType == 'Special_CDC':
            print('In main processing loop - non batches')
            print('Instance URL: ',instanceURL)
            # getRowCountEVOSource(row)
            print('tablename: ', tbl)

            ### get total record counts from tablename in EVO ########
            recordCounts = recordCnts(tbl, loadType)
            print('RecordCounts: ',recordCounts)
            
            with open('/tmp/counts.csv', 'w', encoding="utf-8") as output_file:
                output_file.write(recordCounts)
            counts_df = pd.read_csv('/tmp/counts.csv',dtype=str)
            
            counts_df = counts_df.replace(np.nan, '', regex=True)
            counts_df = counts_df.replace('nan', '', regex=True)
            counts_df = counts_df.replace('null', '', regex=True)
            
            recordcount = 0
            minLUD = ''
            maxLUD = ''
            try:
                for index, row in counts_df.iterrows():
                    recordcount = row['CNT']
                    minLUD = row['MIN_LUD']
                    maxLUD = row['MAX_LUD']
            except Exception as e:
                recordcount = 0
                minLUD = ''
                maxLUD = ''
            print(recordcount)
            print(minLUD)
            print(maxLUD)


            inputSQLtxt = ""
            # BIP_Main.createDataModel(folderPath, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            print("Extract data from EVO SRC for Base Table: " + tbl)
            inputSQLtxt = "SELECT * FROM " + tbl
            if loadType == 'CDC':
                inputSQLtxt = inputSQLtxt + " WHERE ( TRUNC(LAST_UPDATE_DATE) = TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") OR TRUNC(CREATION_DATE) = TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") )"
            elif loadType == 'Validation' or loadType == 'Special_Full' or loadType == 'Special_CDC':
                inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl
                if loadType == 'Special_CDC':
                    inputSQLtxt = inputSQLtxt + " WHERE ( TRUNC(LAST_UPDATE_DATE) = TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") OR TRUNC(CREATION_DATE) = TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") )"
            elif loadType == 'MetaData':
                #inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl + " WHERE OWNER = ''FUSION''" 
                inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl    
                   
            print(inputSQLtxt)
 
            # if row['EVOInstance'] == 'prod' and instance != 'vprod':
            #     baseName = "FSTreport_" + tbl + '_' + instance
            # else:
 
            # baseName = "FSTreport_" + tbl
            # if loadType == 'Validation':
            #     baseName = baseName + '_Validation'
            # reportName = baseName + ".xdo"
            # reportPath = folderPath + "/" + reportName
            # xdmName = baseName
            # dmPath = folderPath + "/" + xdmName + '.xdm'
            
            # folderPath = '/Custom/CloudTools/V4'
            baseName = "FSTReportCSV"
            dm_baseName = "FusionSQLToolDM"
            reportName = baseName + ".xdo"
            reportPath = folderPath + "/" + reportName
            xdmName = dm_baseName
            dmPath = folderPath + "/" + xdmName + '.xdm'
    
            
            #####  Start of BIP Report Creation and Data Extraction ########
    
            
            now = datetime.now()
            #filedatetime = now.strftime("%Y%m%d")
            filedatetime = now.strftime("%Y%m%d%H%M%S")
            bipstartdatetime = now.strftime("%Y-%m-%d %H:%M:%S")
            
            #xml_file = "/tmp/" + tbl + "_" + filedatetime + ".xml"
            csv_file = "/tmp/" + tbl + ".csv"
 
            if loadType == 'Full' or loadType == 'Special_Full':
                prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
            elif loadType == 'CDC' or loadType == 'Special_CDC':
                prefix = "EVO/TblData/CDC/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"

            # if loadType == 'Full':
            #     prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
            # elif loadType == 'CDC':
            #     prefix = "EVO/TblData/CDC/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
            elif loadType == 'Validation':
                prefix = "EVO/TblData/Validation/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
            elif loadType == 'MetaData':
                prefix = "EVO/TblData/MetaData/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
            FName = tbl + "_" + str(filedatetime) + ".csv"  
            filename = prefix + FName  
            
            
            bucket_zone = rawbucket + '/' + prefix
            
            ############################################################################################
            
            ###### create item in stats table ######
        #     stats_item = {
        #         'filename': {'S':FName},
        #         'project': {'S': 'EVO'},
        #         'feed': {'S':tablename},
        #         'year': {'S': yy},
        #         'month': {'S': mm},
        #         'day': {'S': dd},
        #         'evo_rowcount': {'S': str(recordcount)},
        #         'redshift_rowcount': {'S': 'null'},
        #         'redshift_processed_records': {'S': 'null'},
        #         'redshift_inserted_records': {'S': 'null'},
        #         'redshift_updated_records': {'S': 'null'},
        #         'bip_start_time': {'S': bipstartdatetime},
        #         'bip_end_time': {'S':''},
        #         'redshift_start_time': {'S':''},
        #         'redshift_end_time': {'S':''},
        #         'bip_status': {'S': 'In Progress'},
		      #  'redshift_status': {'S': 'null'},
        #         'bucket_zone': {'S': bucket_zone},
        #         'bip_fail_description': {'S': 'None'},
        #         'schemaname': {'S': SchemaName},
        #         'tablename': {'S':tableUID},
        #         'redshift_tablename': {'S':redshift_tablename},
        #         'bip_fail_description': {'S': 'None'},
        #         'redshift_fail_description': {'S': 'None'}} 
            
        #     try:
        #         response = ddbconn.get_item(
        #             TableName=ddb_stats_table,
        #             Key={'filename': {'S':FName}})
        #         if 'Item' in response:
        #             item = response['Item']
        #         else:
        #             ddbconn.put_item(
        #             TableName=ddb_stats_table,
        #             Item=item)
        #     except Exception as e:
        #         ddbconn.put_item(
        #             TableName=ddb_stats_table,
        #             Item=stats_item)
            
            # result = BIP_Main.createDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            # #print(result)
            # # #
            # result = BIP_Main.createDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            # #print(result)
            # # # # # #
            # result1 = BIP_Main.createReport(instanceURL, folderPath, dmPath, reportName, fusionUserName, fusionPassword)
            # #print(result1)
    
            # result = BIP_Main.runReport(instanceURL, folderPath, reportPath, fusionUserName, fusionPassword)
            # # print('results from query running: ', result)
            # ### should check newoutput for failures from run report ####
            # status = result[0]
            # if status != 'Success':
            #     errormsg = result[1]
            #     print(errormsg)
                
            # else:
            #     errormsg = ''
            # print('Status: ', status)
            # newoutput = result[1]
            
            # print(len(newoutput))
            # if len(newoutput) < 101:
            #     zeroRowsReturned = 'true'
            # else:
            #     zeroRowsReturned = 'false'
            
            # print('No Rows Returned Flag: ', zeroRowsReturned)
    
            # try:
            #     BIP_Main.deleteReport(instanceURL, reportPath, fusionUserName, fusionPassword)
            #     BIP_Main.deleteReport(instanceURL, dmPath, fusionUserName, fusionPassword)
            # except Exception as e:
            #     print('no reports or data model to delete so do nothing')
    
            
             # create DM
            result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            
            # run report to return csv file
            result = BIP_Main.CSVrunReport(instanceURL, folderPath, reportPath,fusionUserName, fusionPassword)

            status = result[0]
            if status != 'Success':
                errormsg = result[1]
                print(errormsg)
                
            else:
                errormsg = ''
            print('Status: ', status)
            newoutput = result[1]
            
            print(len(newoutput))
            if len(newoutput) < 10:
                zeroRowsReturned = 'true'
            else:
                zeroRowsReturned = 'false'
                with open(csv_file, 'w', encoding="utf-8") as output_file:
                    output_file.write(newoutput)
                df = pd.read_csv(csv_file,dtype=str)
                #print(len(df))
            print('zero rows reurned flag: ', zeroRowsReturned)
            
            if status == 'Success':
                
                now = datetime.now()
                bipenddatetime = now.strftime("%Y-%m-%d %H:%M:%S")    

                # try:
                #     ddbconn.update_item(
                #         TableName=ddb_stats_table,
                #         Key={"filename": {"S": FName}},
                #         AttributeUpdates={"bip_end_time": {"Value": {"S": bipenddatetime}},
                #                           "redshift_start_time": {"Value": {"S": bipenddatetime}},
                #                           "month": {"Value": {"S": mm}},
                #                           "day": {"Value": {"S": dd}}, "year": {"Value": {"S": yy}},
                #                           "bip_status": {"Value": {"S": 'Success'}}})            
                # except Exception as e:
                #     print(e)
                
                if zeroRowsReturned == 'false':
                    try:

                        # df = pd.read_xml(newoutput, dtype=str)
                        # # print(df)
                        # df.to_xml(xml_file, index=False)
                        # # Convert XML to CSV Format
                        # tree = ET.parse(xml_file)
                        # root = tree.getroot()
                        # get_range = lambda col: range(len(col))
                        # l = [{r[i].tag: r[i].text for i in get_range(r)} for r in root]
        
                        # df2 = pd.DataFrame.from_dict(l)
 
          
                        if loadType == 'Full' or loadType == 'Special_Full':
                            prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        elif loadType == 'CDC' or loadType == 'Special_CDC':
                            prefix = "EVO/TblData/CDC/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"

                        # if loadType == 'Full':
                        #     prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        # elif loadType == 'CDC':
                        #     prefix = "EVO/TblData/CDC/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        elif loadType == 'Validation':
                            prefix = "EVO/TblData/Validation/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        elif loadType == 'MetaData':
                            prefix = "EVO/TblData/MetaData/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        filename = prefix + tbl + "_" + str(filedatetime) + ".csv"
                        
                                               
                        # Save CSV File Format as needed
                        df = df.replace(np.nan, '', regex=True)
                        df = df.replace('null', '', regex=True)
                        df.to_csv(csv_file, index=False)
                        
                        bucket.upload_file(csv_file, filename, extra_args)
                        print("Extract Data for " + tbl + " Completed")
        
                        FileName = "/" + filename
                        row_count = len(df)
                        print('Number of Records Retrieved from Source: ', row_count)
        
                        redshift_row_count = 0
                        recordsprocessed = 0
                        redshiftbeginrowscount = 0
                        redshift_error = 'false'
                        redshift_error_message = ''

                        # Run Redshift Target Objects to load Data.
                        if loadType == 'Full' or loadType == 'Special_Full':
                            try:
                                Redshift_Target.tbl_Truncate(SchemaName, tablename)
                                result = Redshift_Target.tbl_Append(SchemaName, redshift_tablename, FileName)
                                #print(result[0])
                                redshift_row_count = result[0]
                                print(redshift_row_count)
                                redshift_error = result[1]
                                redshift_error_message = result[2]
                                print(redshift_error)
                                print(redshift_error_message)
                            except Exception as e:
                                print('line #  870: ', e)
                                redshift_error = 'true'
                                redshift_error_message = str(e)
                                recordcount = 0
                                redshift_row_count = 0
                                recordsprocessed = 0
                                redshiftbeginrowscount = 0

                        elif loadType == 'CDC' or loadType == 'Special_CDC':
                            try:
                                # upsertkey = 'delivery_detail_id'
                                print(upsertkey)
                                print(SchemaName)
                                print(tablename)
                                print(FileName)
                                result = Redshift_Target.upsert(SchemaName, redshift_tablename, FileName, upsertkey)
                                #print(result)
                                redshift_row_count = result[0]
                                recordsprocessed = result[1]
                                redshiftbeginrowscount = result[2]
                                redshift_error = result[3]
                                redshift_error_message = result[4]
                                print(recordsprocessed)
                                print(redshiftbeginrowscount)
                                print(redshift_error)
                                print(redshift_error_message)
                            except Exception as e:
                                print('line #  893: ', e)
                                redshift_error = 'true'
                                redshift_error_message = str(e)
                                recordcount = 0
                                redshift_row_count = 0
                                recordsprocessed = 0
                                redshiftbeginrowscount = 0
                        elif loadType == 'Validation' or loadType == 'MetaData':
                            try:
                                Redshift_Target.tbl_Truncate(SchemaName, redshift_tablename)
                                result = Redshift_Target.tbl_Append(SchemaName, redshift_tablename, FileName)
                                                              #print(result[0])
                                redshift_row_count = result[0]
                                print(redshift_row_count)
                                redshift_error = result[1]
                                redshift_error_message = result[2]
                                print(redshift_error)
                                print(redshift_error_message)
                            except Exception as e:
                                print('line #  870: ', e)
                                redshift_error = 'true'
                                redshift_error_message = str(e)
                                recordcount = 0
                                redshift_row_count = 0
                                recordsprocessed = 0
                                redshiftbeginrowscount = 0
                                
                        print('record count: ', recordcount)
                        redshift_row_count = int(redshift_row_count)
                        print('redshift_row_count: ',redshift_row_count)
                        
                        if loadType == 'CDC' or loadType == 'Special_CDC':
                            redshift_inserts = redshift_row_count - int(redshiftbeginrowscount)
                            redshift_updates = int(recordsprocessed) - int(redshift_inserts)
                        else:
                            redshift_inserts = redshift_row_count
                            redshift_updates = 0
                        
                        if int(recordcount) == redshift_row_count and fullToCDCFlag == 'true':
                        #if int(recordcount) == int(redshift_row_count) and row['FullToCDCFlag'] == 'true':
                            if loadType == 'Full': 
                                newloadType = 'CDC'
                            elif loadType == 'Special_Full':
                                newloadType = 'Special_CDC'
                        else:
                            newloadType = loadType
                        
                        print(newloadType)
                        
                        
                        updatenow = datetime.now()
                        updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                        
                        if redshift_error == 'true':
                            LUD = row['LastUpdateDate']
                        else:
                            LUD = updatedatetime
                        print('at DDB table update for success')                           
                        try:
                            update = table.update_item(
                                Key={
                                    'TableName': tableUID
                                },
                                UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10, redshifterror = :val11, redshifterrormessage = :val12, redshiftInserts = :val13, redshiftUpdates = :val14',
                                ExpressionAttributeValues={
                                    ':val1': LUD,
                                    ':val2': recordcount,
                                    ':val3': redshift_row_count,
                                    ':val4': errormsg,
                                    ':val5': updatedatetime,
                                    ':val6': minLUD,
                                    ':val7': maxLUD,
                                    ':val8': 'true',
                                    ':val9': newloadType,
                                    ':val10': 'false',
                                    ':val11': redshift_error,
                                    ':val12': redshift_error_message,
                                    ':val13': redshift_inserts,
                                    ':val14': redshift_updates
                                }
                            )
                        except Exception as e:
                            print('Error on DDB update for success: ', e)
                        
                        #### update ddb_stats_table
                        now = datetime.now()
                        redshiftenddatetime = now.strftime("%Y-%m-%d %H:%M:%S")
                        print('redshift end time: ', redshiftenddatetime)
                        print(str(redshiftenddatetime))
                        
                        if redshift_error == 'true':
                            redshift_status = 'Failed'
                        else:
                            redshift_status = 'Success'
                        
                        # try:
                        #     ddbconn.update_item(
                        #             TableName=ddb_stats_table,
                        #             Key={"filename": {"S": FName}},
                        #             AttributeUpdates={"redshift_fail_description": {"Value": {"S": redshift_error_message}},
                        #                               "redshift_rowcount": {"Value": {"S": str(redshift_row_count)}},
                        #                               "redshift_end_time": {"Value": {"S": str(redshiftenddatetime)}},
                        #                               "redshift_inserted_records": {"Value": {"S": str(redshift_inserts)}},
                        #                               "redshift_updated_records": {"Value": {"S": str(redshift_updates)}},
                        #                               "redshift_processed_records": {"Value": {"S": str(redshift_updates + redshift_inserts)}},
                        #                               "redshift_status": {"Value": {"S": redshift_status}}})
                        
                        # except Exception as e:
                        #     print(e)    
                    except Exception as e:
                        print(e)
                #### ZeroRowsReturned is true #### no records returned from BIP
                else:
                    updatenow = datetime.now()
                    updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                    
                    try:
                        update = table.update_item(
                            Key={
                                'TableName': tableUID
                                },
                                UpdateExpression='SET LastUpdateDate = :val1, ZeroRowsReturned = :val2',
                                ExpressionAttributeValues={
                                    ':val1': updatedatetime,
                                    ':val2': 'true'
                                }
                            )
                        
                        ddbconn.update_item(
                                    TableName=ddb_stats_table,
                                    Key={"filename": {"S": FName}},
                                    AttributeUpdates={"redshift_start_time": {"Value": {"S": ''}},
                                                      "redshift_status": {"Value": {"S": 'Success - No rows returned from BIP'}}})
                    except Exception as e:
                        print('Error with DDB table update: ',e)
                    
                
            else:
                #### STATUS IS FAILURE BIP failure #######
                updatenow = datetime.now()
                updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                
                try:
                    update = table.update_item(
                        Key={
                            'TableName': tableUID
                        },
                        # UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = 
                        UpdateExpression='SET SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10, redshifterror = :val11, redshifterrormessage = :val12, redshiftInserts = :val13, redshiftUpdates = :val14',
                                ExpressionAttributeValues={
                                    # ':val1': updatedatetime,
                                    ':val2': recordcount,
                                    ':val3': redshift_row_count,
                                    ':val4': errormsg,
                                    ':val5': updatedatetime,
                                    ':val6': minLUD,
                                    ':val7': maxLUD,
                                    ':val8': 'true',
                                    ':val9': newloadType,
                                    ':val10': 'false',
                                    ':val11': redshift_error,
                                    ':val12': redshift_error_message,
                                    ':val13': 0,
                                    ':val14': 0
                                }
                            )
                    ddbconn.update_item(
                                    TableName=ddb_stats_table,
                                    Key={"filename": {"S": FName}},
                                    AttributeUpdates={'bip_fail_description': {"Value": {"S": errormsg}},
                                                      "bip_status": {"Value": {"S": 'Failed'}},
                                                      "bip_end_time": {"Value": {"S": updatedatetime}}})
                except Exception as e:
                    print('BIP Failure routine: ',e)
        
        
        
            # try:
            #     response = ddbconn.get_item(
            #       TableName=ddb_stats_table ,
            #       Key={'filename': {'S': FName}})
            #     data = response['Item']
            #     df = pd.DataFrame(data)
            #     df.to_csv('/tmp/input.csv', index=False)
            #     newfilename = "/tmp/" + 'input.csv'
            #     sourcekey = 'EVOBaseTblLoadStats_Replicate/Replicate_' + FName
            #     if account_id == prodaccountid:
            #         buck = 'ddb-streams'
            #     elif account_id == devaccountid:
            #         buck = 'ddb-streams-dev'
            #     s3_client.upload_file(newfilename, buck, sourcekey,extra_args)
            # except Exception as e:
            #     print('Failure to write DDB stats table: ', e)
                
                


############################################################################################################################################################################
########################################## Daily exception processing for a failed month ###############################################################################
############################################################################################################################################################################
def daily_batches(tbl,daily_beg_date,daily_end_date,cnt, recordcount):
    from datetime import date
    from datetime import datetime, timedelta
    
    
    format = '%Y-%m-%d'

    first_day_of_week = datetime.strptime(daily_beg_date, format)
    last_day_of_week = datetime.strptime(daily_end_date, format)
    
    
    beg_date_list = []
    end_date_list = []

    no_list = [1, 2, 3, 4, 5 , 6, 7]
    for x in no_list:
        if x == 1:
            y  = first_day_of_week
            z = y
        elif x == 2:
            y = z + timedelta(days=1)
            z = y
        elif x == 3:
            y = z + timedelta(days=1)
            z = y
        elif x == 4:
            y = z + timedelta(days=1)
            z = y
        elif x == 5:
            y = z + timedelta(days=1)
            z = y
        elif x == 6:
            y = z + timedelta(days=1)
            z = y
        elif x == 7:
            y = z + timedelta(days=1)
            z = y
        beg_date_list.append(y)
        end_date_list.append(z)
    print(beg_date_list)
    print(end_date_list)
    cnt1 = 0
    for x in beg_date_list:
        process_daily_batches(tbl,x, end_date_list[cnt1], cnt, cnt1+1,recordcount)
        cnt1 += 1

############################################################################################################################################################################
########################################## End of Daily exception processing for a failed week ########################################################################
############################################################################################################################################################################

###########################################################################################################################################################################
########## process daily batches is called from the Weekly routine - process is just used for full data loads with batch flag set to true ####################################
###########################################################################################################################################################################
def process_daily_batches(tbl, daily_begin_date, daily_end_date,cnt, cnt1,recordcount):
    
    daily_begin_date = daily_begin_date.strftime("%Y-%m-%d")
    daily_end_date = daily_end_date.strftime("%Y-%m-%d")
   
    inputSQLtxt = "SELECT * FROM " + tbl

    #inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= TO_DATE(TRUNC(CURRENT_DATE - " + str(beg_dt_days) + ")) AND TRUNC(LAST_UPDATE_DATE) <= TO_DATE(TRUNC(CURRENT_DATE - " + str(end_dt_days) + "))"

    inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= (to_date(''" + daily_begin_date + "'',''YYYY-MM-DD''))"
    inputSQLtxt = inputSQLtxt + " AND TRUNC(LAST_UPDATE_DATE) <= (to_date(''" + daily_end_date + "'',''YYYY-MM-DD''))"
    print(inputSQLtxt)
    
    
    folderPath = '/Custom/CloudTools/V4'
    baseName = "FSTReportCSV"
    dm_baseName = "FusionSQLToolDM"
    reportName = baseName + ".xdo"
    reportPath = folderPath + "/" + reportName
    xdmName = dm_baseName
    dmPath = folderPath + "/" + xdmName + '.xdm'  
    
    csv_file = "/tmp/" + tbl + ".csv"
    
    # create DM
    result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
    result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
    
    # run report to return csv file
    result = BIP_Main.CSVrunReport(instanceURL, folderPath, reportPath,fusionUserName, fusionPassword)
    
    status = result[0]

    print('Status: ', status)
    newoutput = result[1]
    
    

    data_is_too_large = -1
    zeroRowsReturned = 'false'
    
    if status == 'Success':
        newoutput = result[1]
        errormsg = ''
        print(len(newoutput))
        if len(newoutput) < 10:
            zeroRowsReturned = 'true'
        else:
            zeroRowsReturned = 'false'
            with open(csv_file, 'w', encoding="utf-8") as output_file:
                output_file.write(newoutput)
            
            errormsg = ''

            # Setup file and naming configurations
            now = datetime.now()
            filedatetime = now.strftime("%Y%m%d")

            print(filedatetime)
            updatedatetime = now.strftime("%Y-%m-%d %H:%M:%S")
            print(updatedatetime)
            df = pd.read_csv(csv_file,dtype=str)
            df = df.replace(np.nan, '', regex=True)
            df = df.replace('null', '', regex=True)
            csv_file = "/tmp/" + tbl + ".csv"
            df.to_csv(csv_file, index=False)
            prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
           
            filename = prefix + tbl + '_batch_' + str(cnt) + "_" + str(filedatetime) + ".csv"
            
            tablename = tbl
            FileName = "/" + filename
            
            bucket.upload_file(csv_file, filename, extra_args)
            print("Extract Data for " + tbl + " Completed")
            row_count = len(df)
            print('Number of Records Retrieved from Source: ', row_count)
            result = Redshift_Target.tbl_Append(SchemaName, tablename, FileName)
            #print(result[0])
            redshift_row_count = result[0]
            print(redshift_row_count)
            #print(len(df))
    else:
        errormsg = result[1]
        print('FAILED error message ', errormsg)
        data_is_too_large = errormsg.find('exceeds the maximum limit')
        #exceeds the maximum limit (1073741824 bytes).
        if data_is_too_large != -1:
            errormsg = 'Report data size exceeds the maximum limit for online reports'
            print('FAILED calling daily batch routine: ', errormsg)
            #hourly_batches(tbl,begin_date,end_date, cnt)   
    
    
    if status == 'Success' and zeroRowsReturned == 'false':

        try:        
            if int(recordcount) ==  int(redshift_row_count) :
                newloadType = 'CDC'
            else:
                newloadType = 'Full'
            
            print(newloadType)
            now = datetime.now()
            filedatetime = now.strftime("%Y%m%d")
    
            print(filedatetime)
            updatedatetime = now.strftime("%Y-%m-%d %H:%M:%S")
            print(updatedatetime)
            update = table.update_item(
                Key={
                    'TableName': tableUID
                },
                UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10',
                ExpressionAttributeValues={
                    ':val1': updatedatetime,
                    ':val2': recordcount,
                    ':val3': redshift_row_count,
                    ':val4': errormsg,
                    ':val5': updatedatetime,
                    ':val6': minLUD,
                    ':val7': maxLUD,
                    ':val8': 'true',
                    ':val9': newloadType,
                    ':val10': 'false'
                }
            ) 
        
        except Exception as e:
            print('error during batch daily processing routine update DDB item')


 
####### end of process batches routine called from Daily processing routine ################# 
 
        
        

############################################################################################################################################################################
########################################## Weekly exception processing for a failed month ###############################################################################
############################################################################################################################################################################
def semi_monthly_batches(tbl,beg_date,end_date,cnt, recordcount):
    from datetime import date
    from datetime import datetime, timedelta
    # first_day_of_month = beg_date_days.days
    # last_day_of_month = end_date_days.days
    # num_of_days = beg_date_days.days - end_date_days.days
    # print(num_of_days)
    # month_middle =  first_day_of_month - round(num_of_days / 2)
    # month_middle_next_day = month_middle - 1
    # print(first_day_of_month)
    # print(round(month_middle))
    # print(month_middle_next_day)
    # print(last_day_of_month)

    # beg_date_list.append(first_day_of_month)
    # end_date_list.append(round(month_middle))
    # beg_date_list.append(month_middle_next_day)
    # end_date_list.append(last_day_of_month)
    # print(beg_date_list)
    # print(end_date_list)
    # process_batches(tbl, first_day_of_month, round(month_middle))
    # process_batches(tbl, month_middle_next_day, round(last_day_of_month))
   
    # no_list = [1,2,3,4]
    # for x in no_list:
    #     if x == 1:
    #         y = first_day_of_month
    #         z = first_day_of_month - 6
    #     elif x == 2:
    #         y = first_day_of_month - 7
    #         z = first_day_of_month - 13
    #     elif x == 3:
    #         y = first_day_of_month - 14
    #         z = first_day_of_month - 20
    #     elif x == 4:
    #         y = first_day_of_month - 21
    #         z = last_day_of_month
    #     beg_date_list.append(y)
    #     end_date_list.append(z)
    ####### new code 05/12/20223 ########
    format = '%Y-%m-%d'

    first_day_of_month = datetime.strptime(beg_date, format)
    last_day_of_month = datetime.strptime(end_date, format)
        
    beg_date_list = []
    end_date_list = []
    
    no_list = [1, 2, 3, 4, 5 , 6]
    for x in no_list:
        if x == 1:
            #y = first_day_of_month
            y  = first_day_of_month
            #z = first_day_of_month - 6
            z = y + timedelta(days=6)
            start = y - timedelta(days=y.weekday())
            z = start + timedelta(days=6)
            print(y)
            print(z)
        elif x == 2:
            #y = first_day_of_month
            #y  = datetime.strptime(first_day_of_month, format)
            #z = first_day_of_month - 6
            y = z + timedelta(days=1)
            z = y + timedelta(days=6)
            start = y - timedelta(days=y.weekday())
            z = start + timedelta(days=6)
            print(y)
            print(z)
        elif x == 3:
            # y = first_day_of_month
            # y  = datetime.strptime(first_day_of_month, format)
            # z = first_day_of_month - 6
            y = z + timedelta(days=1)
            z = y + timedelta(days=6)
            start = y - timedelta(days=y.weekday())
            z = start + timedelta(days=6)
            print(y)
            print(z)
        elif x == 4:
            # y = first_day_of_month
            # y  = datetime.strptime(first_day_of_month, format)
            # z = first_day_of_month - 6
            y = z + timedelta(days=1)
            z = y + timedelta(days=6)
            start = y - timedelta(days=y.weekday())
            z = start + timedelta(days=6)
            if z > last_day_of_month:
                z = last_day_of_month
            print(y)
            print(z)
        elif x == 5:
            # y = first_day_of_month
            # y  = datetime.strptime(first_day_of_month, format)
            # z = first_day_of_month - 6
            y = z + timedelta(days=1)
            z = y + timedelta(days=6)
            start = y - timedelta(days=y.weekday())
            z = start + timedelta(days=6)
            if z > last_day_of_month:
                z = last_day_of_month
            print(y)
            print(z)
        elif x == 6:
            # y = first_day_of_month
            # y  = datetime.strptime(first_day_of_month, format)
            # z = first_day_of_month - 6
            y = z + timedelta(days=1)
            z = y + timedelta(days=6)
            start = y - timedelta(days=y.weekday())
            z = start + timedelta(days=6)
            if z > last_day_of_month:
                z = last_day_of_month
            print(y)
            print(z)
    #         y = first_day_of_month - 21
    #         z = last_day_of_month
        if y <= last_day_of_month:
            beg_date_list.append(y)
            end_date_list.append(z)
    print(beg_date_list)
    print(end_date_list)
    cnt1 = 0
    for x in beg_date_list:
        process_batches(tbl,x, end_date_list[cnt1], cnt, cnt1+1, recordcount)
        cnt1 += 1

############################################################################################################################################################################
########################################## End of Weekly exception processing for a failed month ########################################################################
############################################################################################################################################################################

###########################################################################################################################################################################
########## process batches is called from the Weekly routine - process is just used for full data loads with batch flag set to true ####################################
###########################################################################################################################################################################
def process_batches(tbl, begin_date, end_date,cnt, cnt1, recordcount):
    
    begin_date = begin_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")
   
    inputSQLtxt = "SELECT * FROM " + tbl

    #inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= TO_DATE(TRUNC(CURRENT_DATE - " + str(beg_dt_days) + ")) AND TRUNC(LAST_UPDATE_DATE) <= TO_DATE(TRUNC(CURRENT_DATE - " + str(end_dt_days) + "))"

    inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= (to_date(''" + begin_date + "'',''YYYY-MM-DD''))"
    inputSQLtxt = inputSQLtxt + " AND TRUNC(LAST_UPDATE_DATE) <= (to_date(''" + end_date + "'',''YYYY-MM-DD''))"
    print(inputSQLtxt)
    
    #####
    # baseName = "FSTreport_" + tbl + '_batch_' + str(cnt) + '_' + str(cnt1)
    # reportName = baseName + ".xdo"
    # reportPath = folderPath + "/" + reportName
    # xdmName = baseName
    # dmPath = folderPath + "/" + xdmName + '.xdm'
    
    # result = BIP_Main.createDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
    # #print(result)
    # # #
    # result = BIP_Main.createDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
    # #print(result)
    # # # # # #
    # result1 = BIP_Main.createReport(instanceURL, folderPath, dmPath, reportName, fusionUserName, fusionPassword)
    # #print(result1)
    
    # result = BIP_Main.runReport(instanceURL, folderPath, reportPath, fusionUserName, fusionPassword)
    # # print('results from query running: ', result)
    # ### should check newoutput for failures from run report ####
    # status = result[0]
    # print('Status: ', status)
    
    # #print('results from query running: ', newoutput)
    # try:
    #     BIP_Main.deleteReport(instanceURL, reportPath, fusionUserName, fusionPassword)
    #     BIP_Main.deleteReport(instanceURL, dmPath, fusionUserName, fusionPassword)
    # except Exception as e:
    #     print('no reports or data model to delete so do nothing')
    ####
    
    
    folderPath = '/Custom/CloudTools/V4'
    baseName = "FSTReportCSV"
    dm_baseName = "FusionSQLToolDM"
    reportName = baseName + ".xdo"
    reportPath = folderPath + "/" + reportName
    xdmName = dm_baseName
    dmPath = folderPath + "/" + xdmName + '.xdm'  
    
    csv_file = "/tmp/" + tbl + ".csv"
    
    # create DM
    result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
    result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
    
    # run report to return csv file
    result = BIP_Main.CSVrunReport(instanceURL, folderPath, reportPath,fusionUserName, fusionPassword)
    
    status = result[0]

    print('Status: ', status)
    newoutput = result[1]
    
    

    data_is_too_large = -1
    
    zeroRowsReturned = 'false'
    errormsg = ''    
    
    if status == 'Success':
        newoutput = result[1]
        errormsg = ''
        print(len(newoutput))
        if len(newoutput) < 10:
            zeroRowsReturned = 'true'
        else:
            zeroRowsReturned = 'false'
            with open(csv_file, 'w', encoding="utf-8") as output_file:
                output_file.write(newoutput)
            
            errormsg = ''

            # Setup file and naming configurations
            now = datetime.now()
            filedatetime = now.strftime("%Y%m%d")

            print(filedatetime)
            updatedatetime = now.strftime("%Y-%m-%d %H:%M:%S")
            print(updatedatetime)
            df = pd.read_csv(csv_file,dtype=str)
            df = df.replace(np.nan, '', regex=True)
            df = df.replace('null', '', regex=True)
            csv_file = "/tmp/" + tbl + ".csv"
            df.to_csv(csv_file, index=False)
            prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
           
            filename = prefix + tbl + '_batch_' + str(cnt) + "_" + str(filedatetime) + ".csv"
            
            tablename = tbl
            FileName = "/" + filename
            
            bucket.upload_file(csv_file, filename, extra_args)
            print("Extract Data for " + tbl + " Completed")
            row_count = len(df)
            print('Number of Records Retrieved from Source: ', row_count)
            result = Redshift_Target.tbl_Append(SchemaName, tablename, FileName)
            #print(result[0])
            redshift_row_count = result[0]
            print(redshift_row_count)
            #print(len(df))
    else:
        errormsg = result[1]
        print('FAILED error message ', errormsg)
        data_is_too_large = errormsg.find('exceeds the maximum limit')
        #exceeds the maximum limit (1073741824 bytes).
        if data_is_too_large != -1:
            errormsg = 'Report data size exceeds the maximum limit for online reports'
            print('FAILED calling weekly batch routine: ', errormsg)
            daily_batches(tbl,begin_date,end_date, cnt, recordcount)
    
    if status == 'Success' and zeroRowsReturned == 'false':
     
        try:        
            if int(recordcount) ==  int(redshift_row_count) :
                newloadType = 'CDC'
            else:
                newloadType = 'Full'
            
            print(newloadType)
            now = datetime.now()
            filedatetime = now.strftime("%Y%m%d")

            print(filedatetime)
            updatedatetime = now.strftime("%Y-%m-%d %H:%M:%S")
            print(updatedatetime)
            update = table.update_item(
                Key={
                    'TableName': tableUID
                },
                UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10',
                ExpressionAttributeValues={
                    ':val1': updatedatetime,
                    ':val2': recordcount,
                    ':val3': redshift_row_count,
                    ':val4': errormsg,
                    ':val5': updatedatetime,
                    ':val6': minLUD,
                    ':val7': maxLUD,
                    ':val8': 'true',
                    ':val9': newloadType,
                    ':val10': 'false'
                }
            ) 
        
        except Exception as e:
            print('error during batch weekly processing routine update DDB item')



       
 
####### end of process batches routine called from BI-Weekly Batch processing routin3 ################# 
 
         
        
##############################################################################
#################### Full Data Load Batch Processing Routine #################
##############################################################################

table = ddb.Table(ddb_tablename)
response = table.scan()
data = response['Items']
listdf = pd.DataFrame(data)

fileprefix = 'EVO'+ '/'+ 'TblSQL'+ '/'

listdf = listdf.loc[listdf['Frequency'].isin(['Daily'])]
listdf = listdf.loc[listdf['ActiveDownloadFlag'].isin(['true'])]
listdf = listdf.loc[listdf['TableCreatedFlag'].isin(['true'])]
listdf = listdf.loc[listdf['DailyLoadType'].isin(['CDC', 'Full','Validation','Special_Full','Special_CDC','MetaData'])]
listdf = listdf.loc[listdf['Schedule'].isin(['false'])]
listdf = listdf.loc[listdf['BatchesFlag'].isin(['true'])]
if priority != 'All':
    listdf = listdf.loc[listdf['priority'].isin([int(priority)])]
if EVOInstance != 'All':
    listdf = listdf.loc[listdf['EVOInstance'].isin([EVOInstance])]
listdf = listdf.sort_values(['priority','sortOrder'])

redshift_error = 'false'
redshift_error_message = ''     

# dmPath = folderPath + "/FusionSQLToolDM.xdm"
# reportPath = folderPath + "/FSTreport.xdo"
# dmPath = folderPath + "/FusionSQLToolDM.xdm"
# reportPath = folderPath + "/FSTreport.xdo"

for index, row in listdf.iterrows():
    ######  determine which EVO Instance to create and run the reports based upon entry in dynamodb item ###
    ###### In prod this should always be prod more than likely ##########
    if row['EVOInstance'] == 'prod':
        instanceURL = prodInstanceURL
        fusionUserName = prodFusionUserName
        fusionPassword = prodFusionPassword
    if row['EVOInstance'] == 'test':
        instanceURL = testInstanceURL
        fusionUserName = testFusionUserName
        fusionPassword = testFusionPassword
    elif row['EVOInstance'] == 'dev1':
        instanceURL    = dev1InstanceURL
        fusionUserName = dev1FusionUserName
        fusionPassword = dev1FusionPassword    
    elif row['EVOInstance'] == 'dev2':
        instanceURL    = dev2InstanceURL
        fusionUserName = dev2FusionUserName
        fusionPassword = dev2FusionPassword    
    elif row['EVOInstance'] == 'dev3':
        instanceURL    = dev3InstanceURL
        fusionUserName = dev3FusionUserName
        fusionPassword = dev3FusionPassword    
    elif row['EVOInstance'] == 'dev4':
        instanceURL    = dev4InstanceURL
        fusionUserName = dev4FusionUserName
        fusionPassword = dev4FusionPassword
    elif row['EVOInstance'] == 'dev5':
        instanceURL    = dev5InstanceURL
        fusionUserName = dev5FusionUserName
        fusionPassword = dev5FusionPassword
    elif row['EVOInstance'] == 'dev6':
        instanceURL    = dev6InstanceURL
        fusionUserName = dev6FusionUserName
        fusionPassword = dev6FusionPassword
    elif row['EVOInstance'] == 'dev7':
        instanceURL    = dev7InstanceURL
        fusionUserName = dev7FusionUserName
        fusionPassword = dev7FusionPassword


    # print(instanceURL)
    # print(testInstanceURL)
    # print(fusionUserName)
    # print(testFusionUserName)
    # print(fusionPassword)
    # print(testFusionPassword)


    url = instanceURL + "/xmlpserver/services/v2/CatalogService"
    
    SchemaName = row['SchemaName']
    loadType = row['DailyLoadType']
    upsertkey = row['primarykey']
    tbl = row['TableName1']
    tablename = tbl
    redshift_tablename = tbl
    if loadType == 'Validation':
        redshift_tablename = row['ValidationRedshiftTableName']
    tableUID = row['TableName']
    
    FullToCDCFlag = row['FullToCDCFlag']
    fullToCDCFlag = FullToCDCFlag
    

    
    loadtype = loadType
    
    errormsg = ''
    sourceRowCount = 0
    redshift_row_count = 0
    redshift_inserts = 0
    redshift_updates = 0
    redshift_error = 'false'
    redshift_error_message = ''
    newloadType = loadtype
    redshiftbeginrowscount = 0
    
    recLastUpdateDate = row['LastUpdateDate'][0:10]
    #print(recLastUpdateDate)
    
    if recLastUpdateDate != today:

        if row['BatchesFlag'] == 'true' and loadtype == 'Full':
            print('at large files routine to run in batches by period')
            print('Instance URL: ',instanceURL)
        
            batches_redshift_row_count = 0
            sourceRowCount = 0
            redshift_row_count = 0
            tbl = row['TableName1']
            print('tablename: ', tbl)
            tablename = tbl
            SchemaName = row['SchemaName']
            #loadType = row['DailyLoadType']
            upsertkey = row['primarykey']
            ### attempt to get total record counts from tablename
            recordCounts = recordCnts(tbl, loadType)
            print('RecordCounts: ',recordCounts)
            
            with open('/tmp/counts.csv', 'w', encoding="utf-8") as output_file:
                output_file.write(recordCounts)
            counts_df = pd.read_csv('/tmp/counts.csv',dtype=str)
            
            counts_df = counts_df.replace(np.nan, '', regex=True)
            counts_df = counts_df.replace('nan', '', regex=True)
            counts_df = counts_df.replace('null', '', regex=True)
            
            recordcount = 0
            minLUD = ''
            maxLUD = ''
            try:
                for index, row in counts_df.iterrows():
                    recordcount = row['CNT']
                    minLUD = row['MIN_LUD']
                    maxLUD = row['MAX_LUD']
            except Exception as e:
                recordcount = 0
                minLUD = ''
                maxLUD = ''
            print(recordcount)
            print(minLUD)
            print(maxLUD)

            # get the actual day number
            day_num = now.strftime("%d")
            if minLUD == '':
                begin_yearmonth = 202209
            else:
                begin_yearmonth = int(minLUD[:7].replace('-',''))
            current_yearmonth = int(now.strftime("%Y%m"))
            str_begin_yearmonth = str(begin_yearmonth)
            str_current_yearmonth = str(current_yearmonth)
            count = 0
            periods_list = []
            periods_list.append(str_begin_yearmonth)
            print(begin_yearmonth)
            print(current_yearmonth)
            while (begin_yearmonth < current_yearmonth):
                # newdate = np.datetime64(str_begin_yearmonth) + np.timedelta64(1, 'M')
                y = str(begin_yearmonth)[:4]
                m = str(begin_yearmonth)[-2:]
                d = 1
                newdate = date(int(y), int(m), d) + relativedelta(months=1)
                begin_yearmonth = int(str(newdate).replace('-', '')[:6])
                str_begin_yearmonth = str(begin_yearmonth)
                periods_list.append(str_begin_yearmonth)
            
            begin_date_list = []
            end_date_list = []
            for x in periods_list:
                year = x[:4]
                # yr = int(x.partition('-')[0])
                month = x[-2:]
                # mm = int(x.partition('-')[2])
                day = 1
                dd = str(1).zfill(2)
                # print(year)
                # print(month)
            
                #begin_date = datetime(int(year), int(month), day)
                begin_date = year + '-' + month + '-' + dd
            
            
                # print(begin_date)
                begin_date_list.append(begin_date)
                # print(day)
                last_date_month = datetime(int(year), int(month), day)
                current_month_last_date = last_date_month + relativedelta(day=31)
                current_month_last_date = current_month_last_date.strftime("%Y-%m-%d")
            
                end_date_list.append(current_month_last_date)
            
            print(begin_date_list)
            print(end_date_list)
            
            Redshift_Target.tbl_Truncate(SchemaName, tablename)
            
            cnt = 0
            for begin_date in begin_date_list:
                end_date = end_date_list[cnt]
                cnt += 1
                errormsg = ''
            
                inputSQLtxt = ""
                # BIP_Main.createDataModel(folderPath, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
                print("Extract data from EVO SRC for Base Table: " + tbl)
                # n1 = datetime.now()
                # beg_date_days = n1 - begin_date
                # print(beg_date_days.days)
                # end_date_days = n1 - end_date
                # print(end_date_days.days)
            
                ####    These result will tell you how to break up the dates by code.
                ####    ##inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) = (to_date(''2023-03-08'',''YYYY-MM-DD''))"
                ####    #inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) <> (to_date(''2023-03-08'',''YYYY-MM-DD''))"
                ####    #inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) > (to_date(''2023-03-08'',''YYYY-MM-DD''))"
            
            
                inputSQLtxt = "SELECT * FROM " + tbl
                # inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= TO_DATE(''" + begin_date + "'',''YYYY-MM-DD'') AND TRUNC(LAST_UPDATE_DATE) <= TO_DATE(''" + end_date + "'','YYYY-MM-DD')"
                # inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= TRUNC('" + str(begin_date) + "') AND TRUNC(LAST_UPDATE_DATE) <= TRUNC(''" + str(end_date) + "'')"
                # inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= TO_DATE(TRUNC(CURRENT_DATE - " + str(
                #     beg_date_days.days) + ")) AND TRUNC(LAST_UPDATE_DATE) <= TO_DATE(TRUNC(CURRENT_DATE - " + str(
                #     end_date_days.days) + "))"
                # inputSQLtxt += " AND TRUNC(LAST_UPDATE_DATE) <= TO_DATE(CURRENT_DATE - 1)"
                # TO_DATE(TRUNC(CURRENT_DATE - 1))"
                inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= (to_date(''" + begin_date + "'',''YYYY-MM-DD''))"
                inputSQLtxt = inputSQLtxt + " AND TRUNC(LAST_UPDATE_DATE) <= (to_date(''" + end_date + "'',''YYYY-MM-DD''))"
            
                print(inputSQLtxt)
        
                #####
                # baseName = "FSTreport_" + tbl + '_batch_' + str(cnt)
                # reportName = baseName + ".xdo"
                # reportPath = folderPath + "/" + reportName
                # xdmName = baseName
                # dmPath = folderPath + "/" + xdmName + '.xdm'
        
                # result = BIP_Main.createDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
                # #print(result)
                # # #
                # result = BIP_Main.createDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
                # #print(result)
                # # # # # #
                # result1 = BIP_Main.createReport(instanceURL, folderPath, dmPath, reportName, fusionUserName, fusionPassword)
                # #print(result1)
        
                # result = BIP_Main.runReport(instanceURL, folderPath, reportPath, fusionUserName, fusionPassword)
                # # print('results from query running: ', result)
                # ### should check newoutput for failures from run report ####
                # status = result[0]
                # print('Status: ', status)
                
                # try:
                #     BIP_Main.deleteReport(instanceURL, reportPath, fusionUserName, fusionPassword)
                #     BIP_Main.deleteReport(instanceURL, dmPath, fusionUserName, fusionPassword)
                # except Exception as e:
                #     print('no reports or data model to delete so do nothing')
                
                folderPath = '/Custom/CloudTools/V4'
                baseName = "FSTReportCSV"
                dm_baseName = "FusionSQLToolDM"
                reportName = baseName + ".xdo"
                reportPath = folderPath + "/" + reportName
                xdmName = dm_baseName
                dmPath = folderPath + "/" + xdmName + '.xdm'               
 
                 
                csv_file = "/tmp/" + tbl + ".csv"
                 
                 # create DM
                result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
                result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
                
                # run report to return csv file
                result = BIP_Main.CSVrunReport(instanceURL, folderPath, reportPath,fusionUserName, fusionPassword)
                
                status = result[0]

                print('Status: ', status)
                newoutput = result[1]
                
                
     
                data_is_too_large = -1
                    
                if status == 'Success':
                    newoutput = result[1]
                    errormsg = ''
                    print(len(newoutput))
                    if len(newoutput) < 10:
                        zeroRowsReturned = 'true'
                    else:
                        zeroRowsReturned = 'false'
                        with open(csv_file, 'w', encoding="utf-8") as output_file:
                            output_file.write(newoutput)
                        df = pd.read_csv(csv_file,dtype=str)
                        #print(len(df))
                else:
                    errormsg = result[1]
                    print('FAILED error message ', errormsg)
                    data_is_too_large = errormsg.find('exceeds the maximum limit')
                    #exceeds the maximum limit (1073741824 bytes).
                    if data_is_too_large != -1:
                        errormsg = 'Report data size exceeds the maximum limit for online reports'
                        print('FAILED calling weekly batch routine: ', errormsg)
                        semi_monthly_batches(tbl,begin_date,end_date, cnt, recordcount)
                
                #print('results from query running: ', newoutput)
        
                ####
        
                if status == 'Success':
        
                    errormsg = ''
        
                    try:
                        # Setup file and naming configurations
                        now = datetime.now()
                        filedatetime = now.strftime("%Y%m%d")
        
                        print(filedatetime)
                        updatedatetime = now.strftime("%Y-%m-%d %H:%M:%S")
                        print(updatedatetime)
        
                        # xml_file = "/tmp/" + tbl + "_" + filedatetime + ".xml"
                        # print(xml_file)
                        
                        # # Run the Beautiful Soup Extract of specified Data.
                        # #         reportBytes = soup.find('reportBytes').string
                        # #         #print(base64.b64decode(reportBytes).decode('UTF-8'))
                        # # # Decode Object to XML and Save DF.
                        # #         newoutput = base64.b64decode(reportBytes).decode('UTF-8')
        
                        # #print('newoutput: ', newoutput)
                        # df = pd.read_xml(newoutput, dtype = str)
                        # print(len(df))
                        # df.to_xml(xml_file, index=False)
                        # # Convert XML to CSV Format
                        # tree = ET.parse(xml_file)
                        # root = tree.getroot()
                        # get_range = lambda col: range(len(col))
                        # l = [{r[i].tag: r[i].text for i in get_range(r)} for r in root]
        
                        # df2 = pd.DataFrame.from_dict(l)
                        # Save CSV File Format as needed
                        
                        csv_file = "/tmp/" + tbl + ".csv"
                        df = df.replace(np.nan, '', regex=True)
                        df = df.replace('null', '', regex=True)
                        df.to_csv(csv_file, index=False)
                        if loadtype == 'Full':
                            prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        elif loadtype == 'CDC':
                            prefix = "EVO/TblData/CDC/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        elif loadType == 'MetaData':
                            prefix = "EVO/TblData/MetaData/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        
                        filename = prefix + tbl + '_batch_' + str(cnt) + "_" + str(filedatetime) + ".csv"
                        
                        bucket.upload_file(csv_file, filename, extra_args)
                        print("Extract Data for " + tbl + " Completed")
                        tablename = tbl
                        FileName = "/" + filename
                        row_count = len(df)
                        print('Number of Records Retrieved from Source: ', row_count)
        
                        # Run Redshift Target Objects to load Data.
                        if loadType == 'Full':
                            try:
                                
                                
                                updatenow = datetime.now()
                                updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                                
                                result = Redshift_Target.tbl_Append(SchemaName, tablename, FileName)
                                #print(result[0])
                                redshift_row_count = result[0]
                                print(redshift_row_count)
                                print('Source Recordcnt: ', recordcount)
                                #batches_redshift_row_count = batches_redshift_row_count + int(redshift_row_count)
        
                                # if int(recordcount) == int(redshift_row_count) and row['DailyLoadType'] == 'Full' and row['FullToCDCFlag'] == 'true':
                                #     newloadType = 'CDC'
                                # else:
                                #     newloadType = loadType
                                
                                # print(newloadType)
                                
                                # update = table.update_item(
                                #     Key={
                                #         'TableName': tableUID
                                #     },
                                #     UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10',
                                #     ExpressionAttributeValues={
                                #         ':val1': updatedatetime,
                                #         ':val2': recordcount,
                                #         ':val3': redshift_row_count,
                                #         ':val4': errormsg,
                                #         ':val5': updatedatetime,
                                #         ':val6': minLUD,
                                #         ':val7': maxLUD,
                                #         ':val8': 'true',
                                #         ':val9': newloadType,
                                #         ':val10': 'false'
                                #     }
                                # )
                            except Exception as e:
                                print(e)
                                print(errormsg)
        
                    except Exception as e:
                        print(e)
                        print(errormsg)
        
                else:
                    print('Failed: ', errormsg)
            
                
                if int(recordcount) ==  int(redshift_row_count) and loadType == 'Full' and FullToCDCFlag == 'true':
                    newloadType = 'CDC'
                else:
                    newloadType = loadType
                
                print(newloadType)
                now = datetime.now()
                filedatetime = now.strftime("%Y%m%d")

                print(filedatetime)
                updatedatetime = now.strftime("%Y-%m-%d %H:%M:%S")
                print(updatedatetime)
                update = table.update_item(
                    Key={
                        'TableName': tableUID
                    },
                    UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10',
                    ExpressionAttributeValues={
                        ':val1': updatedatetime,
                        ':val2': recordcount,
                        ':val3': redshift_row_count,
                        ':val4': errormsg,
                        ':val5': updatedatetime,
                        ':val6': minLUD,
                        ':val7': maxLUD,
                        ':val8': 'true',
                        ':val9': newloadType,
                        ':val10': 'false'
                    }
                ) 
                    


#######################################################################
#### process items that are scheduled with Full Daily Data Loads #####
####################Currently no items are being scheduled ###########
####### the shceduled process may be for future use ##################


table = ddb.Table(ddb_tablename)
response = table.scan()
data = response['Items']
listdf = pd.DataFrame(data)

fileprefix = 'EVO'+ '/'+ 'TblSQL'+ '/'
print(' At schedule process routine')

scheduledf = listdf
scheduledf = listdf.loc[listdf['Frequency'].isin(['Daily'])]
scheduledf = scheduledf.loc[scheduledf['TableCreatedFlag'].isin(['true'])]
scheduledf = scheduledf.loc[scheduledf['ActiveDownloadFlag'].isin(['true'])]
#scheduledf = scheduledf.loc[scheduledf['DailyLoadType'].isin(['Full','MetaData'])]
scheduledf = scheduledf.loc[scheduledf['DailyLoadType'].isin(['Full','Special_Full'])]
scheduledf = scheduledf.loc[scheduledf['Schedule'].isin(['true','Full','Batches', 'Manual'])]
#scheduledf = scheduledf.loc[scheduledf['BatchesFlag'].isin(['true'])]
print(scheduledf)
print('priority :', priority )
print('EVOInstance: ', EVOInstance)
if priority != 'All':
    scheduledf = scheduledf.loc[scheduledf['priority'].isin([int(priority)])]
if EVOInstance != 'All':
    scheduledf = scheduledf.loc[scheduledf['EVOInstance'].isin([EVOInstance])]
if priority != 'All':
    listdf = listdf.loc[listdf['priority'].isin([int(priority)])]

scheduledf = scheduledf.sort_values(['priority','sortOrder'])
print(scheduledf)

tablenamelist = scheduledf['TableName1'].tolist()

for index, row in scheduledf.iterrows():
    print('in loop row: ', row)
    if row['EVOInstance'] == 'prod':
        instanceURL = prodInstanceURL
        fusionUserName = prodFusionUserName
        fusionPassword = prodFusionPassword
    if row['EVOInstance'] == 'test':
        instanceURL = testInstanceURL
        fusionUserName = testFusionUserName
        fusionPassword = testFusionPassword
    elif row['EVOInstance'] == 'dev1':
        instanceURL    = dev1InstanceURL
        fusionUserName = dev1FusionUserName
        fusionPassword = dev1FusionPassword        
    elif row['EVOInstance'] == 'dev2':
        instanceURL    = dev2InstanceURL
        fusionUserName = dev2FusionUserName
        fusionPassword = dev2FusionPassword    
    elif row['EVOInstance'] == 'dev3':
        instanceURL    = dev3InstanceURL
        fusionUserName = dev3FusionUserName
        fusionPassword = dev3FusionPassword    
    elif row['EVOInstance'] == 'dev4':
        instanceURL    = dev4InstanceURL
        fusionUserName = dev4FusionUserName
        fusionPassword = dev4FusionPassword
    elif row['EVOInstance'] == 'dev5':
        instanceURL    = dev5InstanceURL
        fusionUserName = dev5FusionUserName
        fusionPassword = dev5FusionPassword
    elif row['EVOInstance'] == 'dev6':
        instanceURL    = dev6InstanceURL
        fusionUserName = dev6FusionUserName
        fusionPassword = dev6FusionPassword
    elif row['EVOInstance'] == 'dev7':
        instanceURL    = dev7InstanceURL
        fusionUserName = dev7FusionUserName
        fusionPassword = dev7FusionPassword

    url = instanceURL + "/xmlpserver/services/v2/CatalogService"
    
    errormsg = ''
    sourceRowCount = 0
    redshift_row_count = 0

    if row['LastUpdateDate'] != today:
        print('In main processing loop - Full Data Load Schedule reports ')
        print('Instance URL: ',instanceURL)

        tbl = row['TableName1']
        loadType = row['DailyLoadType']
        loadtype = loadType 
        tablename = tbl
        print('tablename: ', tbl)
        redshift_tablename = tbl
        # if loadType in ['Validation','MetaData']:
        #     redshift_tablename = row['ValidationRedshiftTableName']
        tableUID = row['TableName']
        
        SchemaName = row['SchemaName']

        upsertkey = row['primarykey']
        returnfields = row['returnFields']
        FullToCDCFlag = row['FullToCDCFlag']
        fullToCDCFlag = FullToCDCFlag
        print('Full to CDC Flag: ', FullToCDCFlag)
        lastupdatedate = row['LastUpdateDate']
        #batchesFlag = row['BatchesFlag']
        scheduleFlag = row['Schedule']
        scheduleTruncateFlag = row['ScheduleTruncateFlag']
        scheduleBeginDate = row['ScheduleBeginDate']
        scheduleEndDate = row['ScheduleEndDate']

        
        
        
        ### get total record counts from tablename in EVO ########
        recordCounts = recordCnts(tbl, loadType)
        print('RecordCounts: ',recordCounts)
        
        with open('/tmp/counts.csv', 'w', encoding="utf-8") as output_file:
            output_file.write(recordCounts)
        counts_df = pd.read_csv('/tmp/counts.csv',dtype=str)
        
        counts_df = counts_df.replace(np.nan, '', regex=True)
        counts_df = counts_df.replace('nan', '', regex=True)
        counts_df = counts_df.replace('null', '', regex=True)
        
        recordcount = 0
        minLUD = ''
        maxLUD = ''
        try:
            for index, row in counts_df.iterrows():
                recordcount = row['CNT']
                minLUD = row['MIN_LUD']
                maxLUD = row['MAX_LUD']
        except Exception as e:
            recordcount = 0
            minLUD = ''
            maxLUD = ''
        print(recordcount)
        print(minLUD)
        print(maxLUD)
        
        beginDate = minLUD[:10]
        endDate = maxLUD[:10]
        print(beginDate)
        print(endDate)
        
        # get the actual day number
        day_num = now.strftime("%d")
        if minLUD == '':
            begin_yearmonth = 202209
        else:
            begin_yearmonth = int(minLUD[:7].replace('-',''))
        current_yearmonth = int(now.strftime("%Y%m"))
        str_begin_yearmonth = str(begin_yearmonth)
        str_current_yearmonth = str(current_yearmonth)
        count = 0
        periods_list = []
        periods_list.append(str_begin_yearmonth)
        print(begin_yearmonth)
        print(current_yearmonth)
        while (begin_yearmonth < current_yearmonth):
            # newdate = np.datetime64(str_begin_yearmonth) + np.timedelta64(1, 'M')
            y = str(begin_yearmonth)[:4]
            m = str(begin_yearmonth)[-2:]
            d = 1
            newdate = date(int(y), int(m), d) + relativedelta(months=1)
            begin_yearmonth = int(str(newdate).replace('-', '')[:6])
            str_begin_yearmonth = str(begin_yearmonth)
            periods_list.append(str_begin_yearmonth)
        print('Schedule Flag: ', scheduleFlag)
        if scheduleFlag == 'Batches':
            begin_date_list = []
            end_date_list = []
            print('Inside Batches Loop')
            for x in periods_list:
                year = x[:4]
                # yr = int(x.partition('-')[0])
                month = x[-2:]
                # mm = int(x.partition('-')[2])
                day = 1
                dd = str(1).zfill(2)
                # print(year)
                # print(month)
            
                #begin_date = datetime(int(year), int(month), day)
                begin_date = year + '-' + month + '-' + dd
            
            
                # print(begin_date)
                begin_date_list.append(begin_date)
                # print(day)
                last_date_month = datetime(int(year), int(month), day)
                current_month_last_date = last_date_month + relativedelta(day=31)
                current_month_last_date = current_month_last_date.strftime("%Y-%m-%d")
            
                end_date_list.append(current_month_last_date)
        elif scheduleFlag == 'true' or scheduleFlag == 'Full':
            begin_date_list = []
            end_date_list = []
            begin_date_list.append(beginDate)
            end_date_list.append(endDate)
        elif scheduleFlag == 'Manual':
            begin_date_list = []
            end_date_list = []
            begin_date_list.append(scheduleBeginDate)
            end_date_list.append(scheduleEndDate)
        
        print(begin_date_list)
        print(end_date_list)
        
        #Redshift_Target.tbl_Truncate(SchemaName, tablename)
        # if loadType == 'Validation' or loadType == 'Special_Full' or loadType == 'MetaData':  
        #     begin_date_list = []
        #     end_date_list = []
        #     begin_date_list.append(maxLUD[:10])
        #     end_date_list.append(maxLUD[:10])
        # begin_date_list = ['2023-03-08']
        # end_date_list = ['2023-03-08']
        cnt = 0
        for begin_date in begin_date_list:
            
            
            if scheduleFlag == 'Manual':
                if scheduleTruncateFlag == 'true':
                    #if loadType == 'MetaData':
                    Redshift_Target.tbl_Truncate(SchemaName, redshift_tablename)
                    #else:
                    #    Redshift_Target.tbl_Truncate(SchemaName, tablename)
            elif cnt == 0:
                #if loadType == 'MetaData':
                Redshift_Target.tbl_Truncate(SchemaName, redshift_tablename)
                #else:
                #    Redshift_Target.tbl_Truncate(SchemaName, tablename)

            end_date = end_date_list[cnt]
            cnt += 1
            errormsg = ''
        
            inputSQLtxt = ""

            ####################################################################################
            ####
            ####   This is files that have issues and have to be manually tweaked. By chnaging the Schedule = true
            ####   in EVO_BaseTables_Data_Controller. Also make sure the lastUpdateDtae in the copntroller table for the 
            ####   specific file does not equal todays date or is empty.
            ####
            ####   You will need to to change the date structure by first running command in SQL ConnectBISplash to the table 
            ####   in question and run (change tablename): 
            ####
            ####    select trunc(last_update_date), count(1) as cnt from egp_system_items_b
            ####    group by trunc(last_update_date)
            ####    order by trunc(last_update_date) 
            ####
            ####    These result will tell you how to break up the dates by code.
            ####    ##inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) = (to_date(''2023-03-08'',''YYYY-MM-DD''))" 
            ####    #inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) <> (to_date(''2023-03-08'',''YYYY-MM-DD''))" 
            ####    #inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) > (to_date(''2023-03-08'',''YYYY-MM-DD''))" 
            ####    After results download file from oracle reports, find the reports click more , then history
            ####    you will see your report, start download by right clicking xml data download icon.Rename if you know
            ####    you will have multiple files with 1 or _1 , 2,_2, etc. EX:FSTreport_egp_system_items_b.xml to FSTreport_egp_system_items_b1.xml,
            ####    FSTreport_egp_system_items.xml  to FSTreport_egp_system_items_1.xml.
            ####
            ####    Again this is for manual cant process files standard way. After you run this to break apart the report in question
            ####    download files, rename as described above, change the names in Convert XML_File_To_CSV_Load_Redshift and in BIP Extract Load Manual CSV File to
            ####    Redshift. After chnaging name run  Convert XML_File_To_CSV_Load_Redshift(may take awhile) and then move csv file to S3// EVO/TblData
            ####    /ManualFullTableLoads and run Extract Load Manual CSV File to Redshift. While this is occuring you can run additional reports while
            ####    waiting and doing the process over with 2,_2 file and so on. 
            ####################################################################################
            print("Extract data from EVO SRC for Base Table: " + tbl)
            
            if loadType == 'Validation' or loadType == 'Special_Full':
                inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl
            # elif loadType == 'MetaData':
            #     inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl + " WHERE OWNER = ''FUSION''"               
            else:
                inputSQLtxt = "SELECT * FROM " + tbl
            
        
            inputSQLtxt = inputSQLtxt + " WHERE TRUNC(LAST_UPDATE_DATE) >= (to_date(''" + begin_date + "'',''YYYY-MM-DD''))"
            inputSQLtxt = inputSQLtxt + " AND TRUNC(LAST_UPDATE_DATE) <= (to_date(''" + end_date + "'',''YYYY-MM-DD''))"
 
            
            ##inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) = (to_date(''2023-03-08'',''YYYY-MM-DD''))" 
            #inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) <> (to_date(''2023-03-08'',''YYYY-MM-DD''))" 
            #inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) > (to_date(''2023-03-08'',''YYYY-MM-DD''))" 
            # Above In Dev2 worked for egp_system_items for itemd greater than 3-8-2023  in this example dates may need to vary 3-8 returned 389,763 rows
            #inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) <> trunc(to_date(current_date - 35))"
            #inputSQLtxt = inputSQLtxt + " where trunc(last_update_date) = trunc(to_date(current_date - 16))"
            #inputSQLtxt = inputSQLtxt + " and trunc(last_update_date,''HH24'') in (to_date(''2023-03-08 15'', ''YYYY-MM-DD HH24''), to_date(''2023-03-08 15'', ''YYYY-MM-DD HH24''))"
            # and trunc(last_update_date,'HH24') = to_date('2023-03-08 13', 'YYYY-MM-DD HH24')
            
            print(inputSQLtxt)
    
            folderPath = '/Custom/CloudTools/V4'
            baseName = "FSTReportCSV"
            dm_baseName = "FusionSQLToolDM"
            reportName = baseName + ".xdo"
            reportPath = folderPath + "/" + reportName
            xdmName = dm_baseName
            dmPath = folderPath + "/" + xdmName + '.xdm'               
    
            print(reportPath)
    
            # jobID = BIP_Main.CSVscheduleReport(instanceURL, reportPath, fusionUserName, fusionPassword)
            # print(jobID) 
            
            # create DM
            result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            
            # run report to return csv file
            #result = BIP_Main.CSVrunReport(instanceURL, folderPath, reportPath,fusionUserName, fusionPassword)
    
            jobID = BIP_Main.CSVscheduleReport(instanceURL, tbl, reportPath, fusionUserName, fusionPassword)
            print('Job ID: ', jobID)
            
            time.sleep(60)
            try:
                
                result = BIP_Main.CSVscheduleReportStatus(instanceURL, jobID, fusionUserName, fusionPassword)
                print(result)
                status = result[0]
                print(status)
                statusDetail = result[1]
                print(statusDetail)
            except Exeption as  e:
                print(e)
            time.sleep(60)
            
            while status == 'Running':
                try:
                    
                    result = BIP_Main.CSVscheduleReportStatus(instanceURL, jobID, fusionUserName, fusionPassword)
                    print(result)
                    status = result[0]
                    print(status)
                    statusDetail = result[1]
                    print(statusDetail)
                except Exeption as  e:
                    print(e)
                time.sleep(60)
                
            print('Status: ', status)
            if status == 'Success':
                outputID = BIP_Main.CSVReportOutputInfo(instanceURL, jobID, fusionUserName, fusionPassword)
                print(outputID)
                fileID = BIP_Main.CSVfileLocation(instanceURL, outputID, fusionUserName, fusionPassword)
                print(fileID)
                result = BIP_Main.CSVdownloadReportDataChunk(instanceURL, fileID, fusionUserName, fusionPassword)
                print(len(result))
                
                csv_file = "/tmp/" + tbl + ".csv"
                
                with open(csv_file, 'w', encoding="utf-8") as output_file:
                    output_file.write(result)
                df = pd.read_csv(csv_file,dtype=str)
                
                now = datetime.now()
                filedatetime = now.strftime("%Y%m%d")
    
                print(filedatetime)
                updatedatetime = now.strftime("%Y-%m-%d %H:%M:%S")
                print(updatedatetime)
                
                csv_file = "/tmp/" + tbl + ".csv"
                df = df.replace(np.nan, '', regex=True)
                df = df.replace('null', '', regex=True)
                df.to_csv(csv_file, index=False)
                if loadType == 'Full' or loadType == 'Special_Full':
                    prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                elif loadType == 'MetaData':
                    prefix = "EVO/TblData/MetaData/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                #elif loadType == 'CDC':
                #    prefix = "EVO/TblData/CDC/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                
                filename = prefix + tbl +  "_" + str(filedatetime) + ".csv"
                
                bucket.upload_file(csv_file, filename, extra_args)
                print("Extract Data for " + tbl + " Completed")
                tablename = tbl
                FileName = "/" + filename
                row_count = len(df)
                print('Number of Records Retrieved from Source: ', row_count)
            
                
                redshift_row_count = 0
                recordsprocessed = 0
                redshiftbeginrowscount = 0
                redshift_error = 'false'
                redshift_error_message = ''            
                
                # Run Redshift Target Objects to load Data.
                try:
                    updatenow = datetime.now()
                    updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                    
                    #Redshift_Target.tbl_Truncate(SchemaName, tablename)
                    #if loadType == 'MetaData':
                    
                    result = Redshift_Target.tbl_Append(SchemaName, redshift_tablename, FileName)
                    
                    #else:
                    #result = Redshift_Target.tbl_Append(SchemaName, tablename, FileName)
                    #print(result[0])
                    
                    redshift_row_count = result[0]
                    print(redshift_row_count)
                    redshift_error = result[1]
                    redshift_error_message = result[2]
                    print(redshift_error)
                    print(redshift_error_message)
                    
    
                    print('Source Recordcnt: ', recordcount)
                    errormsg = ''
                except Exception as e:
                    print(e)
    
            else:
                print('Status not equal to Success routine ')
                
            recordcount_diff = int(recordcount) - int(redshift_row_count)
            print('Record Counts Different: ', recordcount_diff)
            print(FullToCDCFlag)
            if recordcount_diff == 0 and FullToCDCFlag == 'true':
            #if int(recordcount) == redshift_row_count and FullToCDCFlag == 'true':
                print('in source and redshift counts are equal routine')
                print(loadtype)
    
                if loadType == 'Full': 
                    newloadType = 'CDC'
                elif loadType == 'Special_Full':
                    newloadType = 'Special_CDC'
            else:
                newloadType = loadType
            print(newloadType)
    
    
            updatenow = datetime.now()
            updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")        
           
            if redshift_error == 'true':
                LUD = lastupdatedate
            else:
                LUD = updatedatetime
            
            print('at DDB table update for success')                           
            try:
                update = table.update_item(
                    Key={
                        'TableName': tableUID
                    },
                    UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10, redshifterror = :val11, redshifterrormessage = :val12, redshiftInserts = :val13, redshiftUpdates = :val14, Schedule = :val15',
                    ExpressionAttributeValues={
                        ':val1': LUD,
                        ':val2': recordcount,
                        ':val3': redshift_row_count,
                        ':val4': errormsg,
                        ':val5': updatedatetime,
                        ':val6': minLUD,
                        ':val7': maxLUD,
                        ':val8': 'true',
                        ':val9': newloadType,
                        ':val10': 'false',
                        ':val11': '',
                        ':val12': '',
                        ':val13': '',
                        ':val14': '',
                        ':val15': 'false'
                        # ':val11': redshift_error,
                        # ':val12': redshift_error_message,
                        # ':val13': redshift_inserts,
                        # ':val14': redshift_updates
                    }
                )
            except Exception as e:
                print('Error on DDB update for success: ', e)
#############################################################################################################################################################
################################### end of scheduled process routine ########################################################################################
 
 
 ####################### Metadata_Cols - all_tab_s columns or primarykeys tables processing ##
 #######################################################################
#### process items that are scheduled with Full Daily Data Loads #####
####################Currently no items are being scheduled ###########
####### the shceduled process may be for future use ##################


table = ddb.Table(ddb_tablename)
response = table.scan()
data = response['Items']
listdf = pd.DataFrame(data)

fileprefix = 'EVO'+ '/'+ 'TblSQL'+ '/'
print(' At schedule process routine for MetaData')

scheduledf = listdf
scheduledf = scheduledf.loc[scheduledf['TableCreatedFlag'].isin(['true'])]
scheduledf = scheduledf.loc[scheduledf['ActiveDownloadFlag'].isin(['true'])]
scheduledf = scheduledf.loc[scheduledf['DailyLoadType'].isin(['MetaData_Cols','MetaData_primarykeys'])]

print(scheduledf)
print('priority :', priority )
print('EVOInstance: ', EVOInstance)
if priority != 'All':
    scheduledf = scheduledf.loc[scheduledf['priority'].isin([int(priority)])]
if EVOInstance != 'All':
    scheduledf = scheduledf.loc[scheduledf['EVOInstance'].isin([EVOInstance])]
if priority != 'All':
    listdf = listdf.loc[listdf['priority'].isin([int(priority)])]

scheduledf = scheduledf.sort_values(['priority','sortOrder'])
print(scheduledf)

tablenamelist = scheduledf['TableName1'].tolist()

for index, row in scheduledf.iterrows():
    print('in loop row: ', row)
    if row['EVOInstance'] == 'prod':
        instanceURL = prodInstanceURL
        fusionUserName = prodFusionUserName
        fusionPassword = prodFusionPassword
    if row['EVOInstance'] == 'test':
        instanceURL = testInstanceURL
        fusionUserName = testFusionUserName
        fusionPassword = testFusionPassword
    elif row['EVOInstance'] == 'dev1':
        instanceURL    = dev1InstanceURL
        fusionUserName = dev1FusionUserName
        fusionPassword = dev1FusionPassword        
    elif row['EVOInstance'] == 'dev2':
        instanceURL    = dev2InstanceURL
        fusionUserName = dev2FusionUserName
        fusionPassword = dev2FusionPassword    
    elif row['EVOInstance'] == 'dev3':
        instanceURL    = dev3InstanceURL
        fusionUserName = dev3FusionUserName
        fusionPassword = dev3FusionPassword    
    elif row['EVOInstance'] == 'dev4':
        instanceURL    = dev4InstanceURL
        fusionUserName = dev4FusionUserName
        fusionPassword = dev4FusionPassword
    elif row['EVOInstance'] == 'dev5':
        instanceURL    = dev5InstanceURL
        fusionUserName = dev5FusionUserName
        fusionPassword = dev5FusionPassword
    elif row['EVOInstance'] == 'dev6':
        instanceURL    = dev6InstanceURL
        fusionUserName = dev6FusionUserName
        fusionPassword = dev6FusionPassword
    elif row['EVOInstance'] == 'dev7':
        instanceURL    = dev7InstanceURL
        fusionUserName = dev7FusionUserName
        fusionPassword = dev7FusionPassword

    url = instanceURL + "/xmlpserver/services/v2/CatalogService"
    
    errormsg = ''
    sourceRowCount = 0
    redshift_row_count = 0

    if row['LastUpdateDate'] != today:
        print('In main processing loop - Full Data Load Schedule reports ')
        print('Instance URL: ',instanceURL)

        tbl = row['TableName1']
        loadType = row['DailyLoadType']
        loadtype = loadType 
        tablename = tbl
        print('tablename: ', tbl)
        SchemaName = row['SchemaName']
        
        #redshift_tablename = row['ValidationRedshiftTableName']
        redshift_tablename = tbl + '_' + EVOInstance + '_' + evoCurrentRelease
        
        emptyDF = pd.DataFrame()
        tableExistsFlag = Redshift_Target.tbl_Exists(emptyDF,SchemaName, redshift_tablename, 'false')
        if tableExistsFlag[0] != 'True':
            createFlag = Redshift_Target.tbl_CreateTbl_MetaDataReleaseTable(SchemaName, redshift_tablename)
            if createFlag == 'True':
                print('Successful creation of Redshift metadata release table: ', redshift_tablename)
            else:
                print('Faled to create Redshift metadata release table: ', redshift_tablename)   
        
        tableUID = row['TableName']
        


        upsertkey = row['primarykey']
        returnfields = row['returnFields']
        FullToCDCFlag = row['FullToCDCFlag']
        fullToCDCFlag = FullToCDCFlag
        print('Full to CDC Flag: ', FullToCDCFlag)
        lastupdatedate = row['LastUpdateDate']
        #batchesFlag = row['BatchesFlag']
        scheduleFlag = row['Schedule']
        scheduleTruncateFlag = row['ScheduleTruncateFlag']
        scheduleBeginDate = row['ScheduleBeginDate']
        scheduleEndDate = row['ScheduleEndDate']

        
        
        if tbl != 'primarykeys':
            ### get total record counts from tablename in EVO ########
            recordCounts = recordCnts(tbl, loadType)
            print('RecordCounts: ',recordCounts)
            
            with open('/tmp/counts.csv', 'w', encoding="utf-8") as output_file:
                output_file.write(recordCounts)
            counts_df = pd.read_csv('/tmp/counts.csv',dtype=str)
            
            counts_df = counts_df.replace(np.nan, '', regex=True)
            counts_df = counts_df.replace('nan', '', regex=True)
            counts_df = counts_df.replace('null', '', regex=True)
            
            recordcount = 0
            minLUD = ''
            maxLUD = ''
            try:
                for index, row in counts_df.iterrows():
                    recordcount = row['CNT']
                    minLUD = row['MIN_LUD']
                    maxLUD = row['MAX_LUD']
            except Exception as e:
                recordcount = 0
                minLUD = ''
                maxLUD = ''
        else:
            recordcount = 0
            minLUD = ''
            maxLUD = ''            
        print(recordcount)
        print(minLUD)
        print(maxLUD)
        



        for x in range(0,2):
            
            
            if x == 0:
                Redshift_Target.tbl_Truncate(SchemaName, redshift_tablename)
                
            errormsg = ''
        
            inputSQLtxt = ""

           
            ####################################################################################
            print("Extract data from EVO SRC for Base Table: " + tbl)
            
            if tbl == 'primarykeys':
                inputSQLtxt = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner FROM all_constraints cons, all_cons_columns cols WHERE"
                inputSQLtxt = inputSQLtxt + " cons.constraint_type = ''P'' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner"
                inputSQLtxt = inputSQLtxt + " AND cons.owner = ''FUSION'' "
            else:
                inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl
                

                if x == 0:
                    
                    inputSQLtxt = inputSQLtxt + " WHERE OWNER = ''FUSION'' AND DATA_TYPE = ''VARCHAR2'' AND TABLE_NAME = UPPER(TABLE_NAME)"
                    #inputSQLtxt = inputSQLtxt + " AND TABLE_NAME <> ''HZ_ORGANIZATION_PROFILES''"

                elif x == 1:
                    inputSQLtxt = inputSQLtxt + " WHERE OWNER = ''FUSION'' AND DATA_TYPE != ''VARCHAR2'' AND TABLE_NAME = UPPER(TABLE_NAME)"
                    #inputSQLtxt = inputSQLtxt + " AND TABLE_NAME <> ''HZ_ORGANIZATION_PROFILES''"
                    
                # elif x == 2:
                #     inputSQLtxt = inputSQLtxt + " WHERE OWNER = ''FUSION'' AND TABLE_NAME = LOWER(TABLE_NAME)"
                #     inputSQLtxt = inputSQLtxt + " AND TABLE_NAME = ''hz_organization_profiles''"

 


            print(inputSQLtxt)
    
            if x != 0 and tbl == 'primarykeys':
                print('do nothing primarykeys only requires executeion one time')
            else:
                folderPath = '/Custom/CloudTools/V4'
                baseName = "FSTReportCSV"
                dm_baseName = "FusionSQLToolDM"
                reportName = baseName + ".xdo"
                reportPath = folderPath + "/" + reportName
                xdmName = dm_baseName
                dmPath = folderPath + "/" + xdmName + '.xdm'               
        
                print(reportPath)
        
                # jobID = BIP_Main.CSVscheduleReport(instanceURL, reportPath, fusionUserName, fusionPassword)
                # print(jobID) 
                
                # create DM
                result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
                result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
                
                # run report to return csv file
                #result = BIP_Main.CSVrunReport(instanceURL, folderPath, reportPath,fusionUserName, fusionPassword)
        
                jobID = BIP_Main.CSVscheduleReport(instanceURL, tbl, reportPath, fusionUserName, fusionPassword)
                print('Job ID: ', jobID)
                
                time.sleep(60)
                try:
                    
                    result = BIP_Main.CSVscheduleReportStatus(instanceURL, jobID, fusionUserName, fusionPassword)
                    print(result)
                    status = result[0]
                    print(status)
                    statusDetail = result[1]
                    print(statusDetail)
                except Exeption as  e:
                    print(e)
                time.sleep(60)
                
                while status == 'Running':
                    try:
                        
                        result = BIP_Main.CSVscheduleReportStatus(instanceURL, jobID, fusionUserName, fusionPassword)
                        print(result)
                        status = result[0]
                        print(status)
                        statusDetail = result[1]
                        print(statusDetail)
                    except Exeption as  e:
                        print(e)
                    time.sleep(60)
                    
                print('Status: ', status)
                if status == 'Success':
                    outputID = BIP_Main.CSVReportOutputInfo(instanceURL, jobID, fusionUserName, fusionPassword)
                    print(outputID)
                    fileID = BIP_Main.CSVfileLocation(instanceURL, outputID, fusionUserName, fusionPassword)
                    print(fileID)
                    result = BIP_Main.CSVdownloadReportDataChunk(instanceURL, fileID, fusionUserName, fusionPassword)
                    print(len(result))
                    
                    csv_file = "/tmp/" + tbl + ".csv"
                    
                    with open(csv_file, 'w', encoding="utf-8") as output_file:
                        output_file.write(result)
                    df = pd.read_csv(csv_file,dtype=str)
                    df = df.replace(np.nan, '', regex=True)
                    df = df.replace('null', '', regex=True)
                    now = datetime.now()
                    filedatetime = now.strftime("%Y%m%d")
        
                    print(filedatetime)
                    updatedatetime = now.strftime("%Y-%m-%d %H:%M:%S")
                    print(updatedatetime)
                    
                    csv_file = "/tmp/" + tbl + ".csv"
                    df.to_csv(csv_file, index=False)
    
                    prefix = "EVO/TblData/MetaData/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                    
                    filename = prefix + tbl +  "_" + str(filedatetime) + ".csv"
                    
                    bucket.upload_file(csv_file, filename, extra_args)
                    print("Extract Data for " + tbl + " Completed")
                    tablename = tbl
                    FileName = "/" + filename
                    row_count = len(df)
                    print('Number of Records Retrieved from Source: ', row_count)
                
                    
                    redshift_row_count = 0
                    recordsprocessed = 0
                    redshiftbeginrowscount = 0
                    redshift_error = 'false'
                    redshift_error_message = ''            
                    
                    # Run Redshift Target Objects to load Data.
                    try:
                        updatenow = datetime.now()
                        updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                        
                        #Redshift_Target.tbl_Truncate(SchemaName, tablename)
                        #if loadType == 'MetaData':
                        
                        result = Redshift_Target.tbl_Append(SchemaName, redshift_tablename, FileName)
                        
                        #else:
                        #result = Redshift_Target.tbl_Append(SchemaName, tablename, FileName)
                        #print(result[0])
                        
                        redshift_row_count = result[0]
                        print(redshift_row_count)
                        redshift_error = result[1]
                        redshift_error_message = result[2]
                        print(redshift_error)
                        print(redshift_error_message)
                        
        
                        print('Source Recordcnt: ', recordcount)
                        errormsg = ''
                    except Exception as e:
                        print(e)
        
                else:
                    print('Status not equal to Success routine ')
                    
                recordcount_diff = int(recordcount) - int(redshift_row_count)
                print('Record Counts Different: ', recordcount_diff)
                print(FullToCDCFlag)
                if recordcount_diff == 0 and FullToCDCFlag == 'true':
                #if int(recordcount) == redshift_row_count and FullToCDCFlag == 'true':
                    print('in source and redshift counts are equal routine')
                    print(loadtype)
        
                newloadType = loadType
                
        
                updatenow = datetime.now()
                updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")        
               
                if redshift_error == 'true':
                    LUD = lastupdatedate
                else:
                    LUD = updatedatetime
                
                print('at DDB table update for success')                           
                try:
                    update = table.update_item(
                        Key={
                            'TableName': tableUID
                        },
                        UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10, redshifterror = :val11, redshifterrormessage = :val12, redshiftInserts = :val13, redshiftUpdates = :val14, Schedule = :val15',
                        ExpressionAttributeValues={
                            ':val1': LUD,
                            ':val2': recordcount,
                            ':val3': redshift_row_count,
                            ':val4': errormsg,
                            ':val5': updatedatetime,
                            ':val6': minLUD,
                            ':val7': maxLUD,
                            ':val8': 'true',
                            ':val9': newloadType,
                            ':val10': 'false',
                            ':val11': '',
                            ':val12': '',
                            ':val13': '',
                            ':val14': '',
                            ':val15': 'false'
                            # ':val11': redshift_error,
                            # ':val12': redshift_error_message,
                            # ':val13': redshift_inserts,
                            # ':val14': redshift_updates
                        }
                    )
                except Exception as e:
                    print('Error on DDB update for success: ', e)
#############################################################################################################################################################
################################### end of scheduled process routine for metadata tables ########################################################################################


#############################################################################################################################################################
################################### Freeform SQL Type #################################################################        
#############################################################################################################################################################

table = ddb.Table(ddb_tablename)
response = table.scan()
data = response['Items']
listdf = pd.DataFrame(data)

fileprefix = 'EVO'+ '/'+ 'TblSQL'+ '/'

listdf = listdf.loc[listdf['Frequency'].isin(['Daily'])]
listdf = listdf.loc[listdf['ActiveDownloadFlag'].isin(['true'])]
#listdf = listdf.loc[listdf['TableCreatedFlag'].isin(['true'])]
listdf = listdf.loc[listdf['DailyLoadType'].isin(['FreeForm'])]
#listdf = listdf.loc[listdf['Schedule'].isin(['false'])]
if priority != 'All':
    listdf = listdf.loc[listdf['priority'].isin([int(priority)])]
if EVOInstance != 'All':
    listdf = listdf.loc[listdf['EVOInstance'].isin([EVOInstance])]
listdf = listdf.sort_values(['priority','sortOrder'])

print('In FreeForm SQL Routine')
print(listdf)
redshift_error = 'false'
redshift_error_message = ''     

dmPath = folderPath + "/FusionSQLToolDM.xdm"
reportPath = folderPath + "/FSTreport.xdo"
tablenamelist = listdf['TableName1'].tolist()
for index, row in listdf.iterrows():
    print(row)
    ######  determine which EVO Instance to create and run the reports based upon entry in dynamodb item ###
    ###### In prod this should always be prod more than likely ##########
    if row['EVOInstance'] == 'prod':
        instanceURL = prodInstanceURL
        fusionUserName = prodFusionUserName
        fusionPassword = prodFusionPassword
    if row['EVOInstance'] == 'test':
        instanceURL = testInstanceURL
        fusionUserName = testFusionUserName
        fusionPassword = testFusionPassword
    elif row['EVOInstance'] == 'dev1':
        instanceURL    = dev1InstanceURL
        fusionUserName = dev1FusionUserName
        fusionPassword = dev1FusionPassword    
    elif row['EVOInstance'] == 'dev2':
        instanceURL    = dev2InstanceURL
        fusionUserName = dev2FusionUserName
        fusionPassword = dev2FusionPassword    
    elif row['EVOInstance'] == 'dev3':
        instanceURL    = dev3InstanceURL
        fusionUserName = dev3FusionUserName
        fusionPassword = dev3FusionPassword    
    elif row['EVOInstance'] == 'dev4':
        instanceURL    = dev4InstanceURL
        fusionUserName = dev4FusionUserName
        fusionPassword = dev4FusionPassword
    elif row['EVOInstance'] == 'dev5':
        instanceURL    = dev5InstanceURL
        fusionUserName = dev5FusionUserName
        fusionPassword = dev5FusionPassword
    elif row['EVOInstance'] == 'dev6':
        instanceURL    = dev6InstanceURL
        fusionUserName = dev6FusionUserName
        fusionPassword = dev6FusionPassword
    elif row['EVOInstance'] == 'dev7':
        instanceURL    = dev7InstanceURL
        fusionUserName = dev7FusionUserName
        fusionPassword = dev7FusionPassword


    url = instanceURL + "/xmlpserver/services/v2/CatalogService"
    
    SchemaName = row['SchemaName']
    loadType = row['DailyLoadType']
    upsertkey = row['primarykey']
    tbl = row['TableName1']
    tablename = tbl
    redshift_tablename = tbl
    # if loadType == 'Validation' or loadType == 'MetaData':
    #     redshift_tablename = row['ValidationRedshiftTableName']
    tableUID = row['TableName']
    
    # returnfields = row['returnFields']
    
    fullToCDCFlag = row['FullToCDCFlag']
    
    freeformSQL = row['zzFreeFormSQL']
    freeformTruncateFlag = row['zzFreeformTruncateFlag']
    freeformUpdateMethod = row['zzFreeformUpdateMethod']
    
    errormsg = ''
    sourceRowCount = 0
    redshift_row_count = 0
    redshift_inserts = 0
    redshift_updates = 0
    redshift_error = 'false'
    redshift_error_message = ''
    newloadType = loadType
    redshiftbeginrowscount = 0
    
    recLastUpdateDate = row['LastUpdateDate'][0:10]
    #print(recLastUpdateDate)
    
    if recLastUpdateDate != today:

        #if row['BatchesFlag'] != 'true' or loadType == 'CDC' or loadType == 'Validation' or loadType == 'MetaData' or loadType == 'Special_Full' or loadType == 'Special_CDC':
        if 1 == 1:    
            print('In main processing loop - non batches')
            print('Instance URL: ',instanceURL)
            # getRowCountEVOSource(row)
            print('tablename: ', tbl)

            # ### get total record counts from tablename in EVO ########
            # recordCounts = recordCnts(tbl, loadType)
            # print('RecordCounts: ',recordCounts)
            
            # with open('/tmp/counts.csv', 'w', encoding="utf-8") as output_file:
            #     output_file.write(recordCounts)
            # counts_df = pd.read_csv('/tmp/counts.csv',dtype=str)
            
            # counts_df = counts_df.replace(np.nan, '', regex=True)
            # counts_df = counts_df.replace('nan', '', regex=True)
            # counts_df = counts_df.replace('null', '', regex=True)
            
            recordcount = 0
            minLUD = ''
            maxLUD = ''
            # try:
            #     for index, row in counts_df.iterrows():
            #         recordcount = row['CNT']
            #         minLUD = row['MIN_LUD']
            #         maxLUD = row['MAX_LUD']
            # except Exception as e:
            #     recordcount = 0
            #     minLUD = ''
            #     maxLUD = ''
            # print(recordcount)
            # print(minLUD)
            # print(maxLUD)


            # inputSQLtxt = ""
            # # BIP_Main.createDataModel(folderPath, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            # print("Extract data from EVO SRC for Base Table: " + tbl)
            # inputSQLtxt = "SELECT * FROM " + tbl
            # if loadType == 'CDC':
            #     inputSQLtxt = inputSQLtxt + " WHERE ( TRUNC(LAST_UPDATE_DATE) = TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") OR TRUNC(CREATION_DATE) = TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") )"
            # elif loadType == 'Validation' or loadType == 'Special_Full' or loadType == 'Special_CDC':
            #     inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl
            #     if loadType == 'Special_CDC':
            #         inputSQLtxt = inputSQLtxt + " WHERE ( TRUNC(LAST_UPDATE_DATE) = TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") OR TRUNC(CREATION_DATE) = TO_DATE(CURRENT_DATE - " + str(start_date_days) + ") )"
            # elif loadType == 'MetaData':
            #     #inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl + " WHERE OWNER = ''FUSION''" 
            #     inputSQLtxt = "SELECT " + returnfields + " FROM " + tbl    
            inputSQLtxt = freeformSQL   
            # inputSQLtxt = "select distinct hou.ORGANIZATION_ID division_id, mp.organization_code, hlv.region_2, hou.name from HR_ALL_ORGANIZATION_UNITS_X HOU,"
            # inputSQLtxt = inputSQLtxt + " HR_ORG_UNIT_CLASSIFICATIONS_X  HOI1, INV_ORG_PARAMETERS MP, HR_LOCATIONS_ALL_F_VL hlv"
            # inputSQLtxt = inputSQLtxt + " WHERE HOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID AND HOU.ORGANIZATION_ID = MP.ORGANIZATION_ID"
            # inputSQLtxt = inputSQLtxt + " and hlv.location_id(+) = HOU.location_id"
            ##AND HOI1.CLASSIFICATION_CODE = ''INV'' and organization_code not in ''MSR'' and hlv.region_2 is not null"
            #inputSQLtxt = "select distinct hou.ORGANIZATION_ID division_id, mp.organization_code, hlv.region_2, hou.name from HR_ALL_ORGANIZATION_UNITS_X HOU,  HR_ORG_UNIT_CLASSIFICATIONS_X HOI1, INV_ORG_PARAMETERS MP, HR_LOCATIONS_ALL_F_VL hlv WHERE HOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID AND HOU.ORGANIZATION_ID = MP.ORGANIZATION_ID and hlv.location_id(+) = HOU.location_id AND HOI1.CLASSIFICATION_CODE = ''INV'' and organization_code not in ''MSR'' and hlv.region_2 is not null"

            print(inputSQLtxt)

            # folderPath = '/Custom/CloudTools/V4'
            baseName = "FSTReportCSV"
            dm_baseName = "FusionSQLToolDM"
            reportName = baseName + ".xdo"
            reportPath = folderPath + "/" + reportName
            xdmName = dm_baseName
            dmPath = folderPath + "/" + xdmName + '.xdm'
    
            
            #####  Start of BIP Report Creation and Data Extraction ########
    
            
            now = datetime.now()
            #filedatetime = now.strftime("%Y%m%d")
            filedatetime = now.strftime("%Y%m%d%H%M%S")
            print(filedatetime)
            bipstartdatetime = now.strftime("%Y-%m-%d %H:%M:%S")
            #mm1 = now.strftime("%m")
            #yy1 = now.strftime("%Y")
            #dd1 = now.strftime("%d")
            
            csv_file = "/tmp/" + tbl + ".csv"
            print(str(dd))
            prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
            print(prefix)
            FName = tbl + "_" + str(filedatetime) + ".csv"  
            filename = prefix + FName  

            bucket_zone = rawbucket + '/' + prefix
            
            
            print(bucket_zone)
            
            ############################################################################################
            
            ###### create item in stats table ######
            stats_item = {
                'filename': {'S':FName},
                'project': {'S': 'EVO'},
                'feed': {'S':tablename},
                'year': {'S': yy},
                'month': {'S': mm},
                'day': {'S': dd},
                'evo_rowcount': {'S': str(recordcount)},
                'redshift_rowcount': {'S': 'null'},
                'redshift_processed_records': {'S': 'null'},
                'redshift_inserted_records': {'S': 'null'},
                'redshift_updated_records': {'S': 'null'},
                'bip_start_time': {'S': bipstartdatetime},
                'bip_end_time': {'S':''},
                'redshift_start_time': {'S':''},
                'redshift_end_time': {'S':''},
                'bip_status': {'S': 'In Progress'},
		        'redshift_status': {'S': 'null'},
                'bucket_zone': {'S': bucket_zone},
                'bip_fail_description': {'S': 'None'},
                'schemaname': {'S': SchemaName},
                'tablename': {'S':tableUID},
                'redshift_tablename': {'S':redshift_tablename},
                'bip_fail_description': {'S': 'None'},
                'redshift_fail_description': {'S': 'None'}} 
            
            try:
                ddb_stats_table = 'EVO_BaseTables_Load_Stats'
                response = ddbconn.get_item(
                    TableName=ddb_stats_table,
                    Key={'filename': {'S':FName}})
                if 'Item' in response:
                    item = response['Item']
                else:
                    ddbconn.put_item(TableName=ddb_stats_table,Item=stats_item)
            except Exception as e:
                print(e)
                ddbconn.put_item(TableName=ddb_stats_table,Item=stats_item)
            

            print('at DM create')
             # create DM
            result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            print(result1)
            result1 = BIP_Main.CSVcreateDataModel(folderPath, xdmName, instanceURL, fusionUserName, fusionPassword, inputSQLtxt)
            print(result1)
            # run report to return csv file
            result = BIP_Main.CSVrunReport(instanceURL, folderPath, reportPath,fusionUserName, fusionPassword)

            status = result[0]
            if status != 'Success':
                errormsg = result[1]
                print(errormsg)
                
            else:
                errormsg = ''
            print('Status: ', status)
            newoutput = result[1]
            
            print(len(newoutput))
            if len(newoutput) < 10:
                zeroRowsReturned = 'true'
            else:
                zeroRowsReturned = 'false'
                with open(csv_file, 'w', encoding="utf-8") as output_file:
                    output_file.write(newoutput)
                df = pd.read_csv(csv_file,dtype=str)
                df = df.replace(np.nan, '', regex=True)
                df = df.replace('null', '', regex=True)
                #print(len(df))
            print('zero rows reurned flag: ', zeroRowsReturned)
            
            if status == 'Success':
                
                now = datetime.now()
                bipenddatetime = now.strftime("%Y-%m-%d %H:%M:%S")    

                try:
                    ddbconn.update_item(
                        TableName=ddb_stats_table,
                        Key={"filename": {"S": FName}},
                        AttributeUpdates={"bip_end_time": {"Value": {"S": bipenddatetime}},
                                          "redshift_start_time": {"Value": {"S": bipenddatetime}},
                                          "month": {"Value": {"S": mm}},
                                          "day": {"Value": {"S": dd}}, "year": {"Value": {"S": yy}},
                                          "bip_status": {"Value": {"S": 'Success'}}})            
                except Exception as e:
                    print(e)
                
                if zeroRowsReturned == 'false':
                    try:

                        # df = pd.read_xml(newoutput, dtype=str)
                        # # print(df)
                        # df.to_xml(xml_file, index=False)
                        # # Convert XML to CSV Format
                        # tree = ET.parse(xml_file)
                        # root = tree.getroot()
                        # get_range = lambda col: range(len(col))
                        # l = [{r[i].tag: r[i].text for i in get_range(r)} for r in root]
        
                        # df2 = pd.DataFrame.from_dict(l)
                        
                        # Save CSV File Format as needed
                        #df.to_csv(csv_file, index=False)
                        
                        print('after csv saved')
            
                        # if loadType == 'Full' or loadType == 'Special_Full':
                        prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        # elif loadType == 'CDC' or loadType == 'Special_CDC':
                        #     prefix = "EVO/TblData/CDC/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"

                        # if loadType == 'Full':
                        #     prefix = "EVO/TblData/FullTableLoads/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        # elif loadType == 'CDC':
                        #     prefix = "EVO/TblData/CDC/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        # elif loadType == 'Validation':
                        #     prefix = "EVO/TblData/Validation/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        # elif loadType == 'MetaData':
                        #     prefix = "EVO/TblData/MetaData/" + tbl + "/" + str(yy) + '/' + str(mm) + '/' + str(dd) + "/"
                        # filename = prefix + tbl + "_" + str(filedatetime) + ".csv"
                        
                        
                        bucket.upload_file(csv_file, filename, extra_args)
                        print("Extract Data for " + tbl + " Completed")
        
                        FileName = "/" + filename
                        row_count = len(df)
                        print('Number of Records Retrieved from Source: ', row_count)
        
                        redshift_row_count = 0
                        recordsprocessed = 0
                        redshiftbeginrowscount = 0
                        redshift_error = 'false'
                        redshift_error_message = ''

                        # Run Redshift Target Objects to load Data.
                        if loadType == 'FreeForm':
                            try:
                                ### check if table exists and create table is it does not exist #####
                                response = Redshift_Target.tbl_Exists(df, SchemaName, redshift_tablename,'true')
                                
                                table_ifexists = response[0]
                                beforecount = response[1]
                                
                                print('Table Exists Flag: ', table_ifexists)
                                print('Before redshift updates counts: ',beforecount)

                                if freeformTruncateFlag == 'true':
                                    Redshift_Target.tbl_Truncate(SchemaName, tablename)
                                result = Redshift_Target.tbl_Append(SchemaName, redshift_tablename, FileName)
                                #print(result[0])
                                redshift_row_count = result[0]
                                print(redshift_row_count)
                                redshift_error = result[1]
                                redshift_error_message = result[2]
                                print(redshift_error)
                                print(redshift_error_message)
                            except Exception as e:
                                print('line #  870: ', e)
                                redshift_error = 'true'
                                redshift_error_message = str(e)
                                recordcount = 0
                                redshift_row_count = 0
                                recordsprocessed = 0
                                redshiftbeginrowscount = 0

                        
                                
                        print('record count: ', recordcount)
                        redshift_row_count = int(redshift_row_count)
                        print('redshift_row_count: ',redshift_row_count)
                        

                        redshift_inserts = redshift_row_count
                        redshift_updates = 0
                        
                        if int(recordcount) == redshift_row_count and fullToCDCFlag == 'true':
                        #if int(recordcount) == int(redshift_row_count) and row['FullToCDCFlag'] == 'true':
                            if loadType == 'Full': 
                                newloadType = 'CDC'
                            elif loadType == 'Special_Full':
                                newloadType = 'Special_CDC'
                        else:
                            newloadType = loadType
                        
                        print(newloadType)
                        
                        
                        updatenow = datetime.now()
                        updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                        
                        if redshift_error == 'true':
                            LUD = row['LastUpdateDate']
                        else:
                            LUD = updatedatetime
                        print('at DDB table update for success')                           
                        try:
                            update = table.update_item(
                                Key={
                                    'TableName': tableUID
                                },
                                UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10, redshifterror = :val11, redshifterrormessage = :val12, redshiftInserts = :val13, redshiftUpdates = :val14',
                                ExpressionAttributeValues={
                                    ':val1': LUD,
                                    ':val2': recordcount,
                                    ':val3': redshift_row_count,
                                    ':val4': errormsg,
                                    ':val5': updatedatetime,
                                    ':val6': minLUD,
                                    ':val7': maxLUD,
                                    ':val8': 'true',
                                    ':val9': newloadType,
                                    ':val10': 'false',
                                    ':val11': redshift_error,
                                    ':val12': redshift_error_message,
                                    ':val13': redshift_inserts,
                                    ':val14': redshift_updates
                                }
                            )
                        except Exception as e:
                            print('Error on DDB update for success: ', e)
                        
                        #### update ddb_stats_table
                        now = datetime.now()
                        redshiftenddatetime = now.strftime("%Y-%m-%d %H:%M:%S")
                        print('redshift end time: ', redshiftenddatetime)
                        print(str(redshiftenddatetime))
                        
                        if redshift_error == 'true':
                            redshift_status = 'Failed'
                        else:
                            redshift_status = 'Success'
                        
                        try:
                            ddbconn.update_item(
                                    TableName=ddb_stats_table,
                                    Key={"filename": {"S": FName}},
                                    AttributeUpdates={"redshift_fail_description": {"Value": {"S": redshift_error_message}},
                                                      "redshift_rowcount": {"Value": {"S": str(redshift_row_count)}},
                                                      "redshift_end_time": {"Value": {"S": str(redshiftenddatetime)}},
                                                      "redshift_inserted_records": {"Value": {"S": str(redshift_inserts)}},
                                                      "redshift_updated_records": {"Value": {"S": str(redshift_updates)}},
                                                      "redshift_processed_records": {"Value": {"S": str(redshift_updates + redshift_inserts)}},
                                                      "redshift_status": {"Value": {"S": redshift_status}}})
                        
                        except Exception as e:
                            print(e)    
                    except Exception as e:
                        print(e)
                #### ZeroRowsReturned is true #### no records returned from BIP
                else:
                    updatenow = datetime.now()
                    updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                    
                    try:
                        update = table.update_item(
                            Key={
                                'TableName': tableUID
                                },
                                UpdateExpression='SET LastUpdateDate = :val1, ZeroRowsReturned = :val2',
                                ExpressionAttributeValues={
                                    ':val1': updatedatetime,
                                    ':val2': 'true'
                                }
                            )
                        
                        ddbconn.update_item(
                                    TableName=ddb_stats_table,
                                    Key={"filename": {"S": FName}},
                                    AttributeUpdates={"redshift_start_time": {"Value": {"S": ''}},
                                                      "redshift_status": {"Value": {"S": 'Success - No rows returned from BIP'}}})
                    except Exception as e:
                        print('Error with DDB table update: ',e)
                    
                
            else:
                #### STATUS IS FAILURE BIP failure #######
                updatenow = datetime.now()
                updatedatetime = updatenow.strftime("%Y-%m-%d %H:%M:%S")
                
                try:
                    update = table.update_item(
                        Key={
                            'TableName': tableUID
                        },
                        # UpdateExpression='SET LastUpdateDate = :val1, SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = 
                        UpdateExpression='SET SourceRowCount = :val2,RedshiftRowCount = :val3,BIPreportErrMsg = :val4,SourceRecCNTUpdateDate = :val5, minLUD = :val6, maxLUD = :val7, TableCreatedFlag = :val8, DailyLoadType = :val9, ZeroRowsReturned = :val10, redshifterror = :val11, redshifterrormessage = :val12, redshiftInserts = :val13, redshiftUpdates = :val14',
                                ExpressionAttributeValues={
                                    # ':val1': updatedatetime,
                                    ':val2': recordcount,
                                    ':val3': redshift_row_count,
                                    ':val4': errormsg,
                                    ':val5': updatedatetime,
                                    ':val6': minLUD,
                                    ':val7': maxLUD,
                                    ':val8': 'true',
                                    ':val9': newloadType,
                                    ':val10': 'false',
                                    ':val11': redshift_error,
                                    ':val12': redshift_error_message,
                                    ':val13': 0,
                                    ':val14': 0
                                }
                            )
                    ddbconn.update_item(
                                    TableName=ddb_stats_table,
                                    Key={"filename": {"S": FName}},
                                    AttributeUpdates={'bip_fail_description': {"Value": {"S": errormsg}},
                                                      "bip_status": {"Value": {"S": 'Failed'}},
                                                      "bip_end_time": {"Value": {"S": updatedatetime}}})
                except Exception as e:
                    print('BIP Failure routine: ',e)
        
        
        
            try:
                response = ddbconn.get_item(
                   TableName=ddb_stats_table ,
                   Key={'filename': {'S': FName}})
                data = response['Item']
                df = pd.DataFrame(data)
                df.to_csv('/tmp/input.csv', index=False)
                newfilename = "/tmp/" + 'input.csv'
                sourcekey = 'EVOBaseTblLoadStats_Replicate/Replicate_' + FName
                if account_id == prodaccountid:
                    buck = 'ddb-streams'
                elif account_id == devaccountid:
                    buck = 'ddb-streams-dev'
                s3_client.upload_file(newfilename, buck, sourcekey,extra_args)
            except Exception as e:
                print('Failure to write DDB stats table: ', e)
                
                









response = table.scan()
data = response['Items']
listdf = pd.DataFrame(data)

errors_cnt = 0

listdf = listdf.loc[listdf['Frequency'].isin(['Daily'])]
listdf = listdf.loc[listdf['ActiveDownloadFlag'].isin(['true'])]
listdf = listdf.loc[listdf['TableCreatedFlag'].isin(['true'])]
#listdf = listdf.loc[listdf['DailyLoadType'].isin(['CDC', 'Full','Validation'])]
#listdf = listdf.loc[listdf['Schedule'].isin(['false'])]

if priority != 'All':
    listdf = listdf.loc[listdf['priority'].isin([int(priority)])]
if EVOInstance != 'All':
    listdf = listdf.loc[listdf['EVOInstance'].isin([EVOInstance])]
listdf = listdf.sort_values(['priority','sortOrder'])

extra_args = {'ACL': 'bucket-owner-full-control'}
s3_client = boto3.client('s3')


try:
    # response = ddbconn.get_item(
    # TableName=ddb_tablename ,
    # Key={'TableName': {'S': tbl_name}})
    # data = response['Item']
    #list_df = pd.DataFrame(data)
    listdf.to_csv('/tmp/input.csv', index=False)
    newfilename = "/tmp/" + 'input.csv'
    sourcekey = 'EVOBaseTblConroller_Replicate/Replicate_' + ddb_tablename + '.csv'
    if account_id == prodaccountid:
        buck = 'ddb-streams'
    elif account_id == devaccountid:
        buck = 'ddb-streams-dev'
    s3_client.upload_file(newfilename, buck, sourcekey,extra_args)
except Exception as e:
    print(e)
