#========================================================================
# Author: Final Project Group 6
#
# Python Version: 3.12.0 64-bit
#
# Course: DSC 200
#
# Assignment: Project 1
#
# Purpose:
#
# TODO
#
# Date Submitted: TODO
#========================================================================

import pandas as pd
import os
import configparser as cp
#import sqlite3
import psycopg2
import re

#========================================================================
# define functions
#========================================================================

# Check if Datasets folder exists before running
flag = True

# Initilize database credentials
credentials = []

host = ""
database = ""
port = ""
user = ""
password = ""

#check if datasets file exit
if not os.path.exists('./Datasets'):
    flag = False
    print("Need to create a folder named Datasets.")

#check if db.conf file exit
if not os.path.exists('./db.conf'):
    flag = False
    print("Need to create a file name db.conf with credentials.")
else:

    try:
        # creat an instance of the parser
        confP = cp.ConfigParser()

        # read in the ini or configuration file
        confP.read(["db.conf"])

        host = confP.get("db", "url")
        database = confP.get("db", "database")
        port = confP.get("db", "port")
        user = confP.get("db", "dbuser")
        password = confP.get("db", "dbpassword")
        
        credentials = [host, database, port, user, password]

    except:
        flag = False
        print("The db.conf file is configured incorrectly. See README.md for configuration.")

# Get the data contained in all of the CSV files used for this project
def getData():
    # Get CSV data
    try:
        pTechCoilsData = pd.read_csv('./Datasets/AP4_PTec_Coils.csv')
        pTechDefectMapsData = pd.read_csv('./Datasets/AP4_Ptec_Defect_Maps_10-coils.csv')
        claimsData = pd.read_csv('./Datasets/claims_2023-05.csv')
        flInspectionCommentsData = pd.read_csv('./Datasets/FLInspectionComments.csv')
        flInspectionMappedDefectsData = pd.read_csv('./Datasets/tblFlatInspectionMappedDefects.csv')
        flInspectionProcessesData = pd.read_csv('./Datasets/tblFlatInspectionProcesses.csv')
        flInspectionData = pd.read_csv('./Datasets/tblFLInspection.csv')

        return [
            pTechCoilsData,
            pTechDefectMapsData,
            claimsData,
            flInspectionCommentsData,
            flInspectionMappedDefectsData,
            flInspectionProcessesData,
            flInspectionData
        ]
    except:
        print("Failed to read files.")

# Runs the individual cleansing functions for all of the datasets
def cleanData(dataframeList):
    cleanedDataframeList = []
    
    cleanedDataframeList.append(cleanPTechCoilsData(dataframeList[0]))
    cleanedDataframeList.append(cleanDefectMapsData(dataframeList[1]))
    cleanedDataframeList.append(cleanClaimsData(dataframeList[2]))
    cleanedDataframeList.append(cleanFlInspectionCommentsData(dataframeList[3]))
    cleanedDataframeList.append(cleanFlInspectionMappedDefectsData(dataframeList[4]))
    cleanedDataframeList.append(cleanFlInspectionProcessesData(dataframeList[5]))
    cleanedDataframeList.append(cleanFlInspectionData(dataframeList[6]))

    return dataframeList

# Merge datasets given the names, joinCondition, and joinType
def mergeDatasets(dataframeList):
    try:
        mergedDataframeList = []

        # Join PTechCoilsData and DefectMapsData
        # When joining pTechCoilsData with claimsData there are no claims associated with any of the coils
        # given and no claims associated with any of the defects given 
        df = pd.merge(dataframeList[0], dataframeList[1], on='CoilId', how='outer')
        df = pd.merge(df, dataframeList[2], left_on='BdeCoilId', right_on='ProductIdentification1', how='outer')
        
        df.to_csv('./Datasets/mergedCoilsData.csv', index=False)

        # Join FlInspectionData and FlInspectionProcessesData
        df1 = pd.merge(dataframeList[6], dataframeList[5], left_on='FLInspectionID', right_on='InspectionProcessID', how='outer')
        df1 = pd.merge(df1, dataframeList[3], on='FLInspectionID', how='outer')
        df1 = pd.merge(df1, dataframeList[4], left_on='FLInspectionID', right_on='InspectionProcessID', how='outer')
        df1.to_csv('./Datasets/mergedInspectionData.csv', index=False)
        
        mergedDataframeList.append(df)
        mergedDataframeList.append(df1)
        #print(df.columns)

        return dataframeList
    except:
        print("Failed to merge datasets.")

def dbConnect(credentials):
    try:
        connection = psycopg2.connect(
            host=credentials[0],
            database=credentials[1],
            port=credentials[2],
            user=credentials[3],
            password=credentials[4]
        )

        return connection
        
    except:
        print("Failed to connect to database.")

def testConnection(connection):
    try:
        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        #Truncate table
        query = 'TRUNCATE merged_coils_data; COMMIT;'
        cursor.execute(query)

        # Drop table
        cursor.execute('DROP TABLE IF EXISTS merged_coils_data; COMMIT;')

        # Create a table
        cursor.execute('CREATE TABLE IF NOT EXISTS merged_coils_data (defect_id int PRIMARY KEY, name varchar(30)); COMMIT;')
        
        cursor.execute('SELECT * from information_schema.tables WHERE table_schema=\'public\'')
        result = cursor.fetchall()
        print(result)

        # Insert into table
        cursor.execute('INSERT INTO merged_coils_data (defect_id, name) VALUES (123, \'test\')')

        cursor.execute('SELECT * from merged_coils_data')
        result = cursor.fetchall()
        print(result)


        # Close the connection
        connection.close()

        print("Success")

    except:
        print("Failure")

def exportToDatabase(connection, dataframeList):
    df = dataframeList[0]
    df2 = dataframeList[1]
    print(df.columns)

    mergedCoilsDataColumns = {
        "CoilId": "INT",
        "StartTime": "TIMESTAMP",
        "EndTime": "TIMESTAMP",
        "ParamSet": "INT",
        "Grade": "INT",
        "Length": "INT",
        "Width": "INT",
        "Thickness": "DECIMAL",
        "Weight": "INT",
        "Charge": "VARCHAR(20)",  # Data Type not provided
        "MaterialId": "INT",
        "Status": "CHAR",
        "BdeCoilId": "VARCHAR(20)",
        "Description": "VARCHAR(20)",
        "LastDefectId": "INT",
        "TargetQuality": "INT",
        "PdiRecvTime": "TIMESTAMP",
        "SLength": "INT",
        "InternalStatus": "CHAR",
        "DefectCount": "INT",
        "Campaign": "INT",
        "DefectId": "INT",
        "Class": "INT",
        "PeriodId": "INT",
        "PeriodLength": "INT",
        "PositionCD": "DECIMAL",
        "PositionRCD": "DECIMAL",
        "PositionMD": "DECIMAL",
        "Side": "INT",
        "SizeCD": "DECIMAL",
        "SizeMD": "DECIMAL",
        "CameraNo": "INT",
        "DefectNo": "INT",
        "MergedTo": "INT",
        "Confidence": "INT",
        "RoiX0": "INT",
        "RoiX1": "INT",
        "RoiY0": "INT",
        "RoiY1": "INT",
        "OriginalClass": "INT",
        "PP_ID": "INT",
        "PostCL": "INT",
        "MergerPP": "INT",
        "OnlineCPP": "INT",
        "OfflineCPP": "INT",
        "Rollerid": "INT",
        "CL_PROD_CLASS": "INT",
        "CL_TEST_CLASS": "INT",
        "AbsPosCD": "DECIMAL",
        "ClaimSource": "CHAR",
        "ClaimNumber": "INT",
        "ClaimDispositionSequence": "INT",
        "BusinessUnit": "CHAR",
        "ClaimType": "CHAR",
        "ProductIdentification1": "VARCHAR(20)",
        "ProductIdentification2": "CHAR",
        "MaterialSource": "CHAR",
        "Heat": "VARCHAR(20)",
        "CastDate": "DATE",
        "ProductType": "CHAR",
        "SteelFamily": "CHAR",
        "SteelType": "INT",
        "SteelGradeASTMAISI": "VARCHAR(20)",
        "Finish": "INT",
        "MaterialGaugeOrDiameter": "DECIMAL",
        "MaterialWidthOrLegLength": "DECIMAL",
        "MaterialLength": "DECIMAL",
        "CustomerNumber": "INT",
        "CustomerDestination": "INT",
        "OrderAbbreviation": "CHAR",
        "OrderNumber": "INT",
        "OrderItem": "INT",
        "NationalExportCode": "CHAR",
        "SisterDivisionsClaimed": "CHAR",
        "Format": "INT",
        "FormatMinGaugeOrDiameter": "DECIMAL",
        "FormatMaxGaugeOrDiameter": "DECIMAL",
        "FormatMinWidthOrLength": "DECIMAL",
        "FormatMaxWidthOrLength": "DECIMAL",
        "FormatMinWeight": "INT",
        "FormatMaxWeight": "INT",
        "InvoiceSeries": "CHAR",
        "InvoiceYear": "INT",
        "InvoiceNumber": "INT",
        "InvoiceItem": "INT",
        "OriginalShippedWeight": "INT",
        "OriginalShipQuality": "VARCHAR(20)",
        "ClaimDispositionStatus": "CHAR",
        "ClaimCreateDate": "DATE",
        "QCApprovedDate": "DATE",
        "ClosedDate": "DATE",
        "TotalWeightClaimed": "INT",
        "CustomerClaimDefect": "VARCHAR(20)",
        "CustomerClaimDefectDesc": "VARCHAR(20)",
        "CustomerClaimDefectWeight": "INT",
        "NASIdentifiedDefect": "INT",
        "NASIdentifiedDefectDesc": "VARCHAR(20)",
        "NASIdentifiedDefectWeight": "INT",
        "AreaofResponsibilityDefect": "VARCHAR(20)",
        "AreaofResponsibilityDefectDesc": "VARCHAR(20)",
        "AreaofResponsibilityDefectWeight": "INT",
        "CustomerDefectOrigin": "CHAR",
        "CustomerDefectGroup": "VARCHAR(20)",
        "CustomerDefectGroupDesc": "VARCHAR(20)",
        "TotalReturnInventoryWeight": "INT",
        "TotalScrapAtCustomerWeight": "INT",
        "TotalSell3rdPartyWeight": "INT",
        "TotalCustomerCreditWeight": "INT",
        "LastInspectionLine": "INT",
        "LastInspectionMachine": "VARCHAR(20)",
        "LastInspectedDate": "DATE",
        "GeneralComment1": "VARCHAR(20)",
        "GeneralComment2": "VARCHAR(20)",
        "GeneralComment3": "VARCHAR(20)",
        "GeneralComment4": "VARCHAR(20)"
    }

    # Create a new dictionary with only matching key-value pairs
    matching_columns = {key: value for key, value in mergedCoilsDataColumns.items() if key in df.columns}

    # Print the resulting dictionary
    print(matching_columns)

    #try:
    # Create a cursor object to interact with the database
    cursor = connection.cursor()

    #Truncate table
    query = "DO $$\
                BEGIN\
                IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'merged_coils_data') THEN\
                    EXECUTE 'TRUNCATE TABLE merged_coils_data';\
                END IF;\
            END $$;"
    cursor.execute(query)

    # Drop table
    cursor.execute('DROP TABLE IF EXISTS merged_coils_data; COMMIT;')

    # Create a table

    # Initialize an empty string to store the column definitions
    column_definitions = ""

    # Iterate through the dictionary items
    for column_name, data_type in mergedCoilsDataColumns.items():
        # Concatenate the column name and data type
        column_definitions += f"{column_name} {data_type}"

        # Add a comma if it's not the last item
        if column_name != list(mergedCoilsDataColumns.keys())[-1]:
            column_definitions += ", "

    # Print the resulting column definitions string
    #print(column_definitions)

    cursor.execute('CREATE TABLE IF NOT EXISTS merged_coils_data ({}); COMMIT;'.format(column_definitions))
    
    cursor.execute('SELECT * from information_schema.tables WHERE table_schema=\'public\'')
    result = cursor.fetchall()
    print(result)

    # Insert into merged_coils_data table
    for index, row in df.iterrows():
        insert_query = f"INSERT INTO merged_coils_data (%s) VALUES (%s, %s, %s);"
        cursor.execute(insert_query, (mergedCoilsDataColumns.keys, row['column1'], row['column2'], row['column3']))

    cursor.execute('SELECT * from merged_coils_data')
    result = cursor.fetchall()
    print(result)


    # Close the connection
    connection.close()

    print("Success")

    #except:
        #print("Failure")

def cleanPTechCoilsData(df):
    # Print observations before
    print(df.shape)

    # detect if both are empty, null, or NAN.
    df = df[df["CoilId"] != df["BdeCoilId"]]

    # Remove Charge column since it is always empty
    #df.drop(['Charge'], axis=1, inplace=True)

    #check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() == 1]
    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)

    # Fix capitilization issues
    df['BdeCoilId'] = df['BdeCoilId'].apply(lambda x: x.upper() if isinstance(x, str) else x)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)


    # Drop records where length, width, thickness, or weight are <= 0. This is not possible.
    df = df[df["Length"] > 0]
    df = df[df["Width"] > 0]
    df = df[df["Thickness"] > 0]
    df = df[df["Weight"] > 0]

    # Print observations after
    print(df.shape)


def cleanDefectMapsData(df):
    # Print observations before
    print(df.shape)

    # empty, null
    df = df[df["CoilId"] != '']
    df = df[df["CoilId"] != None]
    df = df[df["DefectId"] != '']
    df = df[df["DefectId"] != None]

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)


    df = df[df["PeriodLength"] > 0]
    df = df[df["SizeCD"] > 0]
    df = df[df["SizeMD"] > 0]

    # Print observations after
    print(df.shape)

    return df

def cleanClaimsData(df):
    # Print observations before
    print(df.shape)

    # empty, null
    df = df[df["ProductIdentification1"] != '']
    df = df[df["ProductIdentification1"] != None]
    df = df[df["ClaimNumber"] != '']
    df = df[df["ClaimNumber"] != None]

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    df = df[df["TotalWeightClaimed"] > 0]
    df = df[df["CustomerClaimDefectWeight"] > 0]
    df = df[df["NASIdentifiedDefectWeight"] > 0]
    df = df[df["AreaofResponsibilityDefectWeigh"] > 0]

    # Print observations after
    print(df.shape)

    return df

def cleanFlInspectionCommentsData(df):
    # Drop the 'isActive' column as all the value is 1 in the dataset ??

    return df



def cleanFlInspectionMappedDefectsData(df):
    # Print observations before
    print(df.shape)

    # Remove this column the defect count is always 1
    df.drop(['DefectCount'], axis=1, inplace=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Drop records where length is 0 or < 0. Length less than 0 is not possible for a defect.
    df = df.loc[df["Length"] > 0]

    # Print observations after
    print(df.shape)

    return df

def cleanFlInspectionProcessesData(df):
    # Print observations before
    print(df.shape)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # # Remove these columns they contain the same value for every record
    # values = [
    # "InspectionGroup","LateralEdgeSeamTopOS", "LateralEdgeSeamTopMS", "LateralEdgeSeamBottomOS",
    # "LateralEdgeSeamBottomMS", "InspectionType", "BuffTopHead", "BuffTopCenter",
    # "BuffTopTail", "BuffBottomHead", "BuffBottomCenter", "BuffBottomTail",
    # "C47HeadHeight", "C47MiddleHeight", "C47TailHeight", "HeadPitch", "MiddlePitch",
    # "TailPitch", "C09HeadHeight", "C09MiddleHeight", "C09TailHeight",
    # "RoughnessTHeadOSSeverity", "RoughnessTHeadCenterSeverity", "RoughnessTHeadDSSeverity",
    # "RoughnessTBodyOSSeverity", "RoughnessTBodyCenterSeverity", "RoughnessTBodyDSSeverity",
    # "RoughnessTTailOSSeverity", "RoughnessTTailCenterSeverity", "RoughnessTTailDSSeverity",
    # "RoughnessBHeadOSSeverity", "RoughnessBHeadCenterSeverity", "RoughnessBHeadDSSeverity",
    # "RoughnessBBodyOSSeverity", "RoughnessBBodyCenterSeverity", "RoughnessBBodyDSSeverity",
    # "RoughnessBTailOSSeverity", "RoughnessBTailCenterSeverity", "RoughnessBTailDSSeverity",
    # "RoughnessTHeadOSType", "RoughnessTHeadCenterType", "RoughnessTHeadDSType",
    # "RoughnessTBodyOSType", "RoughnessTBodyCenterType", "RoughnessTBodyDSType",
    # "RoughnessTTailOSType", "RoughnessTTailCenterType", "RoughnessTTailDSType",
    # "RoughnessBHeadOSType", "RoughnessBHeadCenterType", "RoughnessBHeadDSType",
    # "RoughnessBBodyOSType", "RoughnessBBodyCenterType", "RoughnessBBodyDSType",
    # "RoughnessBTailOSType", "RoughnessBTailCenterType", "RoughnessBTailDSType",
    # "HeadDefectCode", "TailScrap", "HeadScrap", "TailDefectCode", "SamplesTaken", "PaperUsed", "UserID"
    # ]

    # for value in values:
    #     df.drop([value], axis=1, inplace=True)

    # #check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() == 1]
    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)

    # Print observations after
    print(df.shape)

    return df

def cleanFlInspectionData(df):
    # Print observations before
    print(df.shape)

    # a lot of duplicates data in this datasets (More than 50% data are duplicates)
    # drop the duplications
    df = df.drop_duplicates()

    #check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() == 1]
    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)

    # Print observations after
    print(df.shape)

    return df

#========================================================================
# main program
#========================================================================

if flag:

    dataframeList = getData()
    #mergeDatasets(dataframeList)
    
    #print(dataframeList[0].head())
    
    # Test connecting to the database
    #print(credentials)
    #connection = dbConnect(credentials)
    #testConnection(connection)

    #cleanPTechCoilsData(dataframeList[0])
    #cleanDefectMapsData(dataframeList[1])
    #cleanClaimsData(dataframeList[2])
    #cleanFlInspectionCommentsData([3])
    #cleanFlInspectionMappedDefectsData(dataframeList[4])
    #cleanFlInspectionProcessesData(dataframeList[5])
    cleanedDataframeList = cleanData(dataframeList)
    mergedDataframeList = mergeDatasets(cleanedDataframeList)
    connection = dbConnect(credentials)
    #testConnection(connection)
    exportToDatabase(connection, mergedDataframeList)