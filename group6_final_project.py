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
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

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

    return cleanedDataframeList

# Merge datasets given the names, joinCondition, and joinType
def mergeDatasets(dataframeList):
    #try:
        mergedDataframeList = []

        # Join PTechCoilsData and DefectMapsData
        # When joining pTechCoilsData with claimsData there are no claims associated with any of the coils
        # given and no claims associated with any of the defects given

        # on='CoilId'
        df = pd.merge(dataframeList[0], dataframeList[1], how='outer')
        #left_on='BdeCoilId', right_on='ProductIdentification1'
        #print(df.columns)
        #print(dataframeList[2].columns)
        df = pd.merge(df, dataframeList[2], how='outer')
        
        df.to_csv('./Datasets/mergedCoilsData.csv', index=False)

        # Join FlInspectionData and FlInspectionProcessesData
        df1 = pd.merge(dataframeList[6], dataframeList[5], left_on='FLInspectionID', right_on='InspectionProcessID', how='outer')
        df1 = pd.merge(df1, dataframeList[3], on='FLInspectionID', how='outer')
        df1 = pd.merge(df1, dataframeList[4], left_on='FLInspectionID', right_on='InspectionProcessID', how='outer')
        df1.to_csv('./Datasets/mergedInspectionData.csv', index=False)

        df.drop_duplicates()
        
        mergedDataframeList.append(df)
        mergedDataframeList.append(df1)
        #print(df.columns)

        return mergedDataframeList
    #except:
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

        # Insert into tableVARCHAR(2
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
    #print(df.columns)

    mergedCoilsDataColumns = {
        "CoilId": "INT",
        "StartTime": "VARCHAR(30)",
        "EndTime": "VARCHAR(30)",
        "ParamSet": "INT",
        "Grade": "INT",
        "Length": "INT",
        "Width": "INT",
        "Thickness": "DECIMAL",
        "Weight": "INT",
        "Charge": "VARCHAR(30)",  # Data Type not provided
        "MaterialId": "INT",
        "Status": "CHAR(10)",
        "BdeCoilId": "VARCHAR(30)",
        "Description": "VARCHAR(256)",
        "LastDefectId": "INT",
        "TargetQuality": "INT",
        "PdiRecvTime": "VARCHAR(30)",
        "SLength": "INT",
        "InternalStatus": "CHAR(10)",
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
        "ClaimSource": "CHAR(10)",
        "ClaimNumber": "INT",
        "ClaimDispositionSequence": "INT",
        "BusinessUnit": "CHAR(10)",
        "ClaimType": "CHAR(10)",
        "ProductIdentification1": "VARCHAR(30)",
        "ProductIdentification2": "CHAR(10)",
        "MaterialSource": "CHAR(10)",
        "Heat": "VARCHAR(30)",
        "CastDate": "DATE",
        "ProductType": "CHAR(10)",
        "SteelFamily": "CHAR(10)",
        "SteelType": "INT",
        "SteelGradeASTMAISI": "VARCHAR(30)",
        "Finish": "INT",
        "MaterialGaugeOrDiameter": "DECIMAL",
        "MaterialWidthOrLegLength": "DECIMAL",
        "MaterialLength": "DECIMAL",
        "CustomerNumber": "INT",
        "CustomerDestination": "INT",
        "OrderAbbreviation": "CHAR(10)",
        "OrderNumber": "INT",
        "OrderItem": "INT",
        "NationalExportCode": "CHAR(10)",
        "SisterDivisionsClaimed": "CHAR(10)",
        "Format": "INT",
        "FormatMinGaugeOrDiameter": "DECIMAL",
        "FormatMaxGaugeOrDiameter": "DECIMAL",
        "FormatMinWidthOrLegLength": "DECIMAL",
        "FormatMaxWidthOrLegLength": "DECIMAL",
        "FormatMinWeight": "INT",
        "FormatMaxWeight": "INT",
        "InvoiceSeries": "CHAR(5)",
        "InvoiceYear": "INT",
        "InvoiceNumber": "INT",
        "InvoiceItem": "INT",
        "OriginalShippedWeight": "INT",
        "OriginalShipQuality": "VARCHAR(30)",
        "ClaimDispositionStatus": "CHAR(10)",
        "ClaimCreateDate": "DATE",
        "QCApprovedDate": "DATE",
        "ClosedDate": "DATE",
        "TotalWeightClaimed": "INT",
        "CustomerClaimDefect": "VARCHAR(30)",
        "CustomerClaimDefectDesc": "VARCHAR(256)",
        "CustomerClaimDefectWeight": "INT",
        "NASIdentifiedDefect": "INT",
        "NASIdentifiedDefectDesc": "VARCHAR(256)",
        "NASIdentifiedDefectWeight": "INT",
        "AreaofResponsibilityDefect": "VARCHAR(30)",
        "AreaofResponsibilityDefectDesc": "VARCHAR(256)",
        "AreaofResponsibilityDefectWeigh": "INT",
        "CustomerDefectOrigin": "CHAR(10)",
        "CustomerDefectGroup": "VARCHAR(30)",
        "CustomerDefectGroupDesc": "VARCHAR(256)",
        "TotalReturnInventoryWeight": "INT",
        "TotalScrapAtCustomerWeight": "INT",
        "TotalSell3rdPartyWeight": "INT",
        "TotalCustomerCreditWeight": "INT",
        "LastInspectionLine": "INT",
        "LastInspectionMachine": "VARCHAR(30)",
        "LastInspectedDate": "DATE",
        "GeneralComment1": "VARCHAR(256)",
        "GeneralComment2": "VARCHAR(256)",
        "GeneralComment3": "VARCHAR(256)",
        "GeneralComment4": "VARCHAR(256)"
    }

    # Create a new dictionary with only matching key-value pairs
    mergedCoilsDataColumns = {key: value for key, value in mergedCoilsDataColumns.items() if key in df.columns}

    # Print the resulting dictionary
    #print(mergedCoilsDataColumns)

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

    # Assuming 'ClaimCreateDate' is the column with the VARCHAR(30)
    df['ClaimCreateDate'] = pd.to_datetime(df['ClaimCreateDate'], format='%m/%d/%Y %H:%M:%S:%f', errors='coerce')

    # Insert into merged_coils_data table
    #count = 0

    columns_str = ', '.join(map(str, df.columns))

    for index, row in df.iterrows():
        values = []
        for value in row:
            if pd.isnull(value):
                values.append('NULL')
            elif isinstance(value, (int, float)):
                values.append(str(value))
            elif isinstance(value, str):
                values.append(f"'{value}'")
            else:
                values.append(str(value))  # Add additional handling for other data types if needed
        
        values_str = ', '.join(values)
        #print(columns_str)
        #print(values_str)
        
        insert_query = f"INSERT INTO merged_coils_data ({columns_str}) VALUES ({values_str});"
        cursor.execute(insert_query)

        #count += 1
        #print(count)

    #SELECT from merged_coils_data
    #cursor.execute('SELECT * from merged_coils_data')
    #result = cursor.fetchall()
    #print(result)


    # Close the connection
    connection.commit()
    connection.close()

    print("Success")

    #except:
        #print("Failure")

def cleanPTechCoilsData(df):
    # Print observations before
    print(df.shape)

    # detect if both are empty, null, or NAN.
    df = df[df["CoilId"] != df["BdeCoilId"]]

    # Assuming 'Campaign' is the column you want to filter
    df['Campaign'] = pd.to_numeric(df['Campaign'], errors='coerce')

    # Filter out rows where 'Campaign' is not an integer
    df = df[df['Campaign'].notna() & (df['Campaign'] % 1 == 0)]

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

    return df


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
    
    df.rename(columns={"ProductIdentification1": "BdeCoilId"}, inplace=True)

    string_columns = df.select_dtypes(include='object').columns
    df[string_columns] = df[string_columns].replace({r"'": ""}, regex=True)

    # Print observations after
    print(df.shape)
    #print(df.columns)

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

## Use of tokenization, lammatixation and stopword
def NLP():
    # Download nltk resources (run only once)
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('stopwords')

    # Load the CSV file
    file_path = 'FLInspectionComments.csv'
    df = pd.read_csv(file_path)

    # Check the structure of your DataFrame
    print(df.head())

    # Spell Check, Tokenization, Lemmatization, and Stopword Removal
    lemmatizer = WordNetLemmatizer()
    # spell = SpellChecker()
    stop_words = set(stopwords.words('english'))

    def process_text(text):
        # Spell check
        tokens = word_tokenize(str(text))
        # corrected_tokens = [spell.correction(token) for token in tokens]
        corrected_tokens = [token for token in tokens]

        # Lemmatization with handling None type
        lemmatized_tokens = [lemmatizer.lemmatize(token) if token is not None else '' for token in corrected_tokens]

        # Remove stopwords
        filtered_tokens = [token for token in lemmatized_tokens if token.lower() not in stop_words and token != '']

        return ' '.join(filtered_tokens)

    # Apply processing to the 'Comment' column
    df['comments_processed'] = df['Comment'].apply(process_text)

    # Save the DataFrame with the new column
    output_file_path = 'FLInspectionComments_Processed.csv'
    df[['FLInspectionCommentID', 'comments_processed']].to_csv(output_file_path, index=False)

    # Check the updated DataFrame
    print(df.head())

#========================================================================
# main program
#========================================================================

def main(flag):
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

main(flag)