#========================================================================
# Authors: Final Project Group 6 - Abhishek Shrestha, Brandon Cooper, &
# Nathan Reed
#
# Python Version: 3.12.0 64-bit
#
# Course: DSC 200
#
# Assignment: Final Project
#
# *************************** IMPORTANT *********************************
# 
# Please see README.md for cofiguration information for this program!
#
# ***********************************************************************
#
# Purpose:
#
# The objective of this project is to develop a Python application for
# collecting and preprocessing data pertaining to production flaws at
# Stainless Steel Company X, a stainless-steel manufacturing company.
# The data originates from automated systems, employee observations, and
# customer claims. The project aims to consolidate datasets, perform
# comprehensive cleaning tasks, and upload the cleaned data to a
# designated database.
#
# Date Submitted: 12/13/2023
#========================================================================

import pandas as pd
import os
import configparser as cp
import psycopg2
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
#import spellchecker


# Download nltk resources (run only once)
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('stopwords')


#========================================================================
# Data Cleaning Functions
#========================================================================

def cleanPTechCoilsData(df):
    # Print observations before
    print(df.shape)

    # Remove leading and trailing spaces
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Remove more than one spaces in a row
    df.replace(to_replace=r'\s+', value=' ', regex=True, inplace=True)

    # detect if both are empty, null, or NAN.
    df = df[df["CoilId"] != df["BdeCoilId"]]

    # Assuming 'Campaign' is the column you want to filter
    df['Campaign'] = pd.to_numeric(df['Campaign'], errors='coerce')

    # Filter out rows where 'Campaign' is not an integer
    df = df[df['Campaign'].notna() & (df['Campaign'] % 1 == 0)]

    # Drop records where length, width, thickness, or weight are <= 0. This is not possible.
    df = df[df["Length"] > 0]
    df = df[df["Width"] > 0]
    df = df[df["Thickness"] > 0]
    df = df[df["Weight"] > 0]

    #check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() <= 1]

    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)

    # Fix capitilization issues
    df['BdeCoilId'] = df['BdeCoilId'].apply(lambda x: x.upper() if isinstance(x, str) else x)


     # Escape ' character
    string_columns = df.select_dtypes(include='object').columns
    df[string_columns] = df[string_columns].replace({r"'": ""}, regex=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Print observations after
    print(df.shape)


    # Save the cleaned DataFrame to a new CSV file
    clean_file_path = './Datasets/cleanedDatasets/cleaned_PTechCoilsData.csv'
    df.to_csv(clean_file_path, index=False)

    return df

def cleanDefectMapsData(df):
    # Print observations before
    print(df.shape)

    # Remove leading and trailing spaces
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Remove more than one spaces in a row
    df.replace(to_replace=r'\s+', value=' ', regex=True, inplace=True)

    # empty, null
    df = df[df["CoilId"] != '']
    df = df[df["CoilId"] != None]
    df = df[df["DefectId"] != '']
    df = df[df["DefectId"] != None]

    df = df[df["PeriodLength"] > 0]
    df = df[df["SizeCD"] > 0]
    df = df[df["SizeMD"] > 0]

    # Drop the 'isActive' column as all the value is 1 in the dataset
    columns_to_drop = df.columns[df.nunique() <= 1]

    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)

     # Escape ' character
    string_columns = df.select_dtypes(include='object').columns
    df[string_columns] = df[string_columns].replace({r"'": ""}, regex=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Print observations after
    print(df.shape)

    # Save the cleaned DataFrame to a new CSV file
    clean_file_path = './Datasets/cleanedDatasets/cleanedDefectMapsData.csv'
    df.to_csv(clean_file_path, index=False)

    return df

def cleanClaimsData(df):
    # Print observations before
    print(df.shape)

    # Remove leading and trailing spaces
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Remove more than one spaces in a row
    df.replace(to_replace=r'\s+', value=' ', regex=True, inplace=True)

    # Rename columns
    df.rename(columns={"ProductIdentification1": "BdeCoilId"}, inplace=True)

    # empty, null
    df = df[df["BdeCoilId"] != '']
    df = df[df["BdeCoilId"] != None]
    df = df[df["ClaimNumber"] != '']
    df = df[df["ClaimNumber"] != None]

    # Columns that should not be less than or equal to 0
    df = df[df["TotalWeightClaimed"] > 0]
    df = df[df["CustomerClaimDefectWeight"] > 0]
    df = df[df["MaterialGaugeOrDiameter"] > 0]
    df = df[df["MaterialWidthOrLegLength"] > 0]
    df = df[df["MaterialLength"] > 0]
    df = df[df["FormatMaxGaugeOrDiameter"] > 0]
    df = df[df["FormatMaxWidthOrLegLength"] > 0]
    df = df[df["FormatMaxWeight"] > 0]
    df = df[df["TotalWeightClaimed"] > 0]
    df = df[df["OriginalShippedWeight"] > 0]

    # check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() <= 1]

    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)


    # Escape ' character
    string_columns = df.select_dtypes(include='object').columns
    df[string_columns] = df[string_columns].replace({r"'": ""}, regex=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Print observations after
    print(df.shape)

    # Save the cleaned DataFrame to a new CSV file
    clean_file_path = './Datasets/cleanedDatasets/cleanedClaimsData.csv'
    df.to_csv(clean_file_path, index=False)

    return df

def cleanFlInspectionCommentsData(df):
    # Print observations before
    print(df.shape)

    # Remove leading and trailing spaces
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Remove more than one spaces in a row
    df.replace(to_replace=r'\s+', value=' ', regex=True, inplace=True)

    df = df[df["FLInspectionCommentID"] != '']
    df = df[df["FLInspectionCommentID"] != None]
    df = df[df["FLInspectionID"] != '']
    df = df[df["FLInspectionID"] != None]

    # Drop the below columns if they have the same values for all rows
    if (df['ChangeProgram'] == df['CreateProgram']).all():
        df.drop(['ChangeProgram'], axis=1, inplace=True)

    if (df['ChangeDate'] == df['CreateDate']).all():
        df.drop(['ChangeDate'], axis=1, inplace=True)

    if (df['ChangeTime'] == df['CreateTime']).all():
        df.drop(['ChangeTime'], axis=1, inplace=True)

    # check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() <= 1]

    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)

     # Escape ' character
    string_columns = df.select_dtypes(include='object').columns
    df[string_columns] = df[string_columns].replace({r"'": ""}, regex=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Print observations after
    print(df.shape)

    # Save the cleaned DataFrame to a new CSV file
    clean_file_path = './Datasets/cleanedDatasets/cleanedFlInspectionCommentsData.csv'
    df.to_csv(clean_file_path, index=False)

    return df

def cleanFlInspectionMappedDefectsData(df):
    # Print observations before
    print(df.shape)

    # Remove leading and trailing spaces
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Remove more than one spaces in a row
    df.replace(to_replace=r'\s+', value=' ', regex=True, inplace=True)

    # Rename columns
    df.rename(columns={"InspectionProcessID": "FLInspectionID"}, inplace=True)

    df = df[df["FLInspectionID"] != '']
    df = df[df["FLInspectionID"] != None]

    # Drop records where length is 0 or < 0. Length less than 0 is not possible for a defect.
    df = df.loc[df["Length"] > 0]

    # check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() <= 1]

    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)


     # Escape ' character
    string_columns = df.select_dtypes(include='object').columns
    df[string_columns] = df[string_columns].replace({r"'": ""}, regex=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Print observations after
    print(df.shape)

    # Save the cleaned DataFrame to a new CSV file
    clean_file_path = './Datasets/cleanedDatasets/cleanedFlInspectionMappedDefectsData.csv'
    df.to_csv(clean_file_path, index=False)

    return df

def cleanFlInspectionProcessesData(df):
    # Print observations before
    print(df.shape)

    # Remove leading and trailing spaces
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Remove more than one spaces in a row
    df.replace(to_replace=r'\s+', value=' ', regex=True, inplace=True)

    df.rename(columns={"InspectionProcessID": "FLInspectionID"}, inplace=True)
    df.rename(columns={"FlatCoilID": "CoilId"}, inplace=True)
    df.rename(columns={"CoilNumber": "BdeCoilId"}, inplace=True)

    # detect if both are empty, null, or NAN.
    df = df[df["CoilId"] != df["BdeCoilId"]]

    df = df[df["FLInspectionID"] != '']
    df = df[df["FLInspectionID"] != None]

    #check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() <= 1]

    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)


     # Escape ' character
    string_columns = df.select_dtypes(include='object').columns
    df[string_columns] = df[string_columns].replace({r"'": ""}, regex=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Print observations after
    print(df.shape)

    # Save the cleaned DataFrame to a new CSV file
    clean_file_path = './Datasets/cleanedDatasets/cleanedFlInspectionProcessesData.csv'
    df.to_csv(clean_file_path, index=False)

    return df

def cleanFlInspectionData(df):
    # Print observations before
    print(df.shape)

    # Remove leading and trailing spaces
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Remove more than one spaces in a row
    df.replace(to_replace=r'\s+', value=' ', regex=True, inplace=True)

    df = df[df["FLInspectionID"] != '']
    df = df[df["FLInspectionID"] != None]

    # Rename columns that were spelled incorrectly
    df.rename(columns={"CurrentGuage": "CurrentGauge"}, inplace=True)
    df.rename(columns={"HotAPGuage": "HotAPGauge"}, inplace=True)
    df.rename(columns={"ColdAPGuage": "ColdAPGauge"}, inplace=True)
    df.rename(columns={"ExitCoilNumber": "BdeCoilId"}, inplace=True)

    # Drop redundant and repeated columns
    df.drop(['InspectionDate', 'InspectionTime'], axis=1, inplace=True)

    #check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() <= 1]

    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)

     # Escape ' character
    string_columns = df.select_dtypes(include='object').columns
    df[string_columns] = df[string_columns].replace({r"'": ""}, regex=True)

    # Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # Print observations after
    print(df.shape)

    # Save the cleaned DataFrame to a new CSV file
    clean_file_path = './Datasets/cleanedDatasets/cleanedFlInspectionData.csv'
    df.to_csv(clean_file_path, index=False)


    return df

# Runs the individual cleansing functions for all of the datasets
def cleanData(dataframeList):
    cleanedDataframeList = []
    
    try:
        cleanedDataframeList.append(cleanPTechCoilsData(dataframeList[0]))
        cleanedDataframeList.append(cleanDefectMapsData(dataframeList[1]))
        cleanedDataframeList.append(cleanClaimsData(dataframeList[2]))
        cleanedDataframeList.append(cleanFlInspectionCommentsData(dataframeList[3]))
        cleanedDataframeList.append(cleanFlInspectionMappedDefectsData(dataframeList[4]))
        cleanedDataframeList.append(cleanFlInspectionProcessesData(dataframeList[5]))
        cleanedDataframeList.append(cleanFlInspectionData(dataframeList[6]))

        return cleanedDataframeList
    
    except:
        print('Failed to clean data.')




#========================================================================
# Retrieve Data
#========================================================================

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




#========================================================================
# Merge Datasets Functions
#========================================================================

# Merge datasets given the names, joinCondition, and joinType
def mergeDatasets(dataframeList):
    try:
        mergedDataframeList = []

        df = pd.merge(dataframeList[0], dataframeList[1], how='outer')
        df = pd.merge(df, dataframeList[2], how='outer')
        df.to_csv('./Datasets/mergedDatasets/mergedCoilsData.csv', index=False)

        df1 = pd.merge(dataframeList[6], dataframeList[5], how='outer')
        df1 = pd.merge(df1, dataframeList[3], how='outer')
        df1 = pd.merge(df1, dataframeList[4], how='outer')
        df1.to_csv('./Datasets/mergedDatasets/mergedInspectionData.csv', index=False)

        # Remove duplicate rows
        df.drop_duplicates(inplace=True)
        # Remove duplicate rows
        df1.drop_duplicates(inplace=True)
        
        mergedDataframeList.append(df)
        mergedDataframeList.append(df1)
        #print(df.columns)

        return mergedDataframeList
    except:
        print("Failed to merge datasets.")



#========================================================================
# Database Functions
#========================================================================

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


def exportCoilsData(connection, df):

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
        "Charge": "VARCHAR(30)",
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


    try:
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

        cursor.execute('CREATE TABLE IF NOT EXISTS merged_coils_data ({}); COMMIT;'.format(column_definitions))
        
        cursor.execute('SELECT * from information_schema.tables WHERE table_schema=\'public\'')
        result = cursor.fetchall()
        print(result)

        # Insert into merged_coils_data table
        count = 0

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
            
            insert_query = f"INSERT INTO merged_coils_data ({columns_str}) VALUES ({values_str});"
            cursor.execute(insert_query)

            count += 1
            print(count)


        # Close the connection
        connection.commit()

        print("Success")

    except:
        print("Database operation failure for merged_coils_data")

def exportInspectionData(connection, df):

    mergedInspectionDataColumns = {
        'FLInspectionCommentID': 'INT',
        'FLInspectionID': 'INT',
        'DefectMapSeqNumber': 'INT',
        'DefectMapRemarkSeqNumber': 'INT',
        'Comment': 'VARCHAR(256)',
        'CreateProgram': 'VARCHAR(30)',
        'CreateDate': 'INT',
        'CreateTime': 'INT',
        'ChangeProgram': 'VARCHAR(30)',
        'ChangeDate': 'INT',
        'ChangeTime': 'INT',
        'isActive': 'INT',
        'LineID': 'INT',
        'InspectionDate': 'SMALLVARCHAR(30)',
        'InspectionDateInt': 'INT',
        'InspectionTime': 'SMALLVARCHAR(30)',
        'InspectionTimeInt': 'INT',
        'DealerCode': 'SMALLINT',
        'SuperiorFinishCode': 'NCHAR(10)',
        'InspectionNumber': 'INT',
        'ExitCoilNumber': 'NCHAR(10)',
        'ExitCoilDivision': 'NCHAR(10)',
        'PackProductCode': 'NCHAR(10)',
        'SteelGradeID': 'INT',
        'CurrentGauge': 'FLOAT',
        'HotAPGauge': 'FLOAT',
        'ColdAPGauge': 'FLOAT',
        'CurrentWidth': 'FLOAT',
        'InitialWidth': 'FLOAT',
        'NetWeight': 'FLOAT',
        'TotalLength': 'FLOAT',
        'TotalSheets': 'INT',
        'Percent1AQualityExt': 'DECIMAL(5,2)',
        'Percent1BQualityExt': 'DECIMAL(5,2)',
        'Percent2QualityExt': 'DECIMAL(5,2)',
        'PercentScrapQualityExt': 'DECIMAL(5,2)',
        'Percent1AQualityIntCAP': 'DECIMAL(5,2)',
        'Percent1BQualityIntCAP': 'DECIMAL(5,2)',
        'Percent2QualityIntCAP': 'DECIMAL(5,2)',
        'PercentScrapQualityIntCAP': 'DECIMAL(5,2)',
        'Percent1AQualityExtCAP': 'DECIMAL(5,2)',
        'Percent1BQualityExtCAP': 'DECIMAL(5,2)',
        'Percent2QualityExtCAP': 'DECIMAL(5,2)',
        'PercentScrapQualityExtCAP': 'DECIMAL(5,2)',
        'Percent1AQualityIntHAP': 'DECIMAL(5,2)',
        'Percent1BQualityIntHAP': 'DECIMAL(5,2)',
        'Percent2QualityIntHAP': 'DECIMAL(5,2)',
        'PercentScrapQualityIntHAP': 'DECIMAL(5,2)',
        'Percent1AQualityExtHAP': 'DECIMAL(5,2)',
        'Percent1BQualityExtHAP': 'DECIMAL(5,2)',
        'Percent2QualityExtHAP': 'DECIMAL(5,2)',
        'PercentScrapQualityExtHAP': 'DECIMAL(5,2)',
        'MnDefect1': 'INT',
        'DefectGroup1': 'NCHAR(10)',
        'DefectGroup2': 'NCHAR(10)',
        'MnDefect2': 'INT',
        'MnDefectCAPInt1': 'INT',
        'MnDefectCAPInt2': 'INT',
        'MnDefectCAPExt1': 'INT',
        'MnDefectCAPExt2': 'INT',
        'CAPDefectiveLength': 'FLOAT',
        'MnDefectHAPInt1': 'INT',
        'MnDefectHAPInt2': 'INT',
        'MnDefectHAPExt1': 'INT',
        'MnDefectHAPExt2': 'INT',
        'CAPSolution': 'INT',
        'HAPSolution': 'INT',
        'CutLineSolution': 'INT',
        'ChangeOfSide': 'NCHAR(10)',
        'CAPLineGrpCode': 'NCHAR(10)',
        'HAPLineGrpCode': 'NCHAR(10)',
        'ZMillLineGrpCode': 'NCHAR(10)',
        'CutLineGrpCode': 'NCHAR(10)',
        'CAPWorkCode': 'INT',
        'HAPWorkCode': 'INT',
        'CutLineWorkCode': 'INT',
        'CAPYield': 'DECIMAL(4,3)',
        'HAPYield': 'DECIMAL(4,3)',
        'CutLineYield': 'DECIMAL(4,3)',
        'AccumulatedEfficiency': 'DECIMAL(4,3)',
        'InspectionDateTime': 'VARCHAR(30)',
        'InspectionMappedDefectID': 'INT',
        'DefectCodeID': 'INT',
        'SideID': 'INT',
        'FaceID': 'INT',
        'StartPosition': 'INT',
        'Length': 'INT',
        'QualityID': 'INT',
        'DefectCount': 'INT',
        'Description': 'VARCHAR(256)',
        'FaceDescription': 'VARCHAR(256)',
        'QualityCode': 'INT',
        'QualityDescription': 'VARCHAR(256)',
        'SideDescription': 'CHAR(10)',
        'InspectionProcessID': 'INT',
        'CoilId': 'INT',
        'BdeCoilId': 'VARCHAR(30)',
        'ProcessStartTime': 'TIME',
        'InspectionStartTime': 'TIME',
        'InspectionEndTime': 'TIME',
        'ApprovedTime': 'TIME',
        'InspectionGroup': 'VARCHAR(30)',
        'InspectionStatusID': 'VARCHAR(30)',
        'LateralEdgeSeamTopOS': 'INT',
        'LateralEdgeSeamTopMS': 'INT',
        'LateralEdgeSeamBottomOS': 'INT',
        'LateralEdgeSeamBottomMS': 'INT',
        'InspectionType': 'CHAR(10)',
        'Observations': 'VARCHAR(512)',
        'BuffTopHead': 'VARCHAR(30)',
        'BuffTopCenter': 'VARCHAR(30)',
        'BuffTopTail': 'VARCHAR(30)',
        'BuffBottomHead': 'VARCHAR(30)',
        'BuffBottomCenter': 'VARCHAR(30)',
        'BuffBottomTail': 'VARCHAR(30)',
        'C47HeadHeight': 'VARCHAR(30)',
        'C47MiddleHeight': 'VARCHAR(30)',
        'C47TailHeight': 'VARCHAR(30)',
        'HeadPitch': 'VARCHAR(30)',
        'MiddlePitch': 'VARCHAR(30)',
        'TailPitch': 'VARCHAR(30)',
        'C09HeadHeight': 'VARCHAR(30)',
        'C09MiddleHeight': 'VARCHAR(30)',
        'C09TailHeight': 'VARCHAR(30)',
        'RoughnessTHeadOSSeverity': 'VARCHAR(30)',
        'RoughnessTHeadCenterSeverity': 'VARCHAR(30)',
        'RoughnessTHeadDSSeverity': 'VARCHAR(30)',
        'RoughnessTBodyOSSeverity': 'VARCHAR(30)',
        'RoughnessTBodyCenterSeverity': 'VARCHAR(30)',
        'RoughnessTBodyDSSeverity': 'VARCHAR(30)',
        'RoughnessTTailOSSeverity': 'VARCHAR(30)',
        'RoughnessTTailCenterSeverity': 'VARCHAR(30)',
        'RoughnessTTailDSSeverity': 'VARCHAR(30)',
        'RoughnessBHeadOSSeverity': 'VARCHAR(30)',
        'RoughnessBHeadCenterSeverity': 'VARCHAR(30)',
        'RoughnessBHeadDSSeverity': 'VARCHAR(30)',
        'RoughnessBBodyOSSeverity': 'VARCHAR(30)',
        'RoughnessBBodyCenterSeverity': 'VARCHAR(30)',
        'RoughnessBBodyDSSeverity': 'VARCHAR(30)',
        'RoughnessBTailOSSeverity': 'VARCHAR(30)',
        'RoughnessBTailCenterSeverity': 'VARCHAR(30)',
        'RoughnessBTailDSSeverity': 'VARCHAR(30)',
        'RoughnessTHeadOSType': 'VARCHAR(30)',
        'RoughnessTHeadCenterType': 'VARCHAR(30)',
        'RoughnessTHeadDSType': 'VARCHAR(30)',
        'RoughnessTBodyOSType': 'VARCHAR(30)',
        'RoughnessTBodyCenterType': 'VARCHAR(30)',
        'RoughnessTBodyDSType': 'VARCHAR(30)',
        'RoughnessTTailOSType': 'VARCHAR(30)',
        'RoughnessTTailCenterType': 'VARCHAR(30)',
        'RoughnessTTailDSType': 'VARCHAR(30)',
        'RoughnessBHeadOSType': 'VARCHAR(30)',
        'RoughnessBHeadCenterType': 'VARCHAR(30)',
        'RoughnessBHeadDSType': 'VARCHAR(30)',
        'RoughnessBBodyOSType': 'VARCHAR(30)',
        'RoughnessBBodyCenterType': 'VARCHAR(30)',
        'RoughnessBBodyDSType': 'VARCHAR(30)',
        'RoughnessBTailOSType': 'VARCHAR(30)',
        'RoughnessBTailCenterType': 'VARCHAR(30)',
        'RoughnessBTailDSType': 'VARCHAR(30)',
        'HeadDefectCode': 'VARCHAR(30)',
        'TailScrap': 'VARCHAR(30)',
        'HeadScrap': 'VARCHAR(30)',
        'TailDefectCode': 'VARCHAR(30)',
        'SamplesTaken': 'VARCHAR(30)',
        'PaperUsed': 'VARCHAR(30)',
        'UserID': 'VARCHAR(30)',
        'active': 'INT'
    }

    # Create a new dictionary with only matching key-value pairs
    mergedInspectionDataColumns = {key: value for key, value in mergedInspectionDataColumns.items() if key in df.columns}

    try:
        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        #Truncate table
        query = "DO $$\
                    BEGIN\
                    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'merged_inspection_data') THEN\
                        EXECUTE 'TRUNCATE TABLE merged_inspection_data';\
                    END IF;\
                END $$;"
        cursor.execute(query)

        # Drop table
        cursor.execute('DROP TABLE IF EXISTS merged_inspection_data; COMMIT;')

        # Create a table

        # Initialize an empty string to store the column definitions
        column_definitions = ""

        # Iterate through the dictionary items
        for column_name, data_type in mergedInspectionDataColumns.items():
            # Concatenate the column name and data type
            column_definitions += f"{column_name} {data_type}"

            # Add a comma if it's not the last item
            if column_name != list(mergedInspectionDataColumns.keys())[-1]:
                column_definitions += ", "


        cursor.execute('CREATE TABLE IF NOT EXISTS merged_inspection_data ({}); COMMIT;'.format(column_definitions))
        
        cursor.execute('SELECT * from information_schema.tables WHERE table_schema=\'public\'')
        result = cursor.fetchall()
        print(result)

        # Insert into merged_inspection_data table
        count = 0

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
            
            insert_query = f"INSERT INTO merged_inspection_data ({columns_str}) VALUES ({values_str});"
            cursor.execute(insert_query)

            count += 1
            print(count)


        # Commit
        connection.commit()

        print("Success")

    except:
        print("Database operation failure for merged_inspection_data")

def exportLemmatizedComments(connection, df):

    mergedCoilsDataColumns = {
        "FLInspectionCommentID": "INT",
        "Comments_processed": "VARCHAR(256)",
    }

    # Create a new dictionary with only matching key-value pairs
    mergedCoilsDataColumns = {key: value for key, value in mergedCoilsDataColumns.items() if key in df.columns}


    try:
        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        #Truncate table
        query = "DO $$\
                    BEGIN\
                    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'lemmatized_comments') THEN\
                        EXECUTE 'TRUNCATE TABLE merged_coils_data';\
                    END IF;\
                END $$;"
        cursor.execute(query)

        # Drop table
        cursor.execute('DROP TABLE IF EXISTS lemmatized_comments; COMMIT;')

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

        cursor.execute('CREATE TABLE IF NOT EXISTS lemmatized_comments ({}); COMMIT;'.format(column_definitions))
        
        cursor.execute('SELECT * from information_schema.tables WHERE table_schema=\'public\'')
        result = cursor.fetchall()
        print(result)

        # Insert into merged_coils_data table
        count = 0

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
            
            insert_query = f"INSERT INTO lemmatized_comments ({columns_str}) VALUES ({values_str});"
            cursor.execute(insert_query)

            count += 1
            print(count)


        # Close the connection
        connection.commit()

        print("Success")

    except:
        print("Database operation failure for lemmatized_comments")




#========================================================================
# Error Handling Functions
#========================================================================

def detectErrors():
    # Check if Datasets folder exists before running
    flag = True

    # Initilize database credentials
    credentials = []

    # check if datasets file exit
    if not os.path.exists('./Datasets'):
        flag = False
        print("Need to create a folder for Datasets.")
        
    else:      
        if not os.path.exists('./Datasets/cleanedDatasets'):
            print("Created a folder for Cleaned Datasets.")
            os.makedirs('./Datasets/cleanedDatasets')

        if not os.path.exists('./Datasets/mergedDatasets'):
            print("Created a folder for Merged Datasets.")
            os.makedirs('./Datasets/mergedDatasets')

        if not os.path.exists('./Datasets/lemmatizedComments'):
            print("Created a folder for Lemmatized Comment Data.")
            os.makedirs('./Datasets/lemmatizedComments')

            

    # check if db.conf file exit
    if not os.path.exists('./db.conf'):
        flag = False
        print("Need to create a file name db.conf with credentials.")
    else:

        try:
            # create an instance of the parser
            confP = cp.ConfigParser()

            # read in the configuration file
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

    return flag, credentials



#========================================================================
# NLP
#========================================================================

def NLP():
    try:
        # Download nltk resources (run only once)
        nltk.download('punkt')
        nltk.download('wordnet')
        nltk.download('stopwords')

        # Load the CSV file
        file_path = './Datasets/cleanedDatasets/cleanedFlInspectionCommentsData.csv'
        df = pd.read_csv(file_path)


        # Check the structure of your DataFrame
        #print(df.head())

        # Spell Check, Tokenization, Lemmatization, and Stopword Removal
        lemmatizer = WordNetLemmatizer()
        #spell = spellchecker.SpellChecker()
        stop_words = set(stopwords.words('english'))

        def process_text(text):
            # Spell check
            tokens = word_tokenize(str(text))
            #corrected_tokens = [spell.correction(token) for token in tokens]
            # corrected_tokens = [token for token in tokens]
            # Lemmatization with handling None type
            lemmatized_tokens = [lemmatizer.lemmatize(token) if token is not None else '' for token in tokens]

            # Remove stopwords
            filtered_tokens = [token for token in lemmatized_tokens if token.lower() not in stop_words and token != '']

            return ' '.join(filtered_tokens)

        # Apply processing to the 'Comment' column
        df['Comments_processed'] = df['Comment'].apply(process_text)

        # Save the DataFrame with the new column
        output_file_path = './Datasets/lemmatizedComments/FLInspectionComments_Processed.csv'
        df[['FLInspectionCommentID', 'Comments_processed']].to_csv(output_file_path, index=False)
        df = df[['FLInspectionCommentID', 'Comments_processed']]

        return df
    
    except:
        print("Lemmatization Failed")


#========================================================================
# main program
#========================================================================

def main():

    returnList = detectErrors()
    flag = returnList[0]
    credentials = returnList[1]

    if flag:

        dataframeList = getData()
        cleanedDataframeList = cleanData(dataframeList)
        lemmatizedCommentsDataframe = NLP()
        mergedDataframeList = mergeDatasets(cleanedDataframeList)
        connection = dbConnect(credentials)
        exportCoilsData(connection, mergedDataframeList[0])
        exportInspectionData(connection, mergedDataframeList[1])
        exportLemmatizedComments(connection, lemmatizedCommentsDataframe)

        # Close the connection
        if connection:
            connection.close()
        
main()