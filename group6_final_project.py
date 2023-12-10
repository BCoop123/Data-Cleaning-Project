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

# Clean data that is stored in a dataframe
def cleanData(dataframe):
    df = "" #TODO

    return df

# Merge two datasets given the names, joinCondition, and joinType
def mergeDatasets(dataset1, dataset2, joinCondition, joinType):
    try:
        df = pd.merge({}, {}, on='{}', how='{}').format(dataset1, dataset2, joinCondition, joinType)
        return df
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

        # Create a table
        cursor.execute('CREATE TABLE IF NOT EXISTS defectsTest (defectID int PRIMARY KEY, name varchar(30))')

        cursor.execute('SELECT * from information_schema.tables WHERE table_schema=\'public\'')
        result = cursor.fetchall()
        print(result)

        # Close the connection
        connection.close()

        print("Success")

    except:
        print("Failure")

def cleanPTechCoilsData(df):
        # Print observations before
        print(df.shape)

        # detect if both are empty, null, or NAN.
        df = df[df["CoilId"] != df["BdeCoilId"]]

        # Remove duplicate columns
        df.drop(['Charge'], axis=1, inplace=True)

        # Fix capitilization issues
        df['BdeCoilId'] = df['BdeCoilId'].apply(lambda x: x.upper() if isinstance(x, str) else x)

        # Remove duplicate rows
        df.drop_duplicates(inplace=True)


        # Drop records where length, width, thickness, or weight are 0. This is not possible.
        df = df[df["Length"] != 0]
        df = df[df["Width"] != 0]
        df = df[df["Thickness"] != 0]
        df = df[df["Weight"] != 0]

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


    df = df[df["PeriodLength"] != 0]
    df = df[df["SizeCD"] != 0]
    df = df[df["SizeMD"] != 0]

    # Print observations after
    print(df.shape)

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

    df = df[df["TotalWeightClaimed"] != 0]
    df = df[df["CustomerClaimDefectWeight"] != 0]
    df = df[df["NASIdentifiedDefectWeight"] != 0]
    df = df[df["AreaofResponsibilityDefectWeigh"] != 0]

    # Print observations after
    print(df.shape)

def cleanFlInspectionCommentsData(df):

    # Drop the 'isActive' column as all the value is 1 in the dataset ??



def cleanFlInspectionMappedDefectsData(df):
    pass

def cleanFlInspectionProcessesData(df):
    pass

def cleanFlInspectionData(df):
    # a lot of duplicates data in this datasets (More than 50% data are duplicates)
    # drop the duplications
    df = df.drop_duplicates()

    #check if the columns has null or same values in the entire row
    columns_to_drop = df.columns[df.nunique() == 1]
    #drop those columns that has null or same values in the entire row
    df = df.drop(columns=columns_to_drop)



#========================================================================
# main program
#========================================================================

if flag:

    dataframeList = getData()
    
    #print(dataframeList[0].head())
    
    # Test connecting to the database
    #print(credentials)
    #connection = dbConnect(credentials)
    #testConnection(connection)

    cleanDataset = cleanPTechCoilsData(dataframeList[0])
    cleanClaimsData(dataframeList[2])
