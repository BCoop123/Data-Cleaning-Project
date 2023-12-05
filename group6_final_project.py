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

#========================================================================
# define functions
#========================================================================

# Check if Datasets folder exists before running
flag = True
url = ""
dbuser = ""
dbpassword = ""

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

        # Access the configuration variables
        url = confP.get("db", "url")
        dbuser = confP.get("db", "dbuser")
        dbpassword = confP.get("db", "dbpassword")
        

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

#========================================================================
# main program
#========================================================================

if flag:

    dataframeList = getData()
    
    print(dataframeList[0].head())
    print(url)