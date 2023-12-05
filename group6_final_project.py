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

#========================================================================
# define functions
#========================================================================

# Get the data contained in all of the CSV files used for this project
def getData():
    # Get CSV data
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

# Clean data that is stored in a dataframe
def cleanData(dataframe):
    df = "" #TODO

    return df

# Merge two datasets given the names, joinCondition, and joinType
def mergeDatasets(dataset1, dataset2, joinCondition, joinType):
    df = pd.merge({}, {}, on='{}', how='{}').format(dataset1, dataset2, joinCondition, joinType)

    return df

#========================================================================
# main program
#========================================================================

dataframeList = getData()

print(dataframeList[0].head())