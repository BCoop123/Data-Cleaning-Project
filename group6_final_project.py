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

def getData():
    # Get CSV data
    pTechCoilsData = pd.read_csv('../../Datasets/AP4_PTec_Coils.csv')
    pTechDefectMapsData = pd.read_csv('../../Datasets/AP4_Ptec_Defect_Maps_10-coils.csv')
    claimsData = pd.read_csv('../../Datasets/claims_2023-05.csv')
    flInspectionCommentsData = pd.read_csv('../../Datasets/FLInspectionComments.csv')
    flInspectionMappedDefectsData = pd.read_csv('../../Datasets/tblFlatInspectionMappedDefects.csv')
    flInspectionProcessesData = pd.read_csv('../../Datasets/tblFlatInspectionProcesses.csv')
    flInspectionData = pd.read_csv('../../Datasets/tblFLInspection.csv')

    return [
        pTechCoilsData,
        pTechDefectMapsData,
        claimsData,
        flInspectionCommentsData,
        flInspectionMappedDefectsData,
        flInspectionProcessesData,
        flInspectionData
    ]

list = getData()

print(list[0].head())