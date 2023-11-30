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
    data = pd.read_csv('../../Datasets/AP4_PTec_Coils.csv')
    print(data.head())

getData()