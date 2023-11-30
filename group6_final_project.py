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

def displayMenu():
    print("\n" + "=" * 46)
    print("|" + " " * 20 + "Menu" + " " * 20 + "|")
    print("=" * 46)
    print(f"{'| 1. Merge Datasets':<45}" + "|")
    print(f"{'| 2. Clean Lab6 Data':<45}" + "|")
    print(f"{'| 3. Exit':<45}" + "|")
    print("=" * 46 + "\n")

    option = -1

    #ensures that a valid option is chosen
    while (option < 1) or (option > 3):
        try:
            option = int(input("Please select an option from the menu (1-3): "))
            if (option < 1) or (option > 3):
                print("Invalid Input!")
        except:
            print("Invalid Input!")

    #option that quits the program
    if (option == 3):
        return True

    #executes user chosen option
    if (option == 1):
        mergeDatasets()
    if (option == 2):
        cleanData()

    return False

# Get CSV data
data = pd.read_csv('../../Datasets/AP4_PTec_Coils.csv')
print(data.head())