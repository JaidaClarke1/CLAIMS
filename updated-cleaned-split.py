# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 11:15:00 2022

@author: jaiclarke
"""
import os
from zipfile import ZipFile, is_zipfile
import zipfile
import pandas as pd 



                                                                ############# Part 1: Folders under 100MB ################



#############################################
###### Defining Input and Output Paths ######
#############################################

input_path = r"C:\Users\jaiclarke\Documents\CLAIMS - Consolidate Scripts\KFH CA 2020 - 2021 PPE Project"
input_path2 = r"C:\Users\jaiclarke\Documents\CLAIMS - Consolidate Scripts\Test Folder"
output_file = r"C:\Users\jaiclarke\Documents\CLAIMS - Consolidate Scripts\Output 2"
output_file2 = r"C:\Users\jaiclarke\Documents\CLAIMS - Consolidate Scripts\Output 3"

################################################################################
###### Function to read in the folders and calculate its compressed sizes ######
################################################################################
def nested_zip(zip_path, fh_zip, obj_zip, zip_contents):
    with ZipFile(fh_zip.open(name=obj_zip, mode='r')) as zip_file:
        for nested_item in zip_file.infolist():
            if nested_item.filename.lower().endswith(".zip"):
                nested_zip(zip_path=os.path.join(zip_path, nested_item.filename), fh_zip=zip_file, obj_zip=nested_item, zip_contents=zip_contents)
            else:
                zip_contents.append([nested_item.filename, zip_path, nested_item.file_size, nested_item.compress_size]) #nested_item.compress_size)*(1e+8)#, ])

    return(zip_contents) 

def walk_zip(zip_file):
    zip_contents = []
    if zip_file and is_zipfile(zip_file):
        with ZipFile(file=zip_file, mode='r') as ro_zip:
            for item in ro_zip.infolist():
                if item.filename.lower().endswith(".zip"):
                    nested_zip(zip_path=os.path.join(zip_file, item.filename), fh_zip=ro_zip, obj_zip=item, zip_contents=zip_contents)
                else:
                    zip_contents.append([item.filename, zip_file, item.file_size, item.compress_size])

    return(zip_contents)

if __name__ == "__main__":
    dir_contents = []
    for root, dirs, files in os.walk(input_path):
        for f in files:
            file_stats = os.stat(os.path.join(root, f))
            if is_zipfile(os.path.join(root, f)):
                zip_contents = walk_zip(zip_file=os.path.join(root, f))
                if zip_contents:
                    dir_contents.extend(zip_contents)
                    
#####################################################################
###### Initial dataframe that shows information for every file ###### 
#####################################################################                   
  
        df = pd.DataFrame(data=None, index=None,columns=('Filename', 'FilePath', 'LogicalSize', 'CompressedLogicalSize', 'parentfolder','recordtype','compressedmb'), dtype=None, copy=None)
       
        ziplist = []
        if dir_contents:
            if output_file:
                    for f in dir_contents:
                        #print("f[0]= " +  f[0])
                        f.append((f[0]).split('/')[1])
                        if f[3]>0:
                            f.append('file')
                        else: 
                            f.append('folder')
                        f.append(f[3]*1e-6)
                        df.loc[len(df)] = f
                        ziplist.append(f)
                        
                        
            else:
                for f in dir_contents:
                        print(f'{f[0]} ({f[2]}): {f[1]}')

###################################################################
###### Organizing Dataframe to only pull wanted information ####### 
###################################################################       
        
        df_edit = pd.DataFrame(df.Filename.str.split('/').tolist(), columns= "PO ParentFolder IndividualFile".split()) # change made
        df['PO'] = pd.Series(df_edit['PO']) 
        first_column = df.pop('PO') 
        df.insert(0, 'PO', first_column)
        df2 = df.groupby(['PO'])['compressedmb'].sum().reset_index() 
        df2 = df2.iloc[0:, :]
        df2 = df2.reset_index(drop=True)
        

production_folder_path_list = os.listdir(input_path2) # holds the PO names for each expenditure
folderSize = 0.0  # Variable that will be used to track when a zip group hits 100MB
slidingWindow = [0]     # List to determine the "Start" variable
folderSizeTrackerList = [] # List that shows each folder size added up, until 100MB is hit
groupedExpendatureFoldersList = [] # Shows which expenditures will be placed in a group together
expendatureFolderSizeList = [] # List that will identify if an expenditure folder is over or under 100MB
start_end_index_tracker = [] # Identify the indexes to start and end at when grouping
sizeTagger = "" # Tag whether an expenditure is greater or less than 100MB

#############################################################################################                    
##### Convert df2 to 2 lists (List of expenditures and list of compressed folder size) ######
#############################################################################################

zipExpenditureList = df2['PO'].tolist()
zipCompressedList = df2['compressedmb'].tolist()

FullPathNameList = [] # Full paths to expenditure folders
groupedFullPathNameList = [] # List that will be used to show expenidutre groupings and full paths to each expenditure in the group


#####################################################
###### Finding full paths to each expenditure #######
#####################################################

for expID in production_folder_path_list:
    FullPathNameList.append(input_path2 + "\\" + expID + "\\")

# Get the Folder size of each Expenditure ID
for expenditureID_index in range(len(zipExpenditureList)):
    
    # Get the full path
    fullPath = input_path2 + "\\" + zipExpenditureList[expenditureID_index] + "\\"
    
    # Append the Folder Size List
    if(zipCompressedList[expenditureID_index] > 100):
        sizeTagger = "+100MB"
    else:
        sizeTagger = "-100MB"
    
    # List that shows if an expenditure folder, alone, is greater or less than 100MB
    expendatureFolderSizeList.append([zipExpenditureList[expenditureID_index], zipCompressedList[expenditureID_index], sizeTagger])
    
############################################################################
###### Separating the folders under 100MB from the folders over 100MB ######
############################################################################

under_100 = []
over_100 = []
FullPathNameList_under = []

# If folder size tagger is "-100MB" then add it to the under_100 list    
for i in range(len(expendatureFolderSizeList)):
    if(expendatureFolderSizeList[i][2] == "-100MB"):
       under_100.append(expendatureFolderSizeList[i])
    # if folder size tagger is "+100MB" then add it to the over_100MB list   
    elif(expendatureFolderSizeList[i][2] == "+100MB"):
           over_100.append(expendatureFolderSizeList[i])
           
           
under_df = pd.DataFrame(under_100, columns=['PO', 'Size', 'Size Tagger']) # DF that contains info for all expenditures UNDER 100MB
production_folder_path_list = under_df['PO'].tolist() # PO names for expenditures under 100MB
zipExpenditureList = under_df['PO'].tolist() 

#################################################################################################
###### Separating the full paths for expenditures under 100MB from expenditures over 100MB ######
#################################################################################################

for po_under100 in range(len(FullPathNameList)):
    for i in range(len(under_100)):
        if under_100[i][0] in FullPathNameList[po_under100]:
            FullPathNameList_under.append(FullPathNameList[po_under100])
    
#####################################################################################################
###### Identifying which expenditures can be grouped together until the 100MB threshold is hit ######
#####################################################################################################

for i in range(len(under_df['Size'])):
    folderSize = folderSize + under_df['Size'][i] # adding up the folder sizes for each expenditure folder
    folderSizeTrackerList.append(float(folderSize)) # appending these summations to a tracker list
 
    if (folderSize > 100): # if the grouping size hits/goes over 100MB
        slidingWindow.append(i) 
        
        # Reset the Folder Size List
        folderSize -= folderSizeTrackerList[i] 
        
         # Define the "start" and "end" index of the sliding window       
        start = slidingWindow[-2] 
        end = slidingWindow[-1] - 1
        start_end_index_tracker.append([start,end])
        
        # Group Expenditure Folder in batches
        folderSizeTrackerList[i] = under_df['Size'][i]
        folderSize = folderSize + folderSizeTrackerList[i]
        groupedExpendatureFoldersList.append(zipExpenditureList[start:end+1])
        groupedFullPathNameList.append(FullPathNameList_under[start:end+1])
        
######################################################################################

    # If we hit the last index (or last Expenditure Folder), then we need to group everything 
    # from the last index to the index following the grouping before
    elif(i == len(production_folder_path_list)-1):
        # Append the sliding window
        slidingWindow.append(i)     
        
         # Define the "start" will be +1 index in the most recents Groupings "END" index    
        start = start_end_index_tracker[-1][-1] + 1
        end = i + 1
        
        # Group Expenditure Folder in batches
        start_end_index_tracker.append([start, end])
        groupedExpendatureFoldersList.append(zipExpenditureList[start:end])
        groupedFullPathNameList.append(FullPathNameList_under[start:end])
        
##########################################
###### Create the Zip Folder Names #######
##########################################

groupedZipFolderNameList = [] # The names of each zipped group, once outputted

# Capture the Production Site Name which sits in the last index of the file path
prodFolderName = input_path2.split("\\")[-1]

# Create the Zip Folder Group Name list
for i in range(len(groupedExpendatureFoldersList)):
    groupedZipFolderNameList.append(prodFolderName+"_"+str(i))    

#################################################################
###################### Define the Zip function ##################
#################################################################
def zipit(folders, zip_filename):
    zip_file = zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED)

    for folder in folders:
        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                zip_file.write(
                    os.path.join(dirpath, filename),
                    os.path.relpath(os.path.join(dirpath, filename), os.path.join(folders[0], "../..")))

    zip_file.close()
#################################################################
############### Zip Up Each grouped Zip Folders #################
#################################################################    
zipGroupCounter = 0

for group in groupedFullPathNameList:
    groupToZip = group
 
    zipit(groupToZip, output_file + "\\" + groupedZipFolderNameList[zipGroupCounter] + ".zip") 
    
    print("STATUS - ", zipGroupCounter, "/" , len(groupedZipFolderNameList)-1)
    print(groupedZipFolderNameList[zipGroupCounter] + " -- ZIP COMPLETE")  
    
    zipGroupCounter +=1
    
print(" ---------------------- ALL ZIPS -100MB COMPLETE ---------------------- ") 

                                               
                                                ################ Part 2: Expenditures over 100MB ####################
                                                

folderSize = 0.0
slidingWindow = [0]     
folderSizeTrackerList= []
groupedExpendatureFoldersList = []
expendatureFolderSizeList = []
start_end_index_tracker = []
groupedFullPathNameList = []
FullPathNameList = []
groupedFullPathNameList = []

for root, dirs, files in os.walk(input_path2):
    for name in files:
        FullPathNameList.append(root + "\\" + name) # This FullPathNameList is different from the first, as it is now looking at the file level instead of the folder level

###############################################################
###### Organizing information for all folders over 100MB ######
###############################################################

over_df = pd.DataFrame(over_100,columns=['PO', 'Size', 'Size Tagger'] ) 
production_folder_path_list = over_df['PO'].tolist()

#########################################################################################
###### Obtaining full paths (at the file level) for expenditure folders over 100MB ######
#########################################################################################

FullPathNameList_over = []
for po_over100 in range(len(FullPathNameList)):
    for i in range(len(over_100)):
        if over_100[i][0] in FullPathNameList[po_over100]: # if the PO from our over_100 list is found anywhere in our FullPathNameList
            FullPathNameList_over.append(FullPathNameList[po_over100]) # Take that full path and append it to FullPathNameList_over

#######################################################################
####### Pulling file level info for expenditure folders over 100MB ######
#######################################################################

over_df = pd.merge(df, over_df, on=['PO'], how='inner') 
over_df = over_df.drop(columns=['LogicalSize', 'CompressedLogicalSize', 'Size', 'Size Tagger'])
over_df = over_df[over_df.compressedmb != 0].reset_index() 
zipExpenditureList = over_df['Filename'].tolist()
zipExpenditureList = ["C:\\Users\\jaiclarke\\Documents\\CLAIMS - Consolidate Scripts\\Test Folder\\"+x.replace("/","\\") for x in zipExpenditureList]

zipCompressedList = over_df['compressedmb'].tolist()
production_folder_path_list = over_df['Filename'].tolist()

#####################################################################################################
###### Identifying which files can be grouped together until the 100MB threshold is hit ######
#####################################################################################################

for i in range(len(over_df['compressedmb'])):
    folderSize = folderSize + over_df['compressedmb'][i] # adding up the file sizes 
    folderSizeTrackerList.append(float(folderSize)) # appending these summations to a tracker list

###########################################################################    
    if (folderSize > 100): # if the grouping size hits/goes over 100MB
        slidingWindow.append(i) 
        
        # Reset the Folder Size List
        folderSize -= folderSizeTrackerList[i] 
        
         # Define the "start" and "end" of the sliding window       
        start = slidingWindow[-2] 
        end = slidingWindow[-1] - 1
        start_end_index_tracker.append([start,end])
        
        # Group Expenditure Folder in batches
        folderSizeTrackerList[i] = over_df['compressedmb'][i]
        folderSize = folderSize + folderSizeTrackerList[i]
        groupedExpendatureFoldersList.append(zipExpenditureList[start:end+1])
        groupedFullPathNameList.append(FullPathNameList_over[start:end+1])
######################################################################################

    # If we hit the last index (or last Expdniture Folder), then we need to group everything 
    # from the last index to the index following the grouping before
    elif(i == len(production_folder_path_list)-1):
        # Append the sliding window
        slidingWindow.append(i)     
        
         # Define the "start" will be +1 index in the most recents Groupings "END" index    
        start = start_end_index_tracker[-1][-1] + 1
        end = i + 1
        
        # Group Expenditure Folder in batches
        start_end_index_tracker.append([start, end])
        groupedExpendatureFoldersList.append(zipExpenditureList[start:end])
        groupedFullPathNameList.append(FullPathNameList_over[start:end])
#################################3END ###########################
     
# Create the Zip Folder Names
groupedZipFolderNameList = []

# Capture the Production Site Name which sits in the last index of the file path
prodFolderName = input_path2.split("\\")[-1]

# Create the Zip Folder Group Name list
for i in range(len(groupedExpendatureFoldersList)):
    groupedZipFolderNameList.append(prodFolderName+"_"+str(i))    
    
#################################################################
###################### Define the Zip function ##################
#################################################################

def zipit(folders, zip_filename, root_folder):
    zip_file = zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED)
     
    for files in folders:
        zip_file.write(
            filename=files,
            arcname= os.path.relpath(files, root_folder),
        )      
                    
    zip_file.close()
    
#################################################################
############### Zip Up Each grouped Zip Folders #################
#################################################################    
zipGroupCounter = 0

for group in groupedExpendatureFoldersList:
    groupToZip = group
 
    zipit(groupToZip, output_file2 + "\\" + groupedZipFolderNameList[zipGroupCounter] + ".zip","C:\\Users\\jaiclarke\\Documents\\CLAIMS - Consolidate Scripts\\Test Folder\\" ) 
    
    print("STATUS - ", zipGroupCounter, "/" , len(groupedZipFolderNameList)-1)
    print(groupedZipFolderNameList[zipGroupCounter] + " -- ZIP COMPLETE")  
    
    zipGroupCounter +=1
    
print(" ---------------------- ALL ZIPS +100MB COMPLETE ---------------------- ")     
