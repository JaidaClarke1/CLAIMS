# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 12:27:03 2022

@author: jaiclarke
"""
from PyPDF2 import PdfFileWriter, PdfFileReader, PdfFileMerger
from pathlib import Path
import pandas as pd
import os

########################
#### Pulling in CSV ####
########################
csv = "Sample Page Range Data.csv"
export = pd.read_csv(r"C:\Users\jaiclarke\Desktop\CLAIMS\Sample Data Files" + "\\" + csv) # reading in csv as a dataframe
export.drop(columns=["Related Expenditures", "Meets FEMA Requirements (Y-N)"], inplace = True) # dropping non-essential columns

#####################
#### Changing WD ####
#####################
controlNumFolder = r"C:\Users\jaiclarke\Desktop\CLAIMS\Sample Data Files" # path to where all of the pdf files are stored
os.chdir(controlNumFolder) # changing working directory to where the pdfs are stored

####################################################
#### Storing needed columns into iterable lists ####
####################################################
controlNumberList = export["Control Number"].tolist() 
start = export["Page Start"].tolist()
end = export["Page End"].tolist()
editedControlNum = [] 


##########################################################################
#### Loop 1: Adding ".pdf" to the end of the imported Control Numbers ####
##########################################################################
for i in controlNumberList: # for every element in ControlNumberList
    editedControlNum.append(i + ".pdf") # add ".pdf" to the end of it
## Explanation: This is so that Python can search through the file correctly and not run into errors because of the missing ".pdf" at the end


################################################################
#### Loop 2: Delcaring the start and end pages for each pdf ####
################################################################
page_start = "0"
page_end = "0"

for pageRange in range(len(start)): # for every element in the start list
    page_start = start[pageRange] # start on the page number that is in the "start" list
    page_end = end[pageRange] # end on the page number that is in the corresponding "end list"
    page_start = str(page_start) # convert page start to a string
    page_end = str(page_end) # convert page end to a string
    
    
    sample = editedControlNum[pageRange] # names of the pdf files in the iteration
    input_pdf = PdfFileReader(sample) # passing the name of the variable that holds the pdf files
    pdf_write = PdfFileWriter() # creating memory/space for pdfs to go into (will be used later in script)
    
 
###################################################################
##### Loop 3 (Nested): Placing that start/end pages into list #####
###################################################################
    x = [page_start + "-" + page_end] # list of start and end pages
    for items in x: 
        if "-" in items: # if there is a dash in the number of pages needing to be split (Ex: Excel file says 8-12 instead of pages being in separate columns)
            x1 = items.split('-') # split the start and end pages based on the dash
            print(x1)
            x2 = list(map(int, x1)) # mapping an integer to every element in x1 list
            print(x2)
            
#######################################################################################
#### Loop 4 (Nested): Splitting the pages and adding them to the correspoding PDFs ####
#######################################################################################
            for i in range (x2[0]-1, x2[1]): # for the range of x until y (since Python starts at 0, we call x2[0]-1 so that the program is iteration is starting on the correct page)
                print(i)
                page = input_pdf.getPage(i) # matching the pdf names to the specific page numbers in the iteration
                pdf_write.addPage(page) # adding the pages to the pdf names
                
        #else:
            #page = input_pdf.getPage(int(items)-1)
            #print(page)
            #pdf_write.addPage(page)
    
    with Path(r"C:\Users\jaiclarke\Desktop\CLAIMS\Sample Data Files\Output" + "\\" + "Doc-Split_" + editedControlNum[pageRange]).open(mode="wb") as output_file: # writing to output file
        
        pdf_write.write(output_file)