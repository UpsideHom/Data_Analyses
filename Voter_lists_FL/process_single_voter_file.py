# -*- coding: utf-8 -*-
"""
Created on Thu Dec  8 14:12:50 2022

@author: Francisco Carrera
voter_analysis_single.py
"""

import files_sdk
import pandas as pd
import numpy as np
from io import StringIO
import datetime
import re

# autheniticate
files_sdk.set_api_key("0b4dd5e9ec283c2b3247ca569f2e91830bbbc440dd84a042a8ea763d6fe14e77")
files_sdk.base_url = "https://files.joinupside.com/"

# Collect header file
headers = pd.read_csv('colnames.csv')

# Read and upload voter file of choice - Broward County
with files_sdk.open('/Florida Voter Lists/Mar 07 2023/20230307_VoterDetail3-7-23/BRO_2023030703162023150317.txt', 'rb') as file:
    s = str(file.read(),'utf-8')
    data = StringIO(s) 
    master_voters = pd.read_csv(data, sep='\t', lineterminator='\r', header = None)
    
    
# Attach the headers and filter for Boca Raton
master_voters.columns = list(headers.Voter_registry_cols.values)
master_boca_1 = master_voters[master_voters["Residence City (USPS)"] == "Ft Lauderdale"]

# Calculate age
master_boca_1["Birth Date"] = pd.to_datetime(master_boca_1["Birth Date"])

# Define a lambda function to calculate the difference in years between a date and today's date
years_difference = lambda date: len(pd.date_range(date, datetime.date.today(), freq='Y'))

master_boca_1["Age"] = master_boca_1["Birth Date"].apply(years_difference)

# Gather only those people that are 65+
master_boca_sample = master_boca_1[master_boca_1.Age >= 65]

# Filter those in apt
master_boca_sample = master_boca_sample[master_boca_sample["Residence Address Line 2"].str.contains("APT")]

# Create contacts out of these people
def format_phone_number(number):
    # Remove any non-digit characters
    number = re.sub(r'\D', '', number)
    
    # Format the number
    return '({}{}{}) {}{}{}-{}{}{}{}'.format(*number)

master_boca_sample["Phone"] = master_boca_sample["Daytime Area Code"] + master_boca_sample["Daytime Phone Number"]
master_boca_sample["Formatted_phone"] = [format_phone_number(number) if isinstance(number, str) else np.nan for number in master_boca_sample["Phone"]]

# Normalize email
master_boca_sample["Email address"] = [email.lower() for email in master_boca_sample["Email address"]]

# Exclude any contacts without email or phone
final_boca_sample = master_boca_sample[(master_boca_sample["Email address"] != " ") | (master_boca_sample["Formatted_phone"].notnull())]

# Final formatting
final_boca_sample_extract = final_boca_sample.loc[:,["Name First","Name Middle", "Name Last", "Name Suffix",
                                                     "Residence Address Line 1", "Residence Address Line 2", "Residence Zipcode",
                                                     "Residence City (USPS)","Gender","Age","Formatted_phone", "Email address"]]
# Trim address spaces
final_boca_sample_extract["Residence Address Line 1"] = [re.sub("\s+"," ",ad) for ad in final_boca_sample_extract["Residence Address Line 1"]]
final_boca_sample_extract["Residence Address Line 2"] = [re.sub("\s+"," ",ad).strip() for ad in final_boca_sample_extract["Residence Address Line 2"]]
# Normalize Zip codes
final_boca_sample_extract["Residence Zipcode"] = [ad[:5] for ad in final_boca_sample_extract["Residence Zipcode"]]

# Export
final_boca_sample_extract.to_csv("Sample_ft_lauderdale_March_23_voter_list.csv")



