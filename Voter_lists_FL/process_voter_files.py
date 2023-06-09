# -*- coding: utf-8 -*-
"""
Created on Thu Dec  8 14:12:50 2022

@author: Francisco Carrera
voter_analysis.py
"""

import files_sdk
import pandas as pd
import numpy as np
from io import StringIO

# autheniticate
files_sdk.set_api_key("0b4dd5e9ec283c2b3247ca569f2e91830bbbc440dd84a042a8ea763d6fe14e77")
files_sdk.base_url = "https://files.joinupside.com/"

master_voters = pd.DataFrame()
index = 1

# List the voter files and read them in pandas
for f in files_sdk.folder.list_for("Florida Voter Lists/Mar 07 2023/20230307_VoterDetail3-7-23").auto_paging_iter():
    print(f.path)
    with files_sdk.open(f.path, 'rb') as file:
        s = str(file.read(),'utf-8')
        data = StringIO(s) 
        subject = pd.read_csv(data, sep='\t', lineterminator='\r')
        # Remove unnamed, and empty column along with those with 80% more na
        subject = subject.loc[:, ~subject.columns.str.contains('^Unnamed')]
        subject = subject.loc[:, subject.columns != ' ']
        subject.replace(' ', np.nan, inplace = True)
        subject = subject.loc[:, subject.isnull().mean() < .8]
        subject = subject.iloc[:,0:13]
        index = index + 1
    master_voters = pd.concat([master_voters, subject])

#with files_sdk.open("/Florida Voter Lists/Mar 07 2023/20230307_VoterDetail3-7-23/ALA_2023030703162023145419.txt", 'rb') as f:
#    print(f.read())