# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 11:04:25 2022

@author: Francisco Carrera
deal_note_analysis.py
"""

# Import libraries
import re
import string
import pandas as pd
import numpy as np
import sys
from nltk.tokenize.simple import LineTokenizer
from nltk.tokenize import RegexpTokenizer
from notes_utils import hubspot_deal_note_extract, hubspot_deal_details, hubspot_contact_details

# Import the Hubspot token
key = pd.read_csv('~/Documents/Upside/Encrypt/ETL_App_Token_Hubspot.txt', sep=" ", header=None)
key = key.iloc[0,0]

# Contacts from Hubspot
notes = hubspot_deal_note_extract(key)

# Preprocess notes text - remove html and lower case
notes["Notes_Processed"] = [re.sub(r'<.*?>'," ", str(note).lower()) for note in notes["Note"]]
# convert 24/7 to alternate format
notes["Notes_Processed"] = [re.sub(r'24/7',"twentyfourseven", note) for note in notes["Notes_Processed"]]
# Remove dates
notes["Notes_Processed"] = [re.sub(r'\d+[./]\d+[./]\d+',"", note) for note in notes["Notes_Processed"]]
notes["Notes_Processed"] = [re.sub(r'\d+[./]\d+',"", note) for note in notes["Notes_Processed"]]
# Remove punctuation and extra spaces
notes["Notes_Processed"] = [note.translate(str.maketrans('', '', string.punctuation)) for note in notes["Notes_Processed"]]
notes["Notes_Processed"] = [re.sub(r"\s\s+", " ", note.strip()) for note in notes["Notes_Processed"]]

# Read Upside+ Trackers
trackers = pd.read_csv("upsideplus_tags.csv")

# Count the occurrences of the trackers
for tracker in trackers.Tags:
    word_counts = [note.count(tracker) for note in notes["Notes_Processed"]]
    notes[tracker + "_" + "mentions"] = word_counts

# Pivot at the contact level
notes_tally_piv = notes.pivot_table(index = "Contact_id",
                                            values= ['nurse_mentions', 'on staff_mentions', 'more help_mentions',
       'home care_mentions', 'more care_mentions', 'medical care_mentions',
       'respite care_mentions', 'around the clock_mentions',
       'medical staff_mentions', 'twentyfourseven_mentions',
       'traditional_mentions', 'caregiver_mentions', 'on site_mentions'], aggfunc=np.sum).reset_index()

# Get the contacts
contacts = hubspot_contact_details(key)
contacts_full_prop = pd.DataFrame()
# Get the contact properties
for index, contact in contacts.iterrows():
    contacts_full_prop = pd.concat([contacts_full_prop,pd.DataFrame(contacts.properties[index], index = [0])], ignore_index = True)
# Select properties of interest
contacts_full_prop = contacts_full_prop.loc[:,['hs_object_id','memory_care_needed',"mobility","taking_medication_", "bathing_assistance_needed_",
                     "desired_location_s__multipicklist","family_referred_to",'specified_budget']]
contacts_full_prop.columns = ['Contact_id','Memory Care',"mobility","medication", "Bathing Assistance",
                     "Location","Referral Location",'Budget']
contacts_full_prop.dropna(subset=['Memory Care',"mobility","medication", "Bathing Assistance",
                     "Location","Referral Location",'Budget'], how = 'all', axis = 0, inplace = True)

# Process the leads
contacts_full_prop["Lower_Budget"] = [int(re.sub("\$|or Over|,","",re.sub("-(.*)","", val))) if val != "No Budget Entered" and val != None \
                                      else np.nan for val in contacts_full_prop.Budget]
# Filter leads < 3000
contacts_full_prop = contacts_full_prop[contacts_full_prop["Lower_Budget"] >= 3000]
# Get only those in boca raton
contacts_full_prop.Location.fillna("Unknown", inplace = True)
contacts_full_prop["Referral Location"].fillna("Unknown", inplace = True)
contacts_full_prop = contacts_full_prop[(contacts_full_prop["Location"].str.contains('boca raton|davie|dania|deerfield|lauderdale|Miramar|Plantation|Pompano|Palm|Boynton|Coconut|Coral Springs', flags=re.IGNORECASE)) |
                                        (contacts_full_prop["Referral Location"].str.contains('Boca Raton|Deerfield|Davie|Lauderdale|Miramar|Plantation|Pompano|Palm|Boynton|Coconut|Coral Springs'))]


# Join with the notes depending on the keys available
contacts_notes = contacts_full_prop.merge(notes_tally_piv, how = 'left', on = 'Contact_id')
contacts_notes["mobility_assist"] = [1 if val != 'Completely self-sufficient' and val != 'Independent' and val != None else 0 for val in contacts_notes.mobility]
contacts_notes["medication_assist"] = [1 if val != 'No' and val != 'No assistance needed' and \
                                       val != 'Yes - they can take medications on their own' and val != None else 0 for val in contacts_notes.medication]
contacts_notes["bathing_assist"] = [1 if val != 'No' and val != 'No assistance needed' and val != None else 0 for val in contacts_notes["Bathing Assistance"]]
contacts_notes["memory_assist"] = [1 if val != 'No' and val != 'None' and val != None else 0 for val in contacts_notes["Memory Care"]]

# Mark budget as string
contacts_notes.drop("Lower_Budget", axis = 1, inplace = True)

# Sum all numeric columns
contacts_notes["Upside_plus_score"] = contacts_notes.sum(axis=1, numeric_only = True)

# Mark the segment
contacts_notes["segment"] = "Upside+"
contacts_notes.to_csv("upside+_segment_v2.csv")