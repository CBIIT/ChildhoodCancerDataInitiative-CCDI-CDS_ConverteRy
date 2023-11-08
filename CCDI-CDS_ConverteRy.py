#!/usr/bin/env python3

#CCDI-CDS_ConverteRy.py

##############
#
# Env. Setup
#
##############

#List of needed packages
import pandas as pd
import argparse
import argcomplete
import os
import openpyxl
from datetime import date
import warnings
from openpyxl.utils.dataframe import dataframe_to_rows


parser = argparse.ArgumentParser(
                    prog='CCDI-CDS_ConverteRy.py',
                    description='This script will take a CCDI metadata manifest file and converts to a CDS template based on a fixed set of property equivalencies.',
                    )

parser.add_argument( '-f', '--filename', help='CCDI dataset file (.xlsx)', required=True)
parser.add_argument( '-t', '--template', help="CDS dataset template file, CDS_submission_metadata_template.xlsx", required=True)


argcomplete.autocomplete(parser)

args = parser.parse_args()

#pull in args as variables
file_path=args.filename
template_path=args.template


print('\nThe CCDI to CDS conversion has begun.\n\n')


##############
#
# File name rework
#
##############


#Determine file ext and abs path
file_name=os.path.splitext(os.path.split(os.path.relpath(file_path))[1])[0]
file_ext=os.path.splitext(file_path)[1]
file_dir_path=os.path.split(os.path.abspath(file_path))[0]

if file_dir_path=='':
    file_dir_path="."

#obtain the date
def refresh_date():
    today=date.today()
    today=today.strftime("%Y%m%d")
    return today

todays_date=refresh_date()

#Output file name based on input file name and date/time stamped.
output_file=(file_name+
            "_CDS"+
            todays_date) 


##############
#
# Pull Dictionary Page to create node pulls
#
##############

def read_xlsx(file_path: str, sheet: str):
    #Read in excel file
    warnings.simplefilter(action='ignore', category=UserWarning)
    return pd.read_excel(file_path, sheet, dtype=str)


#create workbook
cds_model=pd.ExcelFile(template_path)

#create CDS workbook
cds_df= read_xlsx(cds_model, 'Metadata')

cds_df_dict= read_xlsx(cds_model, "Dictionary")

cds_req_props=cds_df_dict[cds_df_dict['Required'].notna()]['Field'].dropna().unique().tolist()


##############
#
# Read in data
#
##############

#create workbook
ccdi_data=pd.ExcelFile(file_path)

#create dictionary for dfs
ccdi_dfs= {}

#read in dfs and apply to dictionary
for sheet_name in ccdi_data.sheet_names:
    ccdi_dfs[sheet_name]= read_xlsx(ccdi_data, sheet_name)

#nodes to include for CDS conversion
ccdi_to_cds_nodes=['study','study_admin','study_personnel','participant','diagnosis','sample', 
                    'radiology_file', 'sequencing_file', 'clinical_measure_file', 'methylation_array_file', 
                    'cytogenomic_file', 'pathology_file', 'single_cell_sequencing_file']


## Go through each tab and remove completely empty tabs
nodes_removed=[]

for node in ccdi_to_cds_nodes:
    if node in ccdi_dfs:
        #see if the tab contain any data
        test_df=ccdi_dfs[node]
        test_df=test_df.drop('type', axis=1)
        test_df=test_df.dropna(how='all').dropna(how='all', axis=1)
        #if there is no data, drop the node/tab
        if test_df.empty:
            del ccdi_dfs[node]
            nodes_removed.append(node)
    else:
        nodes_removed.append(node)

ccdi_to_cds_nodes = [node for node in ccdi_to_cds_nodes if node not in nodes_removed]


### MERGING OF ALL DATA
# The variable names will be the initials of the node as they are added
# This will show the addition order in hopes to keep the graph walking logic correct and consistent.
# We are opting for this more structured hard coded approach as more fuzzy walking logics tend to fail or eat up memory.

#function to make dropping of columns easy and customizable.
def drop_type_id_others(data_frame,others_list=[]):
    if 'type' in data_frame.columns:
        data_frame=data_frame.drop(['type'], axis=1)
    if 'id' in data_frame.columns:
        data_frame=data_frame.drop(['id'], axis=1)
    if others_list:
        for other in others_list:
            if other in data_frame.columns:
                data_frame=data_frame.drop([other], axis=1)
    return data_frame

#start with study
df_all= drop_type_id_others(ccdi_dfs['study'])

#make note of columns to remap, so that joins are easier
col_remap= {'study.study_id':'study_id', 'participant.participant_id':'participant_id', 'sample.sample_id':'sample_id', 'pdx.pdx_id':'pdx_id', 'cell_line.cell_line_id':'cell_line_id'}

#add study_admin
if 'study_admin' in ccdi_to_cds_nodes:
    df_node=drop_type_id_others(ccdi_dfs['study_admin'],['study.id'])
    df_node.rename(columns=col_remap, inplace=True)
    df_all = df_all.merge(df_node, left_on='study_id', right_on="study_id")

#add study_personnel
if 'study_personnel' in ccdi_to_cds_nodes:
    df_node=drop_type_id_others(ccdi_dfs['study_personnel'],['study.id'])
    df_node.rename(columns=col_remap, inplace=True)
    df_all = df_all.merge(df_node, left_on='study_id', right_on='study_id')

#pull out df for study
df_study_level=df_all

#add participant
if 'participant' in ccdi_to_cds_nodes:
    df_node=drop_type_id_others(ccdi_dfs['participant'],['study.id'])
    df_node.rename(columns=col_remap, inplace=True)
    df_all = df_all.merge(df_node, left_on='study_id', right_on='study_id')

#add diagnosis
if 'diagnosis' in ccdi_to_cds_nodes:
    df_node=drop_type_id_others(ccdi_dfs['diagnosis'],['participant.id'])
    df_node.rename(columns=col_remap, inplace=True)
    df_all = df_all.merge(df_node, left_on='participant_id', right_on='participant_id')

#pull out df for diagnosis
df_participant_level=df_all

#add sample
if 'sample' in ccdi_to_cds_nodes:
    df_node=drop_type_id_others(ccdi_dfs['sample'],['participant.id'])
    df_node.rename(columns=col_remap, inplace=True)
    df_all = df_all.merge(df_node, left_on='participant_id', right_on='participant_id')

#pull out df for sample
df_sample_level=df_all

# In order for CDS to get the most applicable information about diagnosis we will take the duplicate 
# diagnosis information that comes from both sample and diagnosis and combine them into one column.
# Since this duplication of columns occurs on the previous step where the addition of sample was made,
# the sample properties have the '_y' suffix and thus we will combine the '_y' properties first in the
# following section.

for col in df_sample_level.columns.tolist():
    if col.endswith("_x"):
        col_x=col
        col_base=col[:-2]
        col_y=col_base+"_y"
        # print(col_base + " : "+col_x+" : "+col_y)
        df_sample_level[col_base] = df_sample_level[col_y].combine_first(df_sample_level[col_x])
        df_sample_level.drop(columns=[col_x,col_y], inplace=True)


#ALL [node]_file nodes will need to be concatenated first so there are no conflicts on common column names:
# file_name, file_type, dcf_indexd_guid, etc etc etc

df_file=pd.DataFrame()

if 'radiology_file' in ccdi_to_cds_nodes:
    df_file= pd.concat([df_file, ccdi_dfs['radiology_file']], ignore_index=True)

if 'sequencing_file' in ccdi_to_cds_nodes:
    df_file= pd.concat([df_file, ccdi_dfs['sequencing_file']], ignore_index=True)

if 'methylation_array_file' in ccdi_to_cds_nodes:
    df_file= pd.concat([df_file, ccdi_dfs['methylation_array_file']], ignore_index=True)

if 'cytogenomic_file' in ccdi_to_cds_nodes:
    df_file= pd.concat([df_file, ccdi_dfs['cytogenomic_file']], ignore_index=True)

if 'pathology_file' in ccdi_to_cds_nodes:
    df_file= pd.concat([df_file, ccdi_dfs['pathology_file']], ignore_index=True)

if 'single_cell_sequencing_file' in ccdi_to_cds_nodes:
    df_file= pd.concat([df_file, ccdi_dfs['single_cell_sequencing_file']], ignore_index=True)

if 'clinical_measure_file' in ccdi_to_cds_nodes:
    df_file= pd.concat([df_file, ccdi_dfs['clinical_measure_file']], ignore_index=True)

#rename the columns based on the col_remap dictionary made earlier
df_file.rename(columns=col_remap, inplace=True)

#drop off all the extra properties that are not required for transformation into a flattened data frame
df_file=drop_type_id_others(df_file,['study.id', 'participant.id', 'sample.id', 'pdx.id', 'cell_line.id'])

#Remove any partent column that might be there but is completely empty
if 'sample_id' in df_file:
    if len(df_file['sample_id'].dropna().unique().tolist())==0:
        df_file=drop_type_id_others(df_file,['sample_id'])

if 'participant_id' in df_file:
    if len(df_file['participant_id'].dropna().unique().tolist())==0:
        df_file=drop_type_id_others(df_file,['participant_id'])

if 'study_id' in df_file:
    if len(df_file['study_id'].dropna().unique().tolist())==0:
        df_file=drop_type_id_others(df_file,['study_id'])        


#Make data frames to add based on relationships that are present
df_join_sample_add=pd.DataFrame()
df_join_participant_add=pd.DataFrame()
df_join_study_add=pd.DataFrame()


#join on sample for all files that have a sample_id for linking
if 'sample_id' in df_file.columns:
    df_join_sample = df_sample_level.merge(df_file, how= "right", left_on='sample_id', right_on='sample_id')

    #clean up possible duplicates where the sample level outranks file
    for col in df_join_sample.columns.tolist():
        if col.endswith("_x"):
            col_x=col
            col_base=col[:-2]
            col_y=col_base+"_y"
            # print(col_base+" : "+col_x+" : "+col_y)
            df_join_sample[col_base] = df_join_sample[col_x].combine_first(df_join_sample[col_y])
            df_join_sample.drop(columns=[col_x,col_y], inplace=True)

    #remove all rows that do not have a sample_id, this is what sample will add
    df_join_sample_add=df_join_sample[df_join_sample['sample_id'].notna()]


#join on participant for all files that have a participant_id for linking
if 'participant_id' in df_file.columns:
    df_join_participant = df_participant_level.merge(df_file, how= "right", left_on='participant_id', right_on='participant_id')

    #clean up possible duplicates where the participant level outranks file
    for col in df_join_participant.columns.tolist():
        if col.endswith("_x"):
            col_x=col
            col_base=col[:-2]
            col_y=col_base+"_y"
            # print(col_base+" : "+col_x+" : "+col_y)
            df_join_participant[col_base] = df_join_participant[col_x].combine_first(df_join_participant[col_y])
            df_join_participant.drop(columns=[col_x,col_y], inplace=True)

    #remove all rows that do not have a participant_id, this is what participant will add
    df_join_participant_add=df_join_participant[df_join_participant['participant_id'].notna()]

#join on study for all files that have a study_id for linking
if 'study_id' in df_file.columns:
    df_join_study = df_study_level.merge(df_file, how= "right", left_on='study_id', right_on='study_id')

    #clean up possible duplicates where the study level outranks file
    for col in df_join_study.columns.tolist():
        if col.endswith("_x"):
            col_x=col
            col_base=col[:-2]
            col_y=col_base+"_y"
            # print(col_base+" : "+col_x+" : "+col_y)
            df_join_study[col_base] = df_join_study[col_x].combine_first(df_join_study[col_y])
            df_join_study.drop(columns=[col_x,col_y], inplace=True)

    #remove all rows that do not have a study_id, this is what study will add
    df_join_study_add=df_join_study[df_join_study['study_id'].notna()]

#now add all specific data frames together
df_join_all=pd.concat([df_join_sample_add, df_join_participant_add, df_join_study_add], axis=0)


###############
#
#CCDI to CDS required columns hard coding
#
###############

# From the df_join_all data frame, make either 1:1 mappings, rework mappings for different property names
# or transform the data to match the new mapping setups.

#for simple 1:1 mappings even if the property names are different, a simple function to handle it:
def simple_add(cds_prop,ccdi_prop):
    if ccdi_prop in df_join_all:
        cds_df[cds_prop]=df_join_all[ccdi_prop]
    
    return


#study and study modifiers
simple_add('phs_accession','phs_accession')
simple_add('study_acronym','study_acronym')
simple_add('acl','acl')
simple_add('email','email_address')
simple_add('role_or_affiliation','personnel_type')
simple_add('title','study_short_title')


    #NOT REQUIRED in CCDI, but can be derived via logic
if 'authz' in df_join_all.columns:
    cds_df['authz']=df_join_all['authz']
else:
    authz=df_join_all['acl'].unique().tolist()[0]
    authz="['/programs/"+authz[2:]
    cds_df['authz']=authz

        #if there is one experimental value
if len(df_join_all['experimental_strategy_and_data_subtype'].dropna().unique().tolist())==1:
    cds_df['experimental_strategy_and_data_subtype']=df_join_all['experimental_strategy_and_data_subtype']
        #if there are multiple experimental values
elif len(df_join_all['experimental_strategy_and_data_subtype'].dropna().unique().tolist())>1:
    es_and_ds=";".join(df_join_all['experimental_strategy_and_data_subtype'].unique().tolist())
    cds_df['experimental_strategy_and_data_subtype']=es_and_ds
        #if there are no experimental values
elif len(df_join_all['experimental_strategy_and_data_subtype'].dropna().unique().tolist())<1:
    cds_df['experimental_strategy_and_data_subtype']="Sequencing"


        #if there is one study data type value
if len(df_join_all['study_data_types'].dropna().unique().tolist())==1:
    cds_df['study_data_types']=df_join_all['study_data_types']
        #if there are multiple study data type values
elif len(df_join_all['study_data_types'].dropna().unique().tolist())>1:
    es_and_ds=";".join(df_join_all['study_data_types'].unique().tolist())
    cds_df['study_data_types']=es_and_ds
        #if there are no study data type values
elif len(df_join_all['study_data_types'].dropna().unique().tolist())<1:
    cds_df['study_data_types']="Genomics"

        #if there is a study_name
if len(df_join_all['study_name'].dropna().unique().tolist())==1:
    cds_df['study_name']=df_join_all['study_name']
        #if there isn't a study_name
if len(df_join_all['study_name'].dropna().unique().tolist())!=1:
    cds_df['study_name']=df_join_all['study_short_title']

        #if there is a number_of_participants
if len(df_join_all['number_of_participants'].dropna().unique().tolist())==1:
    cds_df['number_of_participants']=df_join_all['number_of_participants']
        #if there isn't a number_of_participants
if len(df_join_all['number_of_participants'].dropna().unique().tolist())!=1:
    cds_df['number_of_participants']=1

        #if there is a number_of_samples
if len(df_join_all['number_of_samples'].dropna().unique().tolist())==1:
    cds_df['number_of_samples']=df_join_all['number_of_samples']
        #if there isn't a number_of_samples
if len(df_join_all['number_of_samples'].dropna().unique().tolist())!=1:
    cds_df['number_of_samples']=1


    #REQUIRED in CCDI, but has to be reworked
personnel_names=df_join_all['personnel_name'].dropna().unique().tolist()

for personnel_name in personnel_names:
    # clear previous entry
    first=None
    middle=None
    last=None

    #create a true/false data frame to determine which rows get the name
    name_apply=(df_join_all['personnel_name']==personnel_name).tolist()

    personnel_name=personnel_name.split(" ")

    prefix_delete=False
    prefixes=['Dr.','Dr','Mr.','Mr','Mrs.','Mrs','Ms.','Ms','Miss','Sir','Dame','Lord','Lady']
    first_name_part = personnel_name[0]
    if first_name_part in prefixes:
        prefix_delete=True

    if prefix_delete:
        del personnel_name[0]


    if len(personnel_name)>2:
        first=personnel_name[0]
        middle=personnel_name[1]
        last=" ".join(personnel_name[2:])
    elif len(personnel_name)==2:
        first=personnel_name[0]
        last=personnel_name[1]
    elif len(personnel_name)==1:
        last=personnel_name[0]

    for x in range(0,len(name_apply)):
        if name_apply[x]:
            cds_df['first_name'][x]=first
            cds_df['middle_name'][x]=middle
            cds_df['last_name'][x]=last

#participant
simple_add('participant_id','participant_id')

#diagnosis
    #Not a perfect match, some logic required depending on version of CCDI
if 'diagnosis_icd_o' in df_join_all.columns:
    cds_df['primary_diagnosis']=df_join_all['diagnosis_icd_o']
elif 'diagnosis_classification' in df_join_all.columns:
    cds_df['primary_diagnosis']=df_join_all['diagnosis_classification']
else:
    print("ERROR: No 'primary_diagnosis' was transfered.")

#sample
simple_add('sample_id','sample_id')

    #anatomic site is the closing approximation we can get
simple_add('sample_type','anatomic_site')

#file
simple_add('file_name','file_name')
simple_add('file_size','file_size')
simple_add('file_type','file_type')
simple_add('file_url_in_cds','file_url_in_cds')
simple_add('instrument_model','instrument_model')
simple_add('library_id','library_id')
simple_add('library_layout','library_layout')
simple_add('library_selection','library_selection')
simple_add('library_source','library_source')
simple_add('library_strategy','library_strategy')
simple_add('md5sum','md5sum')
simple_add('platform','platform')
simple_add('design_description','design_description')


    #Not required in CCDI (further logic needed)
    #If it is there, it gets added, if not it will be changed to "Not Applicable" by later transformation
simple_add('reference_genome_assembly','reference_genome_assembly')

# Not required, but "easy" data adds

if "gender" in df_join_all:
    simple_add('gender','gender')
elif "sex_at_birth" in df_join_all:
    simple_add('gender','sex_at_birth')

simple_add('race','race')
simple_add('ethnicity','ethnicity')
simple_add('bases','number_of_bp')
simple_add('number_of_reads','number_of_reads')
simple_add('avg_read_length','avg_read_length')
simple_add('coverage','coverage')



simple_add('file_mapping_level','file_mapping_level')
simple_add('adult_or_childhood_study','adult_or_childhood_study')
simple_add('organism_species','organism_species')
simple_add('methylation_platform','methylation_platform')
simple_add('reporter_label','reporter_label')
simple_add('age_at_diagnosis','age_at_diagnosis')


#The not applicable transformation that takes any NAs in the data frame and applies "Not Applicable"
#to the fields that are missing this required data.

cds_df[cds_req_props]=cds_df[cds_req_props].fillna("Not Applicable")


##############
#
# Write out
#
##############

print("\nWriting out the CDS workbook file.\n")

template_workbook = openpyxl.load_workbook(template_path)


ws=template_workbook['Metadata']
#remove any data that might be in the template
ws.delete_rows(2, ws.max_row)

#write the data
for row in dataframe_to_rows(cds_df, index=False, header=False):
    ws.append(row)

#save out template
template_workbook.save(f'{file_dir_path}/{output_file}.xlsx')

print(f"\n\nProcess Complete.\n\nThe output file can be found here: {file_dir_path}/{output_file}\n\n")
