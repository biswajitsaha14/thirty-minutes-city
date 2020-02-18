
"""
__author__ = Biswajit Saha

this script generates 30 minutes summary for all the districts
"""



import pandas as pd
from functools import partial
import os

def district_total(df):
    return df.groupby(['district'])['dwelling'].sum().reset_index().rename(columns={'dwelling':'total_dwelling'})


def summary_by_centretype(df,centretype,centrelist):
    if centrelist:
        df= df[df['centre_name'].isin(centrelist)]
    df['centre_type']= centretype
    return df.groupby(['meshblock_id','label','centre_type','waiting_minutes'])['n'].sum().reset_index().drop(columns=['n'])
    

def centrelist_by_type(centretype,df):
    return df[df['centretype'].str.contains(centretype)]['centrename']

#set uup local variables
df_centres =pd.read_csv('centres.csv')
df_meshblocks = pd.read_csv('meshblocks.csv')
df_servicearea = pd.read_csv('servicearea.csv')

strategic_centres = centrelist_by_type('Strategic',df_centres).tolist()
metro_centres = centrelist_by_type('Metro',df_centres).tolist()

df_district_total = district_total(df_meshblocks)

dfs=[]
params =[
    ['Strategic',strategic_centres],
    ['Metro',metro_centres],
    ['Any',[]]
]

summary_by_centretype_partial=partial(summary_by_centretype, df_servicearea)

for param in params:
    dfs.append(summary_by_centretype_partial(*param))

df_t_1 = pd.concat(dfs,axis=0)
df_t_2 = df_t_1.merge(df_meshblocks,left_on ='meshblock_id',right_on='mb_code16')
#for district
df_district = df_t_2.groupby(['district', 'label','centre_type','waiting_minutes'])['dwelling'].sum().reset_index().rename(columns ={'dwelling': 'dwelling_with_access'})
df_district = df_district.merge(df_district_total, on='district')
#for greater Sydney
df_gsr = df_t_2.groupby([ 'label','centre_type','waiting_minutes'])['dwelling'].sum().reset_index().rename(columns ={'dwelling': 'dwelling_with_access'})
df_gsr['district'] = 'Greater Sydney'
df_gsr['total_dwelling'] = df_district_total['total_dwelling'].sum()

#for combined output
df_combined = pd.concat([df_district,df_gsr],axis=0)
df_combined['access_percentage']= df_combined['dwelling_with_access']/df_combined['total_dwelling']
csvfile ='district_30minutes_access_summary.csv'
df_combined.to_csv(csvfile, index=False)
print('output exported :', csvfile)
print('done')
os.startfile(csvfile)

