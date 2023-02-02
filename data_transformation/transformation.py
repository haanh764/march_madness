import io
import os
import gc
import numpy as np

import boto3
import pandas as pd

AWS_S3_BUCKET_SRC = "march-madness-src"
AWS_S3_BUCKET_TRANSFORMED = "dashboard-march-madness"

session = boto3.Session(profile_name='default')
s3 = session.resource('s3')
# s3_client = boto3.client(
#     "s3",
#     aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY, aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY
# )
s3_client = session.client('s3')
    
def GetDataFrameFromSrc(FileKeyPathSrc):
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_SRC, Key=FileKeyPathSrc)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        return pd.read_csv(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        
def WriteDataFrameToCsv(df, FILE_KEY_PATH_TRANSFORMED):
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET_TRANSFORMED, Key=FILE_KEY_PATH_TRANSFORMED, Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
            
# Merging events from different years
def ReadMensEventsInRange(rangeStart, rangeEnd):
    mensEvents = []
    for year in range(rangeStart,rangeEnd):
        filepath = "2020DataFiles/2020-Mens-Data/MEvents" + str(year) + ".csv"
        print(filepath)
        df = GetDataFrameFromSrc(filepath)
        mensEvents.append(df)
    gc.collect()
    return mensEvents

def MergeWinningLosingTeamWithDataFrame(destinationDataframe, columnNamesDict):
    df = destinationDataframe.merge(mTeams, left_on='WTeamID', right_on='TeamID', how='left')
    df.rename(columns = {'TeamName':'Winning Team Name'}, inplace=True)

    gc.collect()

    df = df.merge(mTeams, left_on='LTeamID', right_on='TeamID', how='left', suffixes=['WTeam', 'LTeam'])
    df.rename(columns = {'TeamName':'Losing Team Name'}, inplace=True)

    df.drop(columns=['TeamIDWTeam','TeamIDLTeam'], inplace=True)
    df.rename(columns = columnNamesDict, inplace=True)
    return df


#-------------------------------------------------------
#               READING DATA
#--------------------------------------------------------

mTeams = GetDataFrameFromSrc("MDataFiles_Stage2/MTeams.csv") 
mPlayers = GetDataFrameFromSrc("2020DataFiles/2020-Mens-Data/MPlayers.csv")#
mSeasons = GetDataFrameFromSrc("MDataFiles_Stage2/MSeasons.csv") 
mTourneyResults = GetDataFrameFromSrc("MDataFiles_Stage2/MNCAATourneyDetailedResults.csv")#
mSeasonResults = GetDataFrameFromSrc("MDataFiles_Stage2/MRegularSeasonCompactResults.csv")#
#mConference = GetDataFrameFromSrc("MDataFiles_Stage2/Conferences.csv") #NOT USED IN ANALYSIS
#mTeamConference = GetDataFrameFromSrc("MDataFiles_Stage2/MTeamConferences.csv") #NOT USED IN ANALYSIS
#mSeasonResults = GetDataFrameFromSrc("MDataFiles_Stage2/MRegularSeasonDetailedResults.csv") #COMPACT VERSION USED


#-------------------------------------------------------
#               TRANSFORMING DATA
#--------------------------------------------------------

mEvents = pd.concat(ReadMensEventsInRange(2015,2016),ignore_index=True) # LOADIN DATA FROM 2018 ONLY UP TO 2019 DUE TO MEMORY LIMITS!!!
print(mEvents['Area'].head(5))

mEvents['EventType'] = mEvents['EventType'].astype(str)
mEvents['EventSubType'] = mEvents['EventSubType'].astype(str)

# clear nulls and empty values 
mEvents['EventType'].fillna('No type', inplace=True)
mEvents['EventSubType'].fillna('No subtype', inplace=True)

# mEvents.fillna('No value', inplace=True)
# mEvents.fillna(0, inplace=True)
print("Total Null values count: ", mEvents.isnull().sum().sum())

# Cleaning events
eventTypesDict = {'assist' : 'Assist', 
                  'block' : 'Block', 
                  'foul' : 'Foul', 
                  'fouled' : 'Fouled', 
                  'jumpb' : 'Jumpball', 
                  'made1' : 'Made 1 point', 
                  'made2' : 'Made 2 points', 
                  'made3' : 'Made 3 points', 
                  'miss1' : 'Missed 1 point', 
                  'miss2' : 'Missed 2 points', 
                  'miss3' : 'Missed 3 points', 
                  'reb' : 'Rebound', 
                  'steal' : 'Steal', 
                  'sub' : 'Substitution', 
                  'timeout' : 'Timeout', 
                  'turnover' : 'Turnover',
                  '' : 'No type'}

eventSubTypesDict = {'10sec' : '10 second violation',
                     '1of1' : '1 of 1 throws',
                     '1of2' : '1 of 2 throws',
                     '1of3' : '1 of 3 throws',
                     '2of2' : '2 of 2 throws',
                     '2of3' : '2 of 3 throws',
                     '3of3' : '3 of 3 throws',
                     '5sec' : '5 second violation',
                     '3sec' : '3 second violation',
                     'admte' : 'Administrative technical',
                     'alley' : 'Alley oop',
                     'bente' : 'Bench technical',
                     'block' : 'Block tie-up',
                     'bpass' : 'Bad pass turnover',
                     'coate' : 'Coach technical',
                     'comm' : 'Commercial timeout',
                     'deadb' : 'Deadball',
                     'def' : 'Defensive',
                     'defdb' : 'Defensive deadball',
                     'dribb' : 'Dribbling',
                     'drive' : 'Driving layup',
                     'dunk' : 'Dunk',
                     'full' : 'Full timeout',
                     'heldb' : 'Held Ball',
                     'hook' : 'Hook shot',
                     'in' : 'Player entered',
                     'jump' : 'Jump',
                     'lanev' : 'Lane violation',
                     'lay' : 'Layup',
                     'lodge' : 'Lodged Ball',
                     'lostb' : 'Jump ball lost',
                     'nan' : 'No subtype',
                     'off' : 'Offensive',
                     'offdb' : 'Offensive deadball',
                     'offen' : 'Offensive turnover',
                     'offgt' : 'Offensive goaltending',
                     'other' : 'Other subtype',
                     'out' : 'Player exited',
                     'outof' : 'Out of bounds',
                     'outrb' : 'Out of bounds rebound',
                     'pers' : 'Personal foul',
                     'pullu' : 'Pull up jump shot',
                     'short' : 'Short timeout',
                     'shotc' : 'Shot clock violation',
                     'start' : 'Start period',
                     'stepb' : 'Step back jump shot',
                     'tech' : 'Technical foul',
                     'tip' : 'Tip in',
                     'trav' : 'Travelling',
                     'turna' : 'Turn around jump shot',
                     'unk' : 'Unknown subtype',
                     'won' : 'Jump ball won',
                     '' : 'No subtype'}


mEvents['EventType'] = mEvents['EventType'].replace(eventTypesDict)
mEvents['EventSubType'] = mEvents['EventSubType'].replace(eventSubTypesDict)

areasDict = {0: 'Unknown',
            1: 'Under basket',
            2: 'In the paint',
            3: 'Inside right wing',
            4: 'Inside right',
            5: 'Inside center',
            6: 'Inside left',
            7: 'Inside left wing',
            8: 'Outside right wing',
            9: 'Outside right',
            10: 'Outside center',
            11: 'Outside left',
            12: 'Outside left wing',
            13: 'Backcourt'} 

mEvents['AreaName'] = mEvents['Area'].map(areasDict)

del eventTypesDict
del eventSubTypesDict
gc.collect()

# Joining mTeams with mEvents
# Renaming columns in mEvents
columnNamesDict = {'DayNum':'Seasons Day Number',
                   'WTeamID':'Winning Team ID',
                   'LTeamID':'Losing Team ID',
                   'ElapsedSeconds':'Elapsed Seconds',
                   'EventSubType':'Event Subtype',
                   'EventType':'Event Type',
                   'WFinalScore':'Winning Team Final Score',
                   'LFinalScore':'Losing Team Final Score',                   
                   'WCurrentScore':'Winning Team Current Score',
                   'LCurrentScore':'Losing Team Current Score',
                   'FirstD1SeasonWTeam' : 'Winning Team First D1 Season',
                   'LastD1SeasonWTeam' : 'Winning Team Last D1 Season',                   
                   'FirstD1SeasonLTeam' : 'Losing Team First D1 Season',
                   'LastD1SeasonLTeam' : 'Losing Team Last D1 Season'
                   }
mEventsTeams = MergeWinningLosingTeamWithDataFrame(mEvents, columnNamesDict)
print(mEventsTeams['AreaName'][:5])
del mEvents
gc.collect()


df = mEventsTeams.groupby(by=['Winning Team ID', 'Season', 'Seasons Day Number']).sum().groupby(by=['Season']).cumsum()['Winning Team Final Score'].reset_index(name='Winning Team Final Season Score')

mEventsTeams = pd.merge(mEventsTeams, df, on=['Winning Team ID','Season', 'Seasons Day Number'], how='left')

# Cleaning of teams
mTeams.fillna('No value', inplace=True)
mTeams.fillna(0, inplace=True)
print("Total Null values count: ", mTeams.isnull().sum().sum())

# adding won tournaments and seasons
df = mTourneyResults.groupby(by=['WTeamID'])['WTeamID'].count()
mTeams['Matches Won by team in Tournaments'] = mTeams.apply(lambda row: df[row.TeamID] if int(row.TeamID) in df.keys() else 0, axis=1)

df = mSeasonResults.groupby(by=['WTeamID'])['WTeamID'].count()
mTeams['Matches Won by team in Seasons'] = mTeams.apply(lambda row: df[row.TeamID] if int(row.TeamID) in df.keys() else 0, axis=1)

# Cleaning players
mPlayers.fillna('No value', inplace=True)
mPlayers.fillna(0, inplace=True)
print("Total Null values count: ", mPlayers.isnull().sum().sum())

masksDict = ['.',',','`','\'','-']
columnNamesDict = ['LastName','FirstName']

for columnName in columnNamesDict:
    for mask in masksDict:
        mPlayers[columnName] = mPlayers[columnName].str.replace(mask, ' ', regex=False)

del masksDict
del columnNamesDict
gc.collect()

# Joining Players and Teams
columnNamesDict = {'PlayerID':'Player ID',
                   'LastName':'Last Name',
                   'FirstName':'First Name',
                   'TeamID':'Team ID',
                   'TeamName':'Team Name',             
                   'FirstD1Season' : 'Player s First D1 Season',
                   'LastD1Season' : 'Player s First D1 Season'
                  }

mPlayersTeams = mPlayers.merge(mTeams, left_on='TeamID', right_on='TeamID', how='left', suffixes=['WTeam', 'LTeam'])

mPlayersTeams.rename(columns = columnNamesDict, inplace=True)

# Clean mTourneyResults
mTourneyResults.fillna('No value', inplace=True)
mTourneyResults.fillna(0, inplace=True)

# Joining mTourneyResults with mTeams
columnNamesDict = {'DayNum':'Seasons Day Number',
                   'WTeamID':'Winning Team ID',
                   'LTeamID':'Losing Team ID',
                   'WScore':'Winning Team Score',
                   'LScore':'Losing Team Score',
                   'WLoc' : 'Is A Home Team',
                   'NumOT' : 'Number of overtime periods',
                   'FirstD1SeasonWTeam' : 'Winning Team First D1 Season',
                   'LastD1SeasonWTeam' : 'Winning Team First D1 Season',                   
                   'FirstD1SeasonLTeam' : 'Losing Team First D1 Season',
                   'LastD1SeasonLTeam' : 'Losing Team First D1 Season',
                   'WFGM':'Field goals by the winning team',
                   'WFGA':'Field goals attempted by the winning team',
                   'WFGM3':'Three pointers made by the winning team',
                   'WFGA3':'Three pointers attempted by the winning team',
                   'WFTM':'Free throws made by the winning team',
                   'WFTA':'Free throws attempted by the winning team',
                   'WOR':'Offensive rebounds by the winning team',
                   'WDR':'Defensive rebounds by the winning team',
                   }
homeTeamReplacementDict = {'H' : 'Home team', 'A' : 'Away team', 'N' : 'Neutral court'}

mTourneyResultsTeams = MergeWinningLosingTeamWithDataFrame(mTourneyResults, columnNamesDict)
mTourneyResultsTeams['Is A Home Team'] = mTourneyResultsTeams['Is A Home Team'].replace(homeTeamReplacementDict)

# Cleaning mSeasonResults

mSeasonResults = mSeasonResults.fillna('No value')
mSeasonResults = mSeasonResults.fillna(0.0)
print("Total Null values count: ", mSeasonResults.isnull().sum().sum())

# Joining mSeasonResults with mTeams

columnNamesDict = {'DayNum':'Seasons Day Number',
                   'WTeamID':'Winning Team ID',
                   'LTeamID':'Losing Team ID',
                   'WScore':'Winning Team Score',
                   'LScore':'Losing Team Score',
                   'WLoc' : 'Is A Home Team',
                   'NumOT' : 'Number of overtime periods',
                   'FirstD1SeasonWTeam' : 'Winning Team First D1 Season',
                   'LastD1SeasonWTeam' : 'Winning Team First D1 Season',                   
                   'FirstD1SeasonLTeam' : 'Losing Team First D1 Season',
                   'LastD1SeasonLTeam' : 'Losing Team First D1 Season',
                   'WFGM':'Field goals by the winning team',
                   'WFGA':'Field goals attempted by the winning team',
                   'WFGM3':'Three pointers made by the winning team',
                   'WFGA3':'Three pointers attempted by the winning team',
                   'WFTM':'Free throws made by the winning team',
                   'WFTA':'Free throws attempted by the winning team',
                   'WOR':'Offensive rebounds by the winning team',
                   'WDR':'Defensive rebounds by the winning team',
                   }

homeTeamReplacementDict = {'H' : 'Home team', 'A' : 'Away team', 'N' : 'Neutral court'}
mSeasonResultsTeams = MergeWinningLosingTeamWithDataFrame(mSeasonResults, columnNamesDict)
mSeasonResultsTeams['Is A Home Team'] = mSeasonResultsTeams['Is A Home Team'].replace(homeTeamReplacementDict)

#-------------------------------------------------------
#               SAVING DATA
#--------------------------------------------------------
WriteDataFrameToCsv(mEventsTeams, 'mEventsTeams.csv')
WriteDataFrameToCsv(mTeams, 'mTeams.csv')
WriteDataFrameToCsv(mPlayersTeams, 'mPlayerTeams.csv')
WriteDataFrameToCsv(mTourneyResultsTeams, 'mTourneyResultsTeams.csv')
WriteDataFrameToCsv(mSeasonResultsTeams, 'mSeasonResultsTeams.csv')
