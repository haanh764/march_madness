import os
import sys
import pandas as pd
import numpy as np

from utils.api import get_csv_file

def get_most_tournament_wins_chart():
    mteams = get_csv_file("MDataFiles_Stage2/MTeams.csv")
    mseasons = get_csv_file("MDataFiles_Stage2/MSeasons.csv")
    mtourney_seed = get_csv_file("MDataFiles_Stage2/MNCAATourneySeeds.csv")
    mseason_results = get_csv_file("MDataFiles_Stage2/MRegularSeasonCompactResults.csv")
    mtourney_results = get_csv_file("MDataFiles_Stage2/MNCAATourneyCompactResults.csv")
    conference = get_csv_file("MDataFiles_Stage2/Conferences.csv")
    team_conference = get_csv_file("MDataFiles_Stage2/MTeamConferences.csv")
    
    print(mtourney_results) 


