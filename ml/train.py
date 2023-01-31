#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os

import torch
import joblib
from torch.utils.data import DataLoader

from pytorch_lightning.callbacks.early_stopping import EarlyStopping
from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning import seed_everything
import pytorch_lightning as pl

import pandas as pd
import numpy as np
import random

import io
import boto3

from model import *
from utils import *

AWS_S3_BUCKET_SRC = "march-madness-src"
AWS_S3_BUCKET_MODELS = "march-madness-models"

FILE_KEY_PATH_SRC = "MDataFiles_Stage2/"
FILE_KEY_PATH_MODELS = "winner_predictor/"


# In[ ]:


def aggregate_data_up_to_match(results_df, team_id, season, day_num):
    data_df_all = results_df[(results_df['Season'] < season) | ((results_df['Season'] == season) & (results_df['DayNum'] < day_num))]
    aggragated_data = []
    lengths = []
    for side in ['w', 'l']:
        if side == 'w':
            data_df = data_df_all[data_df_all['WTeamID'] == team_id]
            data_df = data_df.loc[:, ['WScore', 'NumOT', 'WFGM', 'WFGA', 'WFGM3', 'WFGA3', 'WFTM', 'WFTA', 'WOR', 'WDR', 'WAst', 'WTO', 'WStl', 'WBlk', 'WPF', 'LScore', 'LFGM', 'LFGA', 'LFGM3', 'LFGA3', 'LFTM', 'LFTA', 'LOR', 'LDR', 'LAst', 'LTO', 'LStl', 'LBlk', 'LPF']]
        else:
            data_df = data_df_all[data_df_all['LTeamID'] == team_id]
            data_df = data_df.loc[:, ['LScore', 'NumOT', 'LFGM', 'LFGA', 'LFGM3', 'LFGA3', 'LFTM', 'LFTA', 'LOR', 'LDR', 'LAst', 'LTO', 'LStl', 'LBlk', 'LPF', 'WScore', 'WFGM', 'WFGA', 'WFGM3', 'WFGA3', 'WFTM', 'WFTA', 'WOR', 'WDR', 'WAst', 'WTO', 'WStl', 'WBlk', 'WPF']]
        lengths.append(len(data_df))
        new_col_names = ['Score', 'NumOT', 'FGM', 'FGA', 'FGM3', 'FGA3', 'FTM', 'FTA', 'WOR', 'DR', 'Ast', 'TO', 'Stl', 'Blk', 'PF', 'EScore', 'EFGM', 'EFGA', 'EFGM3', 'EFGA3', 'EFTM', 'EFTA', 'EWOR', 'EDR', 'EAst', 'ETO', 'EStl', 'EBlk', 'EPF']
        new_col_names = [side+col for col in new_col_names]
        data_df.columns = new_col_names
        data_df = data_df.aggregate(['sum'])
        aggragated_data.append(data_df)
    return pd.concat(aggragated_data, axis=1), lengths


# In[ ]:


seed_everything(42)


# In[ ]:


s3_client = boto3.client(
    "s3"
)


# In[ ]:


response = s3_client.get_object(Bucket=AWS_S3_BUCKET_SRC, Key=FILE_KEY_PATH_SRC+"MNCAATourneyDetailedResults.csv")
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    tor_results_df = pd.read_csv(response.get("Body"))
else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")

response = s3_client.get_object(Bucket=AWS_S3_BUCKET_SRC, Key=FILE_KEY_PATH_SRC+"MRegularSeasonDetailedResults.csv")
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    season_results_df = pd.read_csv(response.get("Body"))
else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")

response = s3_client.get_object(Bucket=AWS_S3_BUCKET_SRC, Key=FILE_KEY_PATH_SRC+"MGameCities.csv")
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    cities_df = pd.read_csv(response.get("Body"))
else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")

response = s3_client.get_object(Bucket=AWS_S3_BUCKET_SRC, Key=FILE_KEY_PATH_SRC+"MNCAATourneySeeds.csv")
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    tor_seeds_df = pd.read_csv(response.get("Body"))
else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")


# In[ ]:


results_df = pd.concat([season_results_df, tor_results_df])
results_df = pd.merge(results_df, cities_df,  how='left', left_on=['Season', 'DayNum', 'WTeamID', 'LTeamID'], right_on =['Season', 'DayNum', 'WTeamID', 'LTeamID'])
results_df = pd.concat([results_df, pd.get_dummies(results_df.WLoc)], axis=1)
wanted_columns = list(results_df.columns)
wanted_columns.remove('WLoc')
results_df = results_df.loc[:, wanted_columns]


# In[ ]:


train_data_start_season = 2006
data_df = results_df[results_df['Season'] >= train_data_start_season]
data = []
team_column_names = None
for row in data_df.iterrows():
    row_data = []
    city = row[1]['CityID']
    team1 = row[1]['WTeamID']
    team2 = row[1]['LTeamID']
    A = row[1]['A']
    H = row[1]['H']
    N = row[1]['N']
    row_data.append(team1)
    season = row[1]['Season']
    day_num = row[1]['DayNum']
    team1_row, lengths = aggregate_data_up_to_match(results_df, team1, season, day_num)
    team1_row = team1_row.stack()
    team1_wining_length = lengths[0]
    team1_losing_length = lengths[1]
    team2_row, lengths = aggregate_data_up_to_match(results_df, team2, season, day_num)
    team2_row = team2_row.stack()
    team2_wining_length = lengths[0]
    team2_losing_length = lengths[1]
    if team_column_names is None:
        team_column_names = team1_row.reset_index()
        team_column_names = (team_column_names['level_0'] + team_column_names['level_1']).to_list()
    row_data.append(season)
    row_data.append(day_num)
    row_data.append(city)
    if random.randint(0, 1):
        row_data.append(team1)
        row_data.append(team1_wining_length)
        row_data.append(team1_losing_length)
        row_data.extend(team1_row.values.tolist())
        row_data.append(team2)
        row_data.append(team2_wining_length)
        row_data.append(team2_losing_length)
        row_data.extend(team2_row.values.tolist())
        row_data.append(A)
        row_data.append(H)
        row_data.append(N)
    else:
        row_data.append(team2)
        row_data.append(team2_wining_length)
        row_data.append(team2_losing_length)
        row_data.extend(team2_row.values.tolist())
        row_data.append(team1)
        row_data.append(team1_wining_length)
        row_data.append(team1_losing_length)
        row_data.extend(team1_row.values.tolist())
        row_data.append(H)
        row_data.append(A)
        row_data.append(N)
    data.append(row_data)
columns = ['Result', 'Season', 'DayNum', 'CityID']
for t in ['1', '2']:
    columns.append('Team'+t)
    columns.append('Wining_length'+t)
    columns.append('Losing_length'+t)
    columns.extend([t + col for col in team_column_names])
columns.extend(['A', 'H', 'N'])
examples_dataframe = pd.DataFrame(data, columns=columns)
examples_dataframe['Result'] = examples_dataframe['Result'] == examples_dataframe['Team1']


# In[ ]:


df = examples_dataframe
df = df.fillna(0)
seasons = sorted(np.unique(df.Season).tolist())
assert(len(seasons) > 5)
df_train = df[df['Season'] <= seasons[-5]]
df_dev = df[(df['Season'] <= seasons[-3]) & (df['Season'] > seasons[-5])]
df_test = df[df['Season'] >= seasons[-2]]


# In[ ]:


teams = set()
[teams.add(e) for e in np.unique(df['Team1']).tolist()]
[teams.add(e) for e in np.unique(df['Team2']).tolist()]


# In[ ]:


team_data_df = pd.DataFrame(columns=new_columns)
team_data = []
for team in teams:
    for row in df[::-1].iterrows():
        if team == row[1]['Team1']:
            team_data.append(row[1].loc[use_columns_t1].values)
            break
        elif team == row[1]['Team2']:
            team_data.append(row[1].loc[use_columns_t2].values)
            break


# In[ ]:


team_data_df = pd.DataFrame(team_data, columns=new_columns)


# In[ ]:


with io.StringIO() as csv_buffer:
    team_data_df.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"winner_predictor/supplementary_data/TeamData.csv", Body=csv_buffer.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")


# In[ ]:


batch_size = 128
max_epochs = 300
patience = 15


# In[ ]:


train_dataset = PandasDataset(df_train)
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

scaler = train_dataset.scaler
with io.StringIO() as buffer:
    joblib.dump(scaler, buffer)

    response = s3_client.put_object(
        Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"winner_predictor/model/scaler.gz", Body=csv_buffer.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")


val_dataset = PandasDataset(df_dev, scaler)
val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)

test_dataset = PandasDataset(df_test, scaler)
test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)


# In[ ]:


model_path = 'models'
filename = 'model'


# In[ ]:


model = Model(train_dataset.input_size)


# In[ ]:


monitor = "val_accuracy"
mode = "max"

early_stop_callback = EarlyStopping(monitor=monitor,
                                    min_delta=0.001,
                                    patience=patience,
                                    strict=False,
                                    verbose=False,
                                    check_finite=False,
                                    mode=mode)
checkpoint_callback = ModelCheckpoint(
    save_top_k=1,
    monitor=monitor,
    mode=mode,
    dirpath=model_path,
    filename=filename,)


# In[ ]:


trainer = pl.Trainer(devices=[0], accelerator="gpu", max_epochs=max_epochs, gradient_clip_val=None, accumulate_grad_batches=None, callbacks=[checkpoint_callback, early_stop_callback])


# In[ ]:


trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=val_loader)


# In[ ]:


post_train_scores_test = trainer.test(model, dataloaders=test_loader, ckpt_path=os.path.join(model_path, filename) + '.ckpt')
print(post_train_scores_test)


# In[ ]:


with io.StringIO() as buffer:
    torch.save(model.state_dict(), buffer)

    response = s3_client.put_object(
        Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"winner_predictor/model/model.pth", Body=csv_buffer.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")


# In[ ]:


predictions = torch.Tensor(model.predictions) * 100
test_df = df_test.loc[:, ['Season', 'DayNum', 'Team1', 'Team2']]
predicitions_df = pd.concat([test_df, pd.DataFrame(predictions, columns=['Team 1 Win percentage']), pd.DataFrame(100 - predictions, columns=['Team 2 Win percentage'])], axis=1)

with io.StringIO() as csv_buffer:
    predicitions_df.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"winner_predictor/predictions/MatchPredictions.csv", Body=csv_buffer.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")

