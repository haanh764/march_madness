from model import *
from utils import *
import os

import torch
import joblib

import io
import boto3

AWS_S3_BUCKET_SRC = "march-madness-src"
AWS_S3_BUCKET_MODELS = "march-madness-models"

FILE_KEY_PATH_SRC = "MDataFiles_Stage2/"
FILE_KEY_PATH_MODELS = "winner_predictor/"


def predict_row(row, team_data_df, model, scaler):
    team1 = row[1]['WTeamID']
    team2 = row[1]['LTeamID']
    season = row[1]['Season']
    day_num = row[1]['DayNum']
    city = row[1]['CityID']
    A = row[1]['A']
    H = row[1]['H']
    N = row[1]['N']
    team1_data = team_data_df[team_data_df['Team'] == team1]
    team1_data.columns = use_columns_t1
    team2_data = team_data_df[team_data_df['Team'] == team2]
    team2_data.columns = use_columns_t2
    data_row = []
    data_row.extend([season, day_num, city])
    data_row.extend(team1_data.values.tolist()[0])
    data_row.extend(team2_data.values.tolist()[0])
    data_row.extend([A, H, N])
    x = torch.Tensor(data_row)
    x = scaler.transform(x.reshape(1, -1))
    x = torch.Tensor(x)
    prediciton = model.predict_step((x, None), None)
    indexes_winners = list(range(8, 20))
    indexes_losers = list(range(20, 33))
    index = team_data_df[team_data_df['Team'] == team1].index
    team_data_df.loc[index, win_columns] = team_data_df.loc[index, win_columns] + ([1] + [row[1].loc['WScore']] + [row[1].loc['NumOT']] + row[1].iloc[indexes_winners].to_list() + [row[1].loc['LScore']] + [row[1].loc['NumOT']] + row[1].iloc[indexes_losers].to_list())
    index = team_data_df[team_data_df['Team'] == team2].index
    team_data_df.loc[index, lose_columns] = team_data_df.loc[index, lose_columns] + ([1] + [row[1].loc['LScore']] + [row[1].loc['NumOT']] + row[1].iloc[indexes_losers].to_list() + [row[1].loc['WScore']] + [row[1].loc['NumOT']] + row[1].iloc[indexes_winners].to_list())
    return prediciton, team_data_df


def predict_match(s3_client, team1, team2, season, city, day_num, loc):
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"supplementary_data/TeamData.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        team_data_df = pd.read_csv(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"model/scaler.gz")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        scaler = joblib.load(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    model = Model(len(use_columns_t1)+len(use_columns_t2) + 6)

    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"model/model.pth")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        model.load_state_dict(torch.load(response.get("Body")))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    model.eval()
    A = loc == 'A'
    H = loc == 'H'
    N = loc == 'N'
    team1_data = team_data_df[team_data_df['Team'] == team1]
    team1_data.columns = use_columns_t1
    team2_data = team_data_df[team_data_df['Team'] == team2]
    team2_data.columns = use_columns_t2
    data_row = []
    data_row.extend([season, day_num, city])
    data_row.extend(team1_data.values.tolist()[0])
    data_row.extend(team2_data.values.tolist()[0])
    data_row.extend([A, H, N])
    x = torch.Tensor(data_row)
    x = scaler.transform(x.reshape(1, -1))
    x = torch.Tensor(x)
    prediciton = model.predict_step((x, None), None)
    
    return prediciton

def handle_new_data(timestamp):
    s3_client = boto3.client(
        "s3"
    )
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_SRC, Key=FILE_KEY_PATH_SRC+"MRegularSeasonDetailedResults_{" + str(timestamp) + "}.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        matches_df = pd.read_csv(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_SRC, Key=FILE_KEY_PATH_SRC+"MGameCities.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        cities_df = pd.read_csv(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    matches_df = pd.merge(matches_df, cities_df,  how='left', left_on=['Season', 'DayNum', 'WTeamID', 'LTeamID'], right_on =['Season', 'DayNum', 'WTeamID', 'LTeamID'])
    matches_df = pd.concat([matches_df, pd.get_dummies(matches_df.WLoc)], axis=1)
    wanted_columns = list(matches_df.columns)
    wanted_columns.remove('WLoc')
    matches_df = matches_df.loc[:, wanted_columns]


    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"supplementary_data/TeamData.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        team_data_df = pd.read_csv(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"model/scaler.gz")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        scaler = joblib.load(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    model = Model(len(use_columns_t1)+len(use_columns_t2) + 6)

    response = s3_client.get_object(Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"model/model.pth")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        model.load_state_dict(torch.load(response.get("Body")))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    model.eval()

    predictions = []
    for row in matches_df.iterrows():
        prediction, team_data_df = predict_row(row, team_data_df, model, scaler)
        predictions.append(prediction)


    with io.StringIO() as csv_buffer:
        team_data_df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"supplementary_data/TeamData.csv", Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
    
    predictions = torch.Tensor([float(p.detach()) * 100 for p in predictions])

    out_df = matches_df.loc[:, ['Season', 'DayNum', 'WTeamID', 'LTeamID']]
    out_df.columns = ['Season', 'DayNum', 'Team1', 'Team2']
    out_df = pd.concat([out_df, pd.DataFrame(predictions, columns=['Team 1 Win percentage']), pd.DataFrame(100 - predictions, columns=['Team 2 Win percentage'])], axis=1)

    with io.StringIO() as csv_buffer:
        out_df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET_MODELS, Key=FILE_KEY_PATH_MODELS+"predictions/MatchPredictions_{" + str(timestamp) + "}.csv", Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")

