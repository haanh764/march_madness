import sys, os
from dash import Dash, html, dcc, Input, Output, ctx, State, ALL
import dash
import plotly.express as px
import pandas as pd
from datetime import datetime, date
import io 
import boto3
import numpy as np
import pandas as pd
from dash.exceptions import PreventUpdate

dash_app = Dash(__name__, suppress_callback_exceptions=True)

'''
===================================================API================================================
'''

AWS_S3_BUCKET_SRC = "march-madness-src"
AWS_S3_BUCKET_TRANSFORMED = "dashboard-march-madness"
FILE_KEY_PATH_SRC = "MDataFiles_Stage2/MSecondaryTourneyTeams.csv"
s3_client = boto3.client("s3")

event_seasons = [2015, 2016, 2017, 2018, 2019]
used_seasons = event_seasons
is_all_time = False
limited_memory=True
data_length = 0

if limited_memory:
    used_seasons = used_seasons[len(used_seasons)-2:]
    data_length = 10000

def _get_csv_file(file_key_path_src=FILE_KEY_PATH_SRC, aws_s3_bucket_src=AWS_S3_BUCKET_SRC):
    response = s3_client.get_object(Bucket=aws_s3_bucket_src, Key=file_key_path_src)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    df = pd.DataFrame(columns=[])
    
    if status == 200:
        print(f"Successful S3 get_object response from {file_key_path_src}. Status - {status}")
        df = pd.read_csv(response.get("Body"))
        if data_length:
            df = df[:data_length]
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    return df


def _get_mevents(season):
    mens_events = []
    seasons = used_seasons
    if season and season in used_seasons:
        seasons = [season]
    for year in seasons: 
        mens_events.append(_get_csv_file(f'2020DataFiles/2020-Mens-Data/MEvents{year}.csv'))
    mevents = pd.concat(mens_events)
    return mevents

'''
===================================================CHARTS================================================
'''

# mock fig
df = px.data.iris() 
fig = px.scatter(df, x="sepal_width", y="sepal_length")

# mevents = _get_mevents(None)
mseasons = _get_csv_file("MDataFiles_Stage2/MSeasons.csv")
mregularseasons = _get_csv_file("MDataFiles_Stage2/MRegularSeasonCompactResults.csv")
mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
mplayers = _get_csv_file("2020DataFiles/2020-Mens-Data/MPlayers.csv")
predictions = _get_csv_file("winner_predictor/predictions/MatchPredictions.csv", "march-madness-models")
team_data = _get_csv_file("winner_predictor/supplementary_data/TeamData.csv", "march-madness-models")
# mtourney_detailed_results = _get_csv_file('2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MNCAATourneyDetailedResults.csv')
mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
mtourney_results = _get_csv_file("MDataFiles_Stage2/MNCAATourneyCompactResults.csv")

meventsteams_transformed = _get_csv_file('mEventsTeams.csv', 'dashboard-march-madness')
mteams_transformed = _get_csv_file('mTeams.csv', 'dashboard-march-madness')
mplayerteams_transformed = _get_csv_file('mPlayerTeams.csv', 'dashboard-march-madness')
mtourneyresultsteams_transformed = _get_csv_file('mTourneyResultsTeams.csv', 'dashboard-march-madness')
mseasonresultsteams_transformed = _get_csv_file('mSeasonResultsTeams.csv', 'dashboard-march-madness')

def _get_most_tournament_wins_chart(season):
    wteam = mtourney_results.rename(columns={'WTeamID':'TeamID'})
    if season:
        wteam = wteam.loc[wteam['Season'] == season]
    wteam_merged =  wteam.merge(mteams, on='TeamID').value_counts().groupby('TeamName').agg('count').to_frame('Count').reset_index()
    wteam_merged = wteam_merged.sort_values(by="Count", ascending=False)[:5]
    fig = px.bar(wteam_merged, x="TeamName", y='Count')
    return fig

def _get_most_championship_wins_chart(season):
    wteam = mtourney_results.rename(columns={'WTeamID':'TeamID'})
    # if season:
    #     wteam = wteam.loc[wteam['Season'] == season]
    wteam_merged =  wteam.merge(mteams, on='TeamID')
    wteam_merged = wteam_merged[wteam_merged['DayNum'] ==154]
    wteam_merged = wteam_merged.value_counts().groupby('TeamName').agg('count').to_frame('Count').reset_index()
    wteam_merged = wteam_merged.sort_values(by="Count", ascending=False)[:5]
    fig = px.bar(wteam_merged, x="TeamName", y='Count')
    return fig

def _get_area_bar_chart(season):
    mevents_area = meventsteams_transformed
    if season and season in used_seasons:
        mevents_area = mevents_area.loc[mevents_area['Season'] == season]
        is_all_time = False
    else:
        is_all_time = True
    mevents_area = mevents_area.value_counts().groupby('Area').agg('count').to_frame('Count').reset_index()
    mevents_area = mevents_area.sort_values(by="Count", ascending=False)[:5]
    fig = px.bar(mevents_area, x="Area", y='Count')
    return fig


'''
===================================================DATA================================================
'''

selected_season = ""
selected_team = ""
selected_player = ""


def _get_seasons():
    seasons = mseasons['Season'].unique()
    seasons = [2015, 2016, 2017, 2018, 2019, 2020] # limited 
    return seasons

def _get_teams(season):
    mregularseasons_teamids = mregularseasons['WTeamID'].combine_first(mregularseasons['LTeamID'])
    mregularseasons['TeamID'] = mregularseasons_teamids
    mregularseasons_merged = mregularseasons.merge(mteams, on='TeamID')
    mregularseasons_merged = mregularseasons_merged.loc[mregularseasons_merged['Season'] == season]
    teams = list(mregularseasons_merged['TeamName'].unique())
    return teams

def _get_players(team_id):
    mplayers_teamid = mplayers.loc[mplayers['TeamID'] == team_id]
    players = mplayers_teamid['FirstName']+" "+mplayers_teamid['LastName']
    return players

def _get_events_statistics(season):    
    mevents_statistics = meventsteams_transformed
    if season and season in used_seasons:
        mevents_statistics = mevents_statistics.loc[mevents_statistics['Season'] == season]
        is_all_time = False
    else:
        is_all_time = True
    mevents_statistics = mevents_statistics.value_counts().groupby('Event Type').agg('count').to_frame('Count').reset_index()
    mevents_statistics= mevents_statistics.sort_values(by='Count', ascending=False)[:5]
    events = dict(zip(mevents_statistics['Event Type'], mevents_statistics.Count))
    stats = [ _get_event_statistic_component(evKey, events[evKey]) for evKey in events.keys()]
    return stats

def _get_event_statistic_component(event, count):
    return html.Div(className="level box", children=[
        html.Div(className="level-left", children=[
            html.Label(className='mr-5', children='1'),
            html.H2(event),
        ]),
        html.Div(className="level-right", children=[
            html.Label(count),
        ])
    ])

def _get_player_events(player_id, season=selected_season): 
    mevents_most = meventsteams_transformed
    if season and season in used_seasons:
        mevents_most = mevents_most.loc[mevents_most['Season'] == season]
        is_all_time = False
    else:
        is_all_time = True
    mevents_merged = mevents_most.loc[mevents_most['EventPlayerID'] == player_id]
    mevents_merged = mevents_merged.value_counts().groupby('Event Type').agg('count').to_frame('Count').reset_index()
    return mevents_merged

def _get_actual_winners():
    games_stats = []
    mtourney_results_winners = mtourney_results
    for row in mtourney_results_winners.to_dict('records'):
        game = {}
        game['Season'] =  row['Season']
        game['DayNum'] = row['DayNum']
        game['TeamID'] = row['WTeamID']
        game['OpponentID'] = row['LTeamID']
        game['Loc'] = row['WLoc']
        game['Won'] = 1
        game['Score'] = row['WScore']
        games_stats.append(game)
        game = {}
        game['Season'] = row['Season']
        game['DayNum'] = row['DayNum']
        game['TeamID'] = row['LTeamID']
        game['OpponentID'] = row['WTeamID']
        game['Loc'] = row['WLoc']
        game['Won']= 0
        game['Score'] = row['LScore']
        games_stats.append(game)
    actual = pd.DataFrame(games_stats)
    actual['Team1'] = actual['TeamID']
    actual['Team2'] = actual['OpponentID']
    actual['Won_actual'] = actual['Won']
    return actual

def _get_predicted_winners():
    predictions_clone = predictions
    predictions_clone['Won_prediction'] = predictions_clone['Team 1 Win percentage'] > predictions_clone['Team 2 Win percentage']
    predictions_clone['Won_prediction'] = predictions_clone['Won_prediction'].astype(int)
    return predictions_clone

def _get_predictions_data(season=selected_season):
    actual_clone = _get_actual_winners()
    predictions_clone = _get_predicted_winners()
    predicted_actual = pd.merge(predictions_clone, actual_clone, on=['Team1','Team2', 'Season', 'DayNum'])
    predicted_actual['Team1Name'] = predicted_actual.merge(mteams,
              how='left',
              left_on='Team1',
              right_on='TeamID')['TeamName']
    predicted_actual['Team2Name'] = predicted_actual.merge(mteams,
              how='left',
              left_on='Team2',
              right_on='TeamID')['TeamName']
    if season:
        predicted_actual = predicted_actual.loc[predicted_actual['Season'] == season]
    predicted_actual_correct = predicted_actual.loc[predicted_actual['Won_actual'] == predicted_actual['Won_prediction']]
    predicted_actual_wrong = predicted_actual.loc[predicted_actual['Won_actual'] != predicted_actual['Won_prediction']]
    return predicted_actual

def _get_predictions_list(season):
    predictions_list = _get_predictions_data(season)[:3]
    predictions_list = predictions_list.T.to_dict().values()
    prediction_components = _get_prediction_component(predictions_list)
    return prediction_components

def _get_prediction_component(predictions_list=[]):
    return [html.Div(className="level box", children=[
                html.Div(className="level-item", children=[
                    html.Div(className="level", children=[
                        html.Div(className="level-item", children=[
                            html.Sup(className="is-size-6", children=f"{round(pred['Team 1 Win percentage'], 2)}%"),
                            html.H3(className="ml-5", children=pred['Team1Name']),
                        ]),
                        html.Div(className="level-item", children=[
                            html.Span(className="circle circle--gray", children='vs')
                        ]),
                        html.Div(className="level-item", children=[
                            html.H3(className="has-text-weight-bold mr-5", children=pred['Team2Name']),
                            html.Sup(className="is-size-6", children=f"{round(pred['Team 2 Win percentage'], 2)}%")
                        ]),
                    ])
                ])
            ]) for pred in predictions_list]
    
'''
===================================================CALLBACKS================================================
'''

@dash_app.callback(
    Output(component_id='season-year', component_property='children'),
    Output(component_id='most-tournament-wins-chart', component_property='figure'),
    Output(component_id='event-statistics-list', component_property='children'),
    Output(component_id='top-winner-predictions-list', component_property='children'),
    Output(component_id='area-bar-chart', component_property='figure'),
    Output(component_id='team-dropdown', component_property='options'),
    Output(component_id='team-dropdown', component_property='value'),
    Output(component_id='player-dropdown', component_property='value'),
    Input(component_id='season-dropdown', component_property='value'),
    State(component_id='event-statistics-list', component_property='children')
)
def _on_season_dropdown_change(value, children):
    selected_season = value
    # _team_options = _get_teams(value)
    # _most_tournament_wins_chart = _get_most_tournament_wins_chart(value)
    # _events_statistics = _get_events_statistics(value) # will get killed because it consumes too much ram 
    # _predictions = _get_predictions_list(value)
    # _area_bar_chart = _get_area_bar_chart(value)
    _most_tournament_wins_chart = fig
    _events_statistics = []
    _predictions = []
    _area_bar_chart = []
    _team_options = []
    return [f'Season {value}', _most_tournament_wins_chart,  _events_statistics, _predictions, _area_bar_chart, _team_options, '', '']


@dash_app.callback(
    Output(component_id='player-dropdown', component_property='options'),
    Input(component_id='team-dropdown', component_property='value'),
    prevent_initial_call=True
)
def _on_team_dropdown_change(value):
    selected_team = value
    if not value:
        print("no team selected----------------------------------------")
        return dash.no_update
    else:
        print("team selected----------------------------------------")
        team_id = mteams.loc[mteams['TeamName'] == value]
        _player_options = _get_players(team_id['TeamID'].values[0])
        return _player_options

@dash_app.callback(
    [Output(component_id='player-stats-1-h3', component_property='children'),
    Output(component_id='player-stats-2-h3', component_property='children'),
    Output(component_id='player-stats-3-h3', component_property='children')],
    Input(component_id='player-dropdown', component_property='value'),
    State(component_id='team-dropdown', component_property='value'),
    prevent_initial_call=True
)
def _on_player_dropdown_change(value, selected_team):
    if not value:
        print("no player selected----------------------------------------")
        return dash.no_update
    else:
        print("player selected----------------------------------------")
        # mplayers_clone = mplayers.merge(mteams, on="TeamID")        
        # mplayers_clone = mplayers_clone.loc[mplayers_clone['TeamName'] == selected_team]
        # mplayers_clone['FullName'] = mplayers_clone['FirstName']+" "+mplayers_clone['LastName']
#         player_id = mplayers_clone.loc[mplayers_clone['FullName'] == value]['TeamID'].values[0]
#         player_events = _get_player_events(player_id, selected_season)
#         most_event = list(player_events.sort_values(by="Count", ascending=False)[:5])
#         if len(most_event):
#             most_event = most_event[0]
#         least_event = list(player_events.sort_values(by="Count", ascending=True)[:5])
#         if len(least_event):
#             least_event = least_event[0]
#         num_event = player_events['Count'].sum()
#         print(most_event, least_event, num_event)
        
#         return [most_event, least_event, num_event]
        return [0, 0, 0]


'''
===================================================TEMPLATES================================================
'''


seasons = _get_seasons()
selected_season = seasons[len(seasons)-1]
teams = _get_teams(selected_season)

header = html.Header(className="hero is-light", children=[
        html.Div(className="hero-head", children=[
            html.Nav(className="nav has-shadow", children=[
                html.Div(className="nav-left", children=[
                    html.Span(className="nav-item", children=[
                        html.Img(className="hero__logo", src=dash_app.get_asset_url("images/logo.png"))
                    ])
                ])
            ])
        ])
])

_body_heading =  html.Div(className="columns", children=[
        html.Div(className="column", children=[
            html.H3(children=datetime.now().strftime("%B %d, %Y")),
            html.H1(className="has-text-weight-bold", children=[
                html.Span(id="season-year"),
            ]),
        ]),
        html.Div(className="column is-3", children=[
             dcc.Dropdown(id="season-dropdown", className="", options=seasons, value=seasons[len(seasons)-1]),
        ])
    ])


_body_stats_row = html.Div(className="columns is-multiline", children=[
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Most Tournament Wins"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[
                    html.Div(children=[
                        dcc.Graph(id="most-tournament-wins-chart", figure=_get_most_tournament_wins_chart(selected_season))
                    ])
                ])
            ])
        ]),
    ]),
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Most Championship Wins All Time"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[
                    dcc.Graph(id="most-championship-wins-chart", figure=_get_most_championship_wins_chart(selected_season))
                ])
            ])
        ]),
    ]),
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Event Statistics"),
            html.Br(),
            html.Div(id="event-statistics-list", children=[])
        ]),
    ])
])

_body_custom_row = html.Div(className="columns is-multiline", children=[
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Top Winner Predictions"),
            html.Br(),
            html.Div(id="top-winner-predictions-list", className="", children=[]),
        ]),
    ]),
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Player Statistics"),
            html.Br(),
            dcc.Dropdown(id="team-dropdown", options=teams, value=" ", placeholder="Select team name..."),
            html.Br(),
            dcc.Dropdown(id="player-dropdown", options=[], value=" ", placeholder="Select player name..."),
            html.Br(),
            html.Div(className="columns", children=[
                html.Div(className="column", children=[
                    html.Div(className="box", children=[
                        html.Span(className="", children="Most move"),
                        html.H3(className="has-text-weight-bold", id="player-stats-1-h3", children="0"),
                    ])
                ]),
                html.Div(className="column", children=[
                    html.Div(className="box", children=[
                        html.Span(className="", children="Least move"),
                        html.H3(className="has-text-weight-bold", id="player-stats-2-h3", children="0"),
                    ])
                ]),
                html.Div(className="column", children=[
                    html.Div(className="box", children=[
                        html.Span(className="", children="Total move"),
                        html.H3(className="has-text-weight-bold", id="player-stats-3-h3", children="0"),
                    ])
                ]),
            ])
        ]),
    ])
])

_body_area_row = html.Div(className="columns", children=[
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Event area"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[
                    dcc.Graph(id="area-bar-chart", figure=_get_area_bar_chart(selected_season))
                ])
            ])
        ]),
    ]),
])

body = html.Div(className="section", children=[
    _body_heading,
    _body_stats_row,
    _body_custom_row,
    _body_area_row
])
    
footer = html.Footer(className="section", children=[])

def _get_layout():
    return html.Div(children=[
        header,
        body,
        footer
    ])
    
    
dash_app.layout = _get_layout


'''
===================================================MAIN================================================
'''

app = dash_app.server.wsgi_app


if __name__ == '__main__':
    selected_season = 2020
    print(_get_teams(2020))
    print(_get_most_tournament_wins_chart(2020))
    print(_get_players('1100'))
    print(_get_events_statistics(2020))
    print(_get_player_events(1000, 2000))
    print(_get_predictions_list(2020))
    print(_on_season_dropdown_change(2020, []))
    print(_on_team_dropdown_change("Illinois"))
    print(_on_player_dropdown_change("Alex Austin", "Illinois"))
    dash_app.run_server(debug=True)