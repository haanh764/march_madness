import sys, os
from dash import Dash, html, dcc, Input, Output, ctx, State, ALL
import dash
import plotly.express as px
import pandas as pd
from datetime import datetime, date
import io 
import boto3
import pandas as pd
from dash.exceptions import PreventUpdate

dash_app = Dash(__name__, suppress_callback_exceptions=True)

'''
===================================================API================================================
'''

AWS_S3_BUCKET_SRC = "march-madness-src"
FILE_KEY_PATH_SRC = "MDataFiles_Stage2/MSecondaryTourneyTeams.csv"
s3_client = boto3.client("s3")

def _get_csv_file(file_key_path_src=FILE_KEY_PATH_SRC, aws_s3_bucket_src=AWS_S3_BUCKET_SRC):
    response = s3_client.get_object(Bucket=aws_s3_bucket_src, Key=file_key_path_src)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    df = pd.DataFrame(columns=[])
    
    if status == 200:
        print(f"Successful S3 get_object response from {file_key_path_src}. Status - {status}")
        df = pd.read_csv(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    return df

'''
===================================================CHARTS================================================
'''

# mock fig
df = px.data.iris() 
fig = px.scatter(df, x="sepal_width", y="sepal_length")


def _get_most_tournament_wins_chart(season):
    mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
    mtourney_results = _get_csv_file("MDataFiles_Stage2/MNCAATourneyCompactResults.csv")
    wteam = mtourney_results.rename(columns={'WTeamID':'TeamID'})
    if season:
        wteam = wteam.loc[wteam['Season'] == season]
    wteam_merged =  wteam.merge(mteams, on='TeamID').value_counts().groupby('TeamName').agg('count').to_frame('Count').reset_index()
    wteam_merged = wteam_merged.sort_values(by="Count", ascending=False)[:5]
    fig = px.bar(wteam_merged, x="TeamName", y='Count')
    fig.show()
    return fig


def _get_most_championship_wins_chart(season):
    mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
    mtourney_results = _get_csv_file("MDataFiles_Stage2/MNCAATourneyCompactResults.csv")
    wteam = mtourney_results.rename(columns={'WTeamID':'TeamID'})
    # if season:
    #     wteam = wteam.loc[wteam['Season'] == season]
    wteam_merged =  wteam.merge(mteams, on='TeamID')
    wteam_merged = wteam_merged[wteam_merged['DayNum'] ==154]
    wteam_merged = wteam_merged.value_counts().groupby('TeamName').agg('count').to_frame('Count').reset_index()
    wteam_merged = wteam_merged.sort_values(by="Count", ascending=False)[:5]
    fig = px.bar(wteam_merged, x="TeamName", y='Count')
    fig.show()
    return fig


'''
===================================================DATA================================================
'''

selected_season = ""
selected_team = ""
selected_player = ""

def _get_seasons():
    mseasons = _get_csv_file("MDataFiles_Stage2/MSeasons.csv")
    seasons = mseasons['Season'].unique()
    return seasons

def _get_teams(season):
    mregularseasons = _get_csv_file("MDataFiles_Stage2/MRegularSeasonCompactResults.csv")
    mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
    mregularseasons_teamids = mregularseasons['WTeamID'].combine_first(mregularseasons['LTeamID'])
    mregularseasons['TeamID'] = mregularseasons_teamids
    mregularseasons_merged = mregularseasons.merge(mteams, on='TeamID')
    mregularseasons_merged = mregularseasons_merged.loc[mregularseasons_merged['Season'] == season]
    teams = list(mregularseasons_merged['TeamName'].unique())
    return teams

def _get_players(team_id):
    mplayers = _get_csv_file("2020DataFiles/2020-Mens-Data/MPlayers.csv")
    mplayers = mplayers.loc[mplayers['TeamID'] == team_id]
    players = mplayers['FirstName']+" "+mplayers['LastName']
    return players

def _get_events_statistics(season):    
    mevents = _get_mevents(season)
    mevents = mevents.value_counts().groupby('EventType').agg('count').to_frame('Count').reset_index()
    mevents= mevents.sort_values(by='Count', ascending=False)[:5]
    events = dict(zip(mevents.EventType, mevents.Count))
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

def _get_player_most_events(player_id, season=selected_season):    
    mevents = _get_mevents(season)
    mplayers = _get_csv_file(f'2020DataFiles/2020-Mens-Data/MPlayers.csv')
    mevents_merged = mevents.merge(mplayers,
              how='left',
              left_on='EventPlayerID',
              right_on='PlayerID')
    mevents_merged = mevents_merged.loc[mevents_merged['PlayerID'] == player_id]
    mevents_merged = mevents_merged.value_counts().groupby('EventType').agg('count').to_frame('Count').reset_index()
    mevents_merged= mevents_merged.sort_values(by='Count', ascending=False)[:5]
    event_types = list(mevents_merged['EventType'])
    return event_types

def _get_mevents(season, limited_memory=True):
    mens_events = []
    event_seasons = [2015, 2016, 2017, 2018, 2019]
    if season and season in event_seasons:
        event_seasons = [season]
    if limited_memory:
        event_seasons = [event_seasons[0]]
    for year in event_seasons: 
        mens_events.append(_get_csv_file(f'2020DataFiles/2020-Mens-Data/MEvents{year}.csv'))
    mevents = pd.concat(mens_events)
    return mevents
    
def _get_player_least_events(player_id, season=selected_season):
    mevents = _get_mevents(season)
    mplayers = _get_csv_file(f'2020DataFiles/2020-Mens-Data/MPlayers.csv')
    mevents_merged = mevents.merge(mplayers,
              how='left',
              left_on='EventPlayerID',
              right_on='PlayerID')
    mevents_merged = mevents_merged.loc[mevents_merged['PlayerID'] == player_id]
    mevents_merged = mevents_merged.value_counts().groupby('EventType').agg('count').to_frame('Count').reset_index()
    mevents_merged= mevents_merged.sort_values(by='Count', ascending=True)[:5]
    event_types = list(mevents_merged['EventType'])
    return event_types

def _get_player_num_events(player_id, season=selected_season):
    mevents = _get_mevents(season)
    mplayers = _get_csv_file(f'2020DataFiles/2020-Mens-Data/MPlayers.csv')
    mevents_merged = mevents.merge(mplayers,
              how='left',
              left_on='EventPlayerID',
              right_on='PlayerID')
    mevents_merged = mevents_merged.loc[mevents_merged['PlayerID'] == player_id]
    mevents_merged = mevents_merged.value_counts().groupby('EventType').agg('count').to_frame('Count').reset_index()
    total = mevents_merged['Count'].sum()
    return total

def _get_actual_winners():
    games_stats = []
    mtourney_detailed_results = _get_csv_file('2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MNCAATourneyCompactResults.csv')
    for row in mtourney_detailed_results.to_dict('records'):
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
    predictions = _get_csv_file("winner_predictor/predictions/MatchPredictions.csv", "march-madness-models")
    team_data = _get_csv_file("winner_predictor/supplementary_data/TeamData.csv", "march-madness-models")
    predictions['Won_prediction'] = predictions['Team 1 Win percentage'] > predictions['Team 2 Win percentage']
    predictions['Won_prediction'] = predictions['Won_prediction'].astype(int)
    return predictions

def _get_predictions_data(season=selected_season):
    actual = _get_actual_winners()
    predictions = _get_predicted_winners()
    predicted_actual = pd.merge(actual, predictions, on=['Team1','Team2', 'Season', 'DayNum'])
    if season:
        predicted_actual = predicted_actual.loc[predicted_actual['Season'] == season]
    predicted_actual_correct = predicted_actual.loc[predicted_actual['Won_actual'] == predicted_actual['Won_prediction']]
    predicted_actual_wrong = predicted_actual.loc[predicted_actual['Won_actual'] != predicted_actual['Won_prediction']]
    return predicted_actual
    
'''
===================================================CALLBACKS================================================
'''

@dash_app.callback(
    Output(component_id='season-year', component_property='children'),
    Output(component_id='most-tournament-wins-chart', component_property='figure'),
    Output(component_id='event-statistics-list', component_property='children'),
    Output(component_id='team-dropdown', component_property='options'),
    Output(component_id='team-dropdown', component_property='value'),
    Output(component_id='player-dropdown', component_property='value'),
    Input(component_id='season-dropdown', component_property='value'),
    State(component_id='event-statistics-list', component_property='children')
)
def _on_season_dropdown_change(value, children):
    selected_season = value
    _team_options = _get_teams(value)
    _most_tournament_wins_chart = _get_most_tournament_wins_chart(value)
    _event_statistics = _get_events_statistics(value)
    return [f'Season {value}', _most_tournament_wins_chart,  _event_statistics, _team_options, '', '']


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
        mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
        team_id = mteams.loc[mteams['TeamName'] == value]
        print(mteams.loc[mteams['TeamName'] == value])
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
        mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
        mplayers = _get_csv_file("2020DataFiles/2020-Mens-Data/MPlayers.csv")

        mplayers = mplayers.merge(mteams, on="TeamID")        
        mplayers = mplayers.loc[mplayers['TeamName'] == selected_team]
        mplayers['FullName'] = mplayers['FirstName']+" "+mplayers['LastName']
        player_id = mplayers.loc[mplayers['FullName'] == value]['TeamID'].values[0]
        
        most_event = _get_player_most_events(player_id, selected_season)[0]
        least_event = _get_player_least_events(player_id, selected_season)[0]
        num_event = _get_player_num_events(player_id, selected_season)
        
        return [most_event, least_event, num_event]


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
                    dcc.Graph(id="most-championship-wins-chart", figure=_get_most_championship_wins_chart(None))
                ])
            ])
        ]),
    ]),
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Event Statistics"),
            html.Br(),
            html.Div(id="event-statistics-list", children=[
                html.Div(className="level box", children=[
                    html.Div(className="level-left", children=[
                        html.Label(className='mr-2',children='1'),
                        html.H2("event name"),
                    ]),
                    html.Div(className="level-right", children=[
                        html.Label("90"),
                    ])
                ])
            ])
        ]),
    ])
])

_body_custom_row = html.Div(className="columns is-multiline", children=[
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Top Winner Predictions"),
            html.Br(),
            html.Div(className="level box", children=[
                html.Div(className="level-item", children=[
                    html.Div(className="level", children=[
                        html.Div(className="level-item", children=[
                            html.Sup(className="is-size-6", children="80%"),
                            html.H2(className="ml-5", children="Team 1"),
                        ]),
                        html.Div(className="level-item", children=[
                            html.Span(className="circle circle--gray", children="vs")
                        ]),
                        html.Div(className="level-item", children=[
                            html.H2(className="has-text-weight-bold mr-5", children="Team 1"),
                            html.Sup(className="is-size-6", children="80%")
                        ]),
                    ])
                ])
            ]),
            html.Div(className="level box", children=[
                html.Div(className="level-item", children=[
                    html.Div(className="level", children=[
                        html.Div(className="level-item", children=[
                            html.Sup(className="is-size-6", children="80%"),
                            html.H2(className="ml-5", children="Team 1"),
                        ]),
                        html.Div(className="level-item", children=[
                            html.Span(className="circle circle--blue", children="1 : 1")
                        ]),
                        html.Div(className="level-item", children=[
                            html.H2(className="has-text-weight-bold mr-5", children="Team 1"),
                            html.Sup(className="is-size-6", children="80%")
                        ]),
                    ])
                ])
            ]),
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
                    dcc.Graph(figure=fig)
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
    _on_season_dropdown_change(2020, [])
    _predictions = _get_predictions_data(2019)
    print(_predictions)
    print(_on_player_dropdown_change("Alex Austin", "Illinois"))
    dash_app.run_server(debug=True)