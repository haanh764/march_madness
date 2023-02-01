import sys, os
from dash import Dash, html, dcc, Input, Output, ctx, State, ALL
import plotly.express as px
import pandas as pd
from datetime import datetime, date
import io 
import boto3
import pandas as pd

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
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_csv(response.get("Body"), error_bad_lines=False)
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    return df

'''
===================================================CHARTS================================================
'''

# mock fig
df = px.data.iris() 
fig = px.scatter(df, x="sepal_width", y="sepal_length")

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
    events = [{"name": "test", "occurences": 100}]
    # do something with season
    return [html.Div(className="level box", children=[
                html.Div(className="level-left", children=[
                    html.Label(className='mr-2',children='1'),
                    html.H2(event['name']),
                ]),
                html.Div(className="level-right", children=[
                    html.Label(event['occurences']),
                ])
            ]) for event in events]

'''
===================================================CALLBACKS================================================
'''

@dash_app.callback(
    Output(component_id='season-year', component_property='children'),
    Output(component_id='event-statistics-list', component_property='children'),
    Output(component_id='team-dropdown', component_property='options'),
    Output(component_id='team-dropdown', component_property='value'),
    Output(component_id='player-dropdown', component_property='value'),
    Input(component_id='season-dropdown', component_property='value'),
    State(component_id='event-statistics-list', component_property='children')
)
def _on_season_dropdown_change(value):
    selected_season = value
    _team_options = _get_teams(value)
    _event_statistics = _get_events_statistics(value)
    print(_event_statistics, _team_options)
    return [f'Season {value}', _event_statistics, _team_options, '', '', '']


@dash_app.callback(
    Output(component_id='player-dropdown', component_property='options'),
    Input(component_id='team-dropdown', component_property='value'),
    prevent_initial_call=True
)
def _on_team_dropdown_change(value):
    selected_team = value
    mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
    team_id = mteams.loc[mteams['TeamName'] == value]
    _player_options = _get_players(team_id['TeamID'].values[0])
    return _player_options

@dash_app.callback(
    [Output(component_id='player-stats-1-h3', component_property='children'),
    Output(component_id='player-stats-2-h3', component_property='children'),
    Output(component_id='player-stats-3-h3', component_property='children')],
    Input(component_id='player-dropdown', component_property='value'),
    prevent_initial_call=True
)
def _on_player_dropdown_change(value):
    selected_player = value
    mteams = _get_csv_file("MDataFiles_Stage2/MTeams.csv")
    mplayers = _get_csv_file("2020DataFiles/2020-Mens-Data/MPlayers.csv")
    mplayers = mplayers.merge(mteams, on="TeamID")
    mplayers = mplayers.loc[mplayers['TeamName'] == selected_team]
    mplayers['FullName'] = mplayers['FirstName']+" "+mplayers['LastName']
    player_id = mplayers.loc[mplayers['FullName'] == value]['TeamID'].values[0]
    # get stats
    return ['1', '2', '3']


'''
===================================================TEMPLATES================================================
'''

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

seasons = _get_seasons()
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
                    dcc.Graph(figure=fig)
                ])
            ])
        ]),
    ]),
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Most Championship Wins"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[
                    dcc.Graph(figure=fig)
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

teams = _get_teams(seasons[len(seasons)-1])
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
            dcc.Dropdown(id="team-dropdown", options=teams, value="", placeholder="Select team name..."),
            html.Br(),
            dcc.Dropdown(id="player-dropdown", options=[], value='', placeholder="Select player name..."),
            html.Br(),
            html.Div(className="columns", children=[
                html.Div(className="column", children=[
                    html.Div(className="box", children=[
                        html.Span(className="", children="Most moves done"),
                        html.H3(className="has-text-weight-bold", id="player-stats-1-h3", children="Sub"),
                    ])
                ]),
                html.Div(className="column", children=[
                    html.Div(className="box", children=[
                        html.Span(className="", children="Stats #2"),
                        html.H3(className="has-text-weight-bold", id="player-stats-2-h3", children="Sub"),
                    ])
                ]),
                html.Div(className="column", children=[
                    html.Div(className="box", children=[
                        html.Span(className="", children="Stats #3"),
                        html.H3(className="has-text-weight-bold", id="player-stats-3-h3", children="Sub"),
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
    selected_team = "Illinois"
    print(_on_player_dropdown_change("Alex Austin"))
    dash_app.run_server(debug=True)
