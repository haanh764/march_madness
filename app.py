import sys, os
from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd
from datetime import datetime, date
from utils.stats import get_most_tournament_wins_chart


dash_app = Dash(__name__)

header = html.Header(className="hero is-light", children=[
        html.Div(className="hero-head", children=[
            html.Nav(className="nav has-shadow", children=[
                html.Div(className="nav-left", children=[
                    html.Span(className="nav-item", children=[
                        html.Img(src=dash_app.get_asset_url("images/logo.png"))
                    ])
                ])
            ])
        ])
])

_body_heading =  html.Div(className="columns", children=[
        html.Main(className="column", children=[
            html.Div(className="level", children=[
                html.Div(className="level-left", children=[
                    html.Div(className="level-item", children=[
                        html.H1(children=datetime.now().strftime("%B %d, %Y")),
                        # wrong syntax i know but i want to try to call only  
                        html.Div(children=get_most_tournament_wins_chart())
                    ]),
                ]),
                html.Div(className="level-right", children=[
                    html.Div(className="level-item", children=[
                        dcc.DatePickerRange(
                            id='my-date-picker-range',
                            min_date_allowed=date(1995, 8, 5),
                            max_date_allowed=date(2017, 9, 19),
                            initial_visible_month=date(2017, 8, 5),
                            end_date=date(2017, 8, 25)
                        ),
                    ]),
                ]),
            ])
        ])
    ])

_body_stats_row = html.Div(className="columns is-multiline", children=[
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Most Tournament Wins"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[])
            ])
        ]),
    ]),
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Most Championship Wins"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[])
            ])
        ]),
    ]),
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Statistics"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[])
            ])
        ]),
    ])
])

_body_custom_row = html.Div(className="columns is-multiline", children=[
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Latest Prediction"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[])
            ])
        ]),
    ]),
    html.Div(className="column", children=[
        html.Div(className="box", children=[
            html.Div(className="heading", children="Player Statistics"),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[
                    html.Label('Player Name'),
                ]),
            ]),
            html.Div(className="level", children=[
                html.Div(className="level-item", children=[
                    dcc.Dropdown(id="player-dropdown", options=['Player 1', 'Player 2', 'Player 3'], value='Player 2'),
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
                html.Div(className="level-item", children=[])
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
    
dash_app.layout = html.Div(children=[
    header,
    body,
    footer
])

app = dash_app.server.wsgi_app

if __name__ == '__main__':
    dash_app.run_server(debug=True)
