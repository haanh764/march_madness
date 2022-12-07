from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd

dash_app = Dash(__name__)

df = pd.DataFrame({
    "Fruit": ["Apples", "Oranges", "Bananas", "Apples", "Oranges", "Bananas"],
    "Amount": [4, 1, 2, 2, 4, 5],
    "City": ["SF", "SF", "SF", "Montreal", "Montreal", "Montreal"]
})

fig = px.bar(df, x="Fruit", y="Amount", color="City", barmode="group")

dash_app.layout = html.Div(children=[
    html.H1(children='Hello! Welcome to March Madness!'),

    html.Div(children='''
        Dash: A web application framework for your data.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

app = dash_app.server.wsgi_app

if __name__ == '__main__':
    dash_app.run_server(debug=True)
