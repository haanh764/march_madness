# Data Warehouse Project: March Madness Analytics Documentation

### 1. Project Structure
```
march_madness
│   README.md
│   requirements.txt
|   zappa_settings.json
|   app.py
│
└───data_transformation
│   │   ......py
│   
│   
└───prediction
    │   .....py
    │   .....py
```

- `requirements.txt`: contains required libraries
- `zappa_settings.json`: contains zappa config for dashbpard deployment on AWS Lambda
- `app.py`: main Dash app for dashboard, contains source code for creating charts
- `data_transformation`: folder contains source code to transform source data into data needed for dashboard
- `prediction`: folder contains source code for ML model

### 2. How to re-deploy dashboard:
- Create new conda environment: `conda create -n march-madness python=3.9.15`
- Activate `march-madness` environment: `conda activate march-madness`
- Change to project directory and install the `requirements.txt`: `cd march_madness/` && `pip install -r requirements.txt`
- Export env path to virtual env path: `export VIRTUAL_ENV=/opt/conda/envs/march-madness` (the path depends on your computer, you can check it py using `which python`)
- After make change in the dashboard, re-deploy the dashboard with: `zappa update dev`
- Check this link to see the update dashboard: `https://z8eqecfs2f.execute-api.eu-central-1.amazonaws.com/dev`
