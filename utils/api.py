
import io 
import boto3
import pandas as pd


AWS_S3_BUCKET_SRC = "march-madness-src"
FILE_KEY_PATH_SRC = "MDataFiles_Stage2/MSecondaryTourneyTeams.csv"
session = boto3.Session(profile_name="default")
s3 = session.resource("s3")
s3_client = session.client("s3")

AWS_S3_BUCKET_TRANSFORMED = "dashboard-march-madness"
FILE_KEY_PATH_TRANSFORMED = "test/example_write.csv"

def get_csv_file(file_key_path_src=FILE_KEY_PATH_SRC, aws_s3_bucket_src=AWS_S3_BUCKET_SRC):
    response = s3_client.get_object(Bucket=aws_s3_bucket_src, Key=file_key_path_src)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    df = pd.DataFrame(columns=[])
    
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_csv(response.get("Body"), error_bad_lines=False)
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    return df


def write_file(df, aws_s3_bucket_transformed=AWS_S3_BUCKET_TRANSFORMED, file_key_path_transformed=FILE_KEY_PATH_TRANSFORMED):
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        response = s3_client.put_object(
            Bucket=aws_s3_bucket_transformed, Key=file_key_path_transformed, Body=csv_buffer.getvalue()
        )
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
        return status == 200
