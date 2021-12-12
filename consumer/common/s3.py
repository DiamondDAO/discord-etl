import json
import boto3
import pandas as pd
import io


def get_matching_s3_objects(bucket, client, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    paginator = client.get_paginator("list_objects_v2")

    if isinstance(prefix, str):
        prefixes = (prefix,)
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def getRawContent(path, s3, bucket, fileType=".json"):
    all_content = []
    client = boto3.client("s3")
    for obj in get_matching_s3_objects(bucket, client, path, fileType):
        key = obj["Key"]
        content_object = s3.Object(bucket, key)
        file_content = content_object.get()["Body"].read().decode("utf-8")
        raw_content = json.loads(file_content)
        all_content.append(raw_content)
    return all_content


def truncate_all_tables(cur, engine, table_names):
    all_tables = ", ".join(table_names)
    execute_string = "TRUNCATE " + all_tables
    cur.execute(execute_string)
    engine.commit()
    print(f"All discord tables truncated")


def ingest(cleaned_json, cur, engine, table_name):
    if not cleaned_json:
        return
    for idx, entry in enumerate(cleaned_json):
        cleaned_json[idx] = {k.lower(): v for k, v in entry.items()}
    df = pd.DataFrame(cleaned_json)
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])
    output = io.StringIO()
    df.to_csv(output, sep="\t", header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table_name, null="")
    engine.commit()
    print(f"{table_name} table saved")


def write_to_s3(cleaned_json, s3, bucket, table_name):
    file_name = "discord/cleaned_data/" + table_name + ".json"
    s3object = s3.Object(bucket, file_name)
    s3object.put(Body=(bytes(json.dumps(cleaned_json).encode("UTF-8"))))
