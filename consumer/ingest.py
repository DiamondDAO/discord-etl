import boto3
import json
from datetime import datetime, timedelta
import sys
import io
import psycopg2
import os
import pandas as pd
from dotenv import load_dotenv
import argparse

sys.path.append(__file__)
from common.s3 import getRawContent, truncate_all_tables, ingest, write_to_s3
from common.cleaning import *
from common.helpers import str2bool, update_process_dict, extend_dict_value


def processData(tableName, path, cleaner, altData, s3, bucket, guild_id=None):
    if altData:
        data = altData
    else:
        data = getRawContent(path, s3, bucket)

    cleaned_data = cleaner(data)
    if not guild_id:
        guild_id = ""
    print(f"{guild_id}-{tableName} data cleaned")

    return data, cleaned_data


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--save_to_s3", type=str2bool, default=True, help="Save cleaned files to s3")
    parser.add_argument("--ingest", type=str2bool, default=False, help="Ingest cleaned data into db")
    args = parser.parse_args()

    all_tables = {
        "guild": [],
        "user": [],
        "role": [],
        "guild_member": [],
        "guild_member_role": [],
        "guild_member_history": [],
        "channel": [],
        "guild_message": [],
        "guild_message_history": [],
        "guild_message_reaction": [],
        "user_history": [],
        "role_history": [],
        "channel_history": [],
        "guild_history": [],
    }
    all_tables_dict = {}
    for key, value in all_tables.items():
        all_tables_dict.update({"discord." + key: value})
    all_tables = [key for key in all_tables_dict.keys()]

    load_dotenv()

    s3_bucket = os.getenv("S3_BUCKET") if os.getenv("S3_BUCKET") else "chainverse"
    s3 = boto3.resource("s3")

    database_dict = {
        "database": os.environ.get("POSTGRES_DB"),
        "user": os.environ.get("POSTGRES_USERNAME"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "host": os.environ.get("POSTGRES_WRITER"),
        "port": os.environ.get("POSTGRES_PORT"),
    }

    engine = psycopg2.connect(**database_dict)
    cur = engine.cursor()

    truncate_all_tables(cur, engine, all_tables)

    # process users
    process_data_dict = {
        "tableName": "",
        "path": "",
        "cleaner": "",
        "altData": None,
        "s3": s3,
        "bucket": s3_bucket,
        "guild_id": None,
    }

    # process users
    process_data_dict = update_process_dict(process_data_dict, "user", "discord/users/_", clean_users)
    raw_users, users = processData(**process_data_dict)
    all_tables_dict["discord.user"] = extend_dict_value(all_tables_dict, "discord.user", users)

    process_data_dict = update_process_dict(process_data_dict, "user_history", "discord/users/_", clean_user_histories)
    raw_user_history, user_history = processData(**process_data_dict)
    all_tables_dict["discord.user_history"] = extend_dict_value(all_tables_dict, "discord.user_history", user_history)

    # process guilds
    process_data_dict = update_process_dict(process_data_dict, "guild", "discord/guilds/guildEntities", clean_guilds)
    raw_guilds, guilds = processData(**process_data_dict)
    all_tables_dict["discord.guild"] = extend_dict_value(all_tables_dict, "discord.guild", guilds)

    process_data_dict = update_process_dict(
        process_data_dict, "guild_history", "discord/guild/guildEvents", clean_guild_histories
    )
    raw_guild_history, guild_history = processData(**process_data_dict)
    all_tables_dict["discord.guild_history"] = extend_dict_value(
        all_tables_dict, "discord.guild_history", guild_history
    )

    # process guild subdata
    for guild in guilds:
        guild_id = guild["id"]
        guild_path = f"discord/guilds/{guild_id}/"

        # process messages
        process_data_dict = update_process_dict(
            process_data_dict, "guild_message", guild_path + "messages/_", clean_guild_messages, guild_id=guild_id
        )
        raw_messages, messages = processData(**process_data_dict)
        all_tables_dict["discord.guild_message"] = extend_dict_value(all_tables_dict, "discord.guild_message", messages)

        process_data_dict = update_process_dict(
            process_data_dict,
            "guild_message_history",
            guild_path + "messages/_",
            clean_guild_message_histories,
            raw_messages,
            guild_id=guild_id,
        )
        raw_message_history, message_history = processData(**process_data_dict)
        all_tables_dict["discord.guild_message_history"] = extend_dict_value(
            all_tables_dict, "discord.guild_message_history", message_history
        )

        # process members and roles
        process_data_dict = update_process_dict(
            process_data_dict, "guild_member", guild_path + "members/_", clean_guild_members, guild_id=guild_id
        )
        raw_members, members = processData(**process_data_dict)
        all_tables_dict["discord.guild_member"] = extend_dict_value(all_tables_dict, "discord.guild_member", members)

        cleaned_mem_histories, cleaned_mem_roles = clean_guild_member_histories(raw_members)

        all_tables_dict["discord.guild_member_history"] = extend_dict_value(
            all_tables_dict, "discord.guild_member_history", cleaned_mem_histories
        )
        all_tables_dict["discord.guild_member_role"] = extend_dict_value(
            all_tables_dict, "discord.guild_member_role", cleaned_mem_roles
        )

        process_data_dict = update_process_dict(
            process_data_dict, "role", guild_path + "roles/_", clean_roles, cleaned_mem_roles, guild_id=guild_id
        )
        raw_roles, roles = processData(**process_data_dict)
        for idx, entry in enumerate(roles):
            roles[idx]["guild"] = guild_id
        all_tables_dict["discord.role"] = extend_dict_value(all_tables_dict, "discord.role", roles)

        process_data_dict = update_process_dict(
            process_data_dict, "role_history", guild_path + "roles/", clean_role_histories, guild_id=guild_id
        )
        raw_role_history, role_history = processData(**process_data_dict)
        all_tables_dict["discord.role_history"] = extend_dict_value(
            all_tables_dict, "discord.role_history", role_history
        )

        # process reactions
        process_data_dict = update_process_dict(
            process_data_dict,
            "guild_message_reaction",
            guild_path + "reaactions",
            clean_guild_message_reactions,
            guild_id=guild_id,
        )
        raw_message_reaction, message_reaction = processData(**process_data_dict)
        all_tables_dict["discord.guild_message_reaction"] = extend_dict_value(
            all_tables_dict, "discord.guild_message_reaction", message_reaction
        )

        # process channels
        process_data_dict = update_process_dict(
            process_data_dict, "channel", guild_path + "channels/_", clean_guild_channels, guild_id=guild_id
        )
        raw_channels, channels = processData(**process_data_dict)
        all_tables_dict["discord.channel"] = extend_dict_value(all_tables_dict, "discord.channel", channels)

        process_data_dict = update_process_dict(
            process_data_dict,
            "channel_history",
            guild_path + "channels/",
            clean_guild_channel_histories,
            guild_id=guild_id,
        )
        raw_channel_history, channel_history = processData(**process_data_dict)
        all_tables_dict["discord.channel_history"] = extend_dict_value(
            all_tables_dict, "discord.channel_history", channel_history
        )

        with open("cleaned_data_dict.json", "w") as file:
            json.dump(all_tables_dict, file)

        print(f"Data for guild {guild_id} processed")

    if args.ingest:
        for key, value in all_tables_dict.items():
            print(key)
            ingest(value, cur, engine, key)

    if args.save_to_s3:
        for key, value in all_tables_dict.items():
            print(key)
            try:
                file_name = key.split(".")[1]
            except:
                file_name = key
            write_to_s3(value, s3, s3_bucket, key)

    cur.close()
    print("Done")
