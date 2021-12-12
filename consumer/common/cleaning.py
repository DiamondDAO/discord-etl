from datetime import datetime
import time


def clean_date(raw_date):
    if not raw_date:
        return ""
    raw_date = raw_date // 1000
    final_date = datetime.fromtimestamp(raw_date)
    final_date = final_date.strftime("%Y-%m-%d, %H:%M:%S")
    return final_date


def clean_guilds(raw_guilds):
    guild_entry_keys = [
        "id",
        "name",
    ]
    final_guilds = []
    for raw_guild in raw_guilds:
        clean_guild = {}
        final_guild = {}

        for key, value in raw_guild.items():
            clean_guild[key] = value

        for key in guild_entry_keys:
            try:
                final_guild[key] = clean_guild[key]
            except:
                final_guild[key] = None
            final_guild[key] = fix_sql_field(final_guild[key])
        final_guilds.append(final_guild)
    return final_guilds


def clean_guild_histories(raw_guild_histories):
    guild_history_keys = [
        "id",
        "name",
        "icon",
        "deleted",
        "description",
        "publicUpdatesChannelId",
        "ownerId",
        "recordTimestamp",
    ]

    final_guild_histories = []
    for raw_history in raw_guild_histories:
        clean_history = {}
        final_history = {}

        for key, value in raw_history.items():
            clean_history[key] = value

        for key in guild_history_keys:
            try:
                final_history[key] = clean_history[key]
            except:
                final_history[key] = None
            final_history[key] = fix_sql_field(final_history[key])
        final_history["recordTimestamp"] = clean_date(final_history["recordTimestamp"])
        final_guild_histories.append(final_history)

    return final_guild_histories


def clean_guild_messages(raw_guild_messages):
    guild_message_keys = [
        "id",
        "channelId",
        "author",
        "createdTimestamp",
    ]

    final_guild_messages = []
    for raw_message in raw_guild_messages:
        clean_message = {}
        final_message = {}

        for key, value in raw_message.items():
            clean_message[key] = value

        for key in guild_message_keys:
            try:
                final_message[key] = clean_message[key]
            except:
                final_message[key] = None
            final_message[key] = fix_sql_field(final_message[key])
        final_message["createdTimestamp"] = clean_date(final_message["createdTimestamp"])
        final_guild_messages.append(final_message)

    return final_guild_messages


def clean_guild_message_histories(raw_guild_message_histories):
    guild_message_history_keys = [
        "id",
        "deleted",
        "content",
        "pinned",
        "mentions_everyone",  # raw.mentions.everyone
        "mentions_users",  # raw.mentions.users
        "mentions_roles",  # raw.mentions.roles
        "editedTimestamp",
        "timestamp",
    ]

    final_guild_message_histories = []
    for raw_history in raw_guild_message_histories:
        clean_history = {}
        final_history = {}
        for key, value in raw_history.items():
            clean_history[key] = value
        clean_history["mentions_everyone"] = "{}"
        clean_history["mentions_users"] = "{}"
        clean_history["mentions_roles"] = "{}"
        if "mentions" in raw_history.keys():
            if raw_history["mentions"]["everyone"]:
                x = ", ".join(raw_history["mentions"]["everyone"])
                x = "{" + x + "}"
                clean_history["mentions_everyone"] = x
            if raw_history["mentions"]["users"]:
                x = ", ".join(raw_history["mentions"]["users"])
                x = "{" + x + "}"
                clean_history["mentions_users"] = x
            if raw_history["mentions"]["roles"]:
                x = ", ".join(raw_history["mentions"]["roles"])
                x = "{" + x + "}"
                clean_history["mentions_roles"] = x

        clean_history["timestamp"] = clean_history["recordTimestamp"]
        for key in guild_message_history_keys:
            try:
                final_history[key] = clean_history[key]
            except:
                final_history[key] = None
            final_history[key] = fix_sql_field(final_history[key])
        if final_history["editedTimestamp"]:
            final_history["editedTimestamp"] = clean_date(final_history["editedTimestamp"])
        if final_history["timestamp"]:
            final_history["timestamp"] = clean_date(final_history["timestamp"])
        final_guild_message_histories.append(final_history)
    return final_guild_message_histories


def clean_guild_members(raw_guild_members):
    guild_member_keys = [
        "member",
        "guild",
        "joinedTimestamp",
    ]

    final_guild_members = []

    for raw_member in raw_guild_members:
        clean_member = {}
        final_member = {}
        for key, value in raw_member.items():
            clean_member[key] = value

        clean_member["member"] = clean_member["user"]

        for key in guild_member_keys:
            try:
                final_member[key] = clean_member[key]
            except:
                final_member[key] = None
            final_member[key] = fix_sql_field(final_member[key])
        final_member["joinedTimestamp"] = clean_date(final_member["joinedTimestamp"])
        final_guild_members.append(final_member)

    return final_guild_members


def clean_guild_member_histories(raw_member_histories):
    member_history_keys = [
        "member",
        "premiumSinceTimestamp",
        "deleted",
        "nickname",
        "recordTimestamp",
    ]

    final_member_histories = []
    final_guild_member_roles = []
    for raw_history in raw_member_histories:
        clean_history = {}
        final_history = {}

        if "roles" in raw_history.keys():
            for role in raw_history["roles"]:
                clean_role = {}
                clean_role["role"] = role["id"]
                clean_role["user"] = raw_history["user"]

                final_guild_member_roles.append(clean_role)

        for key, value in raw_history.items():
            clean_history[key] = value

        clean_history["member"] = clean_history["user"]
        for key in member_history_keys:
            try:
                final_history[key] = clean_history[key]
            except:
                final_history[key] = None
            final_history[key] = fix_sql_field(final_history[key])
        final_history["premiumSinceTimestamp"] = clean_date(final_history["premiumSinceTimestamp"])
        final_history["recordTimestamp"] = clean_date(final_history["recordTimestamp"])
        final_member_histories.append(final_history)

    return final_member_histories, final_guild_member_roles


def clean_guild_message_reactions(raw_guild_message_reactions):
    guild_message_reaction_keys = [
        "message",
        "reactionEmoji",
        "member",
        "recordTimestamp",  # note that for initial reactions this will be the time when the bot joined and fetched them
    ]

    final_reactions = []
    for raw_reaction in raw_guild_message_reactions:
        for user in raw_reaction["users"]:
            clean_reaction = {}
            final_reaction = {}

            for key, value in raw_reaction.items():
                clean_reaction[key] = value
            clean_reaction["member"] = user

            for key in guild_message_reaction_keys:
                try:
                    final_reaction[key] = clean_reaction[key]
                except:
                    final_reaction[key] = None
                final_reaction[key] = fix_sql_field(final_reaction[key])
            final_reaction["recordTimestamp"] = clean_date(final_reaction["recordTimestamp"])
            final_reactions.append(final_reaction)

    return final_reactions


def clean_users(raw_users):
    user_keys = [
        "id",
        "bot",
    ]

    final_users = []
    for raw_user in raw_users:
        clean_user = {}
        final_user = {}

        for key, value in raw_user.items():
            clean_user[key] = value

        for key in user_keys:
            try:
                final_user[key] = fix_sql_field(clean_user[key])
            except:
                final_user[key] = None

        final_users.append(final_user)

    return final_users


def clean_user_histories(raw_user_histories):
    user_history_keys = [
        "id",
        "flags",
        "username",
        "discriminator",
        "avatar",
        "recordTimestamp",
    ]

    final_user_histories = []
    for raw_history in raw_user_histories:
        clean_history = {}
        final_history = {}

        for key, value in raw_history.items():
            clean_history[key] = value

        for key in user_history_keys:
            try:
                final_history[key] = clean_history[key]
            except:
                final_history[key] = None
            final_history[key] = fix_sql_field(final_history[key])
        final_history["recordTimestamp"] = clean_date(final_history["recordTimestamp"])
        final_user_histories.append(final_history)

    return final_user_histories


def clean_roles(raw_roles):
    role_keys = [
        "id",
    ]

    final_roles = []
    for raw_role in raw_roles:
        clean_role = {}
        final_role = {}
        for key, value in raw_role.items():
            clean_role[key] = value

        clean_role["id"] = clean_role["role"]
        for key in role_keys:
            try:
                final_role[key] = clean_role[key]
            except:
                final_role[key] = None
            final_role[key] = fix_sql_field(final_role[key])
        final_roles.append(final_role)

    return final_roles


def clean_role_histories(raw_role_histories):
    role_history_keys = [
        "id",
        "name",
        "color",
        "hoist",
        "permissions",
        "deleted",
        "icon",
        "unicodeEmoji",
        "recordTimestamp",
    ]

    final_role_histories = []
    for raw_history in raw_role_histories:
        clean_history = {}
        final_history = {}

        for key, value in raw_history.items():
            clean_history[key] = value

        for key in role_history_keys:
            try:
                final_history[key] = clean_history[key]
            except:
                final_history[key] = None
            final_history[key] = fix_sql_field(final_history[key])
        final_history["recordTimestamp"] = clean_date(final_history["recordTimestamp"])
        final_role_histories.append(final_history)

    return final_role_histories


def clean_guild_channels(raw_channels):
    channel_keys = [
        "id",
        "guild",
        "type",
    ]

    final_channels = []
    for raw_channel in raw_channels:
        clean_channel = {}
        final_channel = {}

        for key, value in raw_channel.items():
            clean_channel[key] = value

        for key in channel_keys:
            try:
                final_channel[key] = clean_channel[key]
            except:
                final_channel[key] = None
            final_channel[key] = fix_sql_field(final_channel[key])
        final_channels.append(final_channel)

    return final_channels


def clean_guild_channel_histories(raw_channel_histories):
    channel_history_keys = [
        "id",
        "name",
        "parentId",
        "deleted",
        "recordTimestamp",
    ]

    final_channel_histories = []
    for raw_history in raw_channel_histories:
        clean_history = {}
        final_history = {}

        for key, value in raw_history.items():
            clean_history[key] = value

        for key in channel_history_keys:
            try:
                final_history[key] = clean_history[key]
            except:
                final_history[key] = None
            final_history[key] = fix_sql_field(final_history[key])
        final_history["recordTimestamp"] = clean_date(final_history["recordTimestamp"])
        final_channel_histories.append(final_history)

    return final_channel_histories


def fix_sql_field(x):
    if type(x) == bool:
        return x
    if type(x) == list:
        return x
    try:
        x = int(x)
        return int(x)
    except:
        if x is None:
            return ""
        if type(x) == str:
            return " ".join(x.split())
        else:
            return x
