CREATE SCHEMA discord;

CREATE TABLE discord.guild (id bigint PRIMARY KEY, name varchar(100));

CREATE TABLE discord.user (id bigint PRIMARY KEY, bot boolean);

CREATE TABLE discord.role (
    id bigint PRIMARY KEY,
    guild bigint REFERENCES discord.guild (id)
);

CREATE TABLE discord.guild_member (
    member bigint REFERENCES discord.user (id),
    guild bigint REFERENCES discord.guild (id),
    joinedTimestamp timestamp
);

CREATE TABLE discord.guild_member_role (
    role bigint REFERENCES discord.role (id),
    member bigint REFERENCES discord.user (id)
);

CREATE TABLE discord.guild_member_history (
    member bigint REFERENCES discord.user (id),
    premiumSinceTimestamp timestamp,
    deleted boolean,
    nickname varchar(100),
    recordTimestamp timestamp
);

CREATE TABLE discord.channel (
    id bigint PRIMARY KEY,
    guild bigint references discord.guild (id),
    type varchar(100)
);

CREATE TABLE discord.guild_message (
    id bigint PRIMARY KEY,
    channelId bigint REFERENCES discord.channel (id),
    author bigint,
    createdTimestamp timestamp
);

CREATE TABLE discord.guild_message_history (
    id bigint REFERENCES discord.guild_message (id),
    deleted boolean,
    content text,
    pinned boolean,
    mentions_everyone bigint ARRAY,
    mentions_users bigint ARRAY,
    mentions_roles bigint ARRAY,
    editedTimestamp timestamp,
    timestamp timestamp
);

CREATE TABLE discord.guild_message_reaction (
    message bigint references discord.guild_message (id),
    reactionEmoji varchar(20),
    member bigint REFERENCES discord.user (id),
    recordTimestamp timestamp
);

CREATE TABLE discord.user_history (
    id bigint REFERENCES discord.user (id),
    flags bigint,
    username varchar(100),
    discriminator bigint,
    avatar varchar(100),
    recordTimestamp timestamp
);

CREATE TABLE discord.role_history (
    id bigint references discord.role (id),
    name varchar(100),
    color bigint,
    hoist boolean,
    permissions bigint,
    deleted boolean,
    icon varchar(100),
    unicodeEmoji varchar(100),
    recordTimestamp timestamp
);

CREATE TABLE discord.channel_history (
    id bigint references discord.channel (id),
    name varchar(100),
    parentId bigint REFERENCES discord.channel (id),
    deleted boolean,
    recordTimestamp timestamp
);

CREATE TABLE discord.guild_history (
    guild_id bigint REFERENCES discord.guild (id),
    name varchar(100),
    icon varchar(100),
    deleted boolean,
    description varchar(200),
    publicUpdatesChannelId bigint REFERENCES discord.channel (id),
    ownerId bigint REFERENCES discord.user (id),
    recordTimestamp timestamp
);