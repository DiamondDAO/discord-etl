-- WIP, setting up schema for Discord data ETL

CREATE SCHEMA discord
    CREATE TABLE discord.guild (
        id int PRIMARY KEY,
        name varchar(100)
    );

    CREATE TABLE discord.guild_history (
        id GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        guild_id int REFERENCES discord.guild (id),
        name varchar(100),
        icon varchar(100),
        deleted boolean,
        description varchar(200),
        publicUpdatesChannelId int REFERENCES discord.channel (id),
        ownerId int REFERENCES ,
        recordTimestamp
    );

    CREATE TABLE discord.guild_message (
        id,
        channelId,
        author,
        createdTimestamp
    );

    CREATE TABLE discord.guild_message_history (
        id,
        deleted,
        content,
        pinned,
        mentions_everyone,
        mentions_users,
        mentions_roles,
        editedTimestamp,
        timestamp
    );

    CREATE TABLE discord.guild_member (
        user,
        guild,
        joinTimestamp
    );

    CREATE TABLE discord.member_history (
        id,
        premiumSinceTimestamp,
        deleted,
        nickname,
        recordTimestamp
    );

    CREATE TABLE discord.message_reaction (
        id,
        premiumSinceTimestamp,
        deleted,
        nickname,
        recordTimestamp
    );

    CREATE TABLE discord.user (
        id,
        bot
    );

    CREATE TABLE discord.user_history (
        id,
        flags,
        username,
        discriminator,
        avatar,
        recordTimestamp
    );

    CREATE TABLE discord.role (
        id,
        guild
    );

    CREATE TABLE discord.role_history (
        id,
        name,
        color,
        hoist,
        permissions,
        deleted,
        icon,
        unicodeEmoji,
        recordTimestamp
    );

    CREATE TABLE discord.channel (
        id int PRIMARY KEY,
        guild,
        type
    );

    CREATE TABLE discord.channel_history (
        id,
        name,
        parentId,
        deleted,
        recordTimestamp
    );

    

    

    
    

