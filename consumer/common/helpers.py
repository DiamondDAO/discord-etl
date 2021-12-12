import argparse


def update_process_dict(process_dict, tableName, path, cleaner, altData=None, guild_id=None):
    return_dict = process_dict
    return_dict["tableName"] = tableName
    return_dict["path"] = path
    return_dict["cleaner"] = cleaner
    return_dict["altData"] = altData
    return_dict["guild_id"] = guild_id
    return return_dict


def extend_dict_value(dictionary, key, update_value):
    x = dictionary[key]
    x.extend(update_value)
    return x


def str2bool(v):
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")
