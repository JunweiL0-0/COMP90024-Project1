import json
import ijson

SEPARATER = '*' * 5


def is_json(myjson: str):
    """
    return true if the input is a json string, return false otherwise
    """
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True

def get_num_of_tweet(twitter_file_path: str):
    """
    :param a string represent the path to the json file
    :return number of items in the json file
    """
    counter = 0
    with open(twitter_file_path, "rb") as f:
        for tweet in ijson.items(f, "item"):
            counter += 1
    return counter

def print_num_process(comm_size: int):
    """
    :param comm_size: number of processor

    Print the number of processors
    """
    print(f"{SEPARATER} Running on {comm_size} processors {SEPARATER}")