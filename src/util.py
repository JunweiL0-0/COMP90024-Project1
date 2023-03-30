import json
import bigjson

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

def get_tweet_genereator(twitter_file_path: str, comm_size: int):
    """
    :param a string represent the path to the json file
    :return number of items in the json file
    """
    with open(twitter_file_path, "rb") as f:
        json = bigjson.load(f)
        index = 0
        for tweet in json:
            index = ((index + 1) % comm_size)
            yield (index, tweet)

def print_num_process(comm_size: int):
    """
    :param comm_size: number of processor

    Print the number of processors
    """
    print(f"{SEPARATER} Running on {comm_size} processors {SEPARATER}")

def print_elapsed_time(end_time, start_time):
    # Output running time on master processor
    elapsed = end_time - start_time
    print(f"Porgram elapsed time: {elapsed:.10f}")
