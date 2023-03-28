import json

SEPARATER = '*' * 5

def print_num_processor(comm_size):
    """
    :param comm_size: number of processor

    Print the number of processors
    """
    print(f"{SEPARATER} Running on {comm_size} processors {SEPARATER}")


def is_json(myjson):
    """
    return true if the input is a json string, return false otherwise
    """
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True
