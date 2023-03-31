import json, os, math, ijson

SEPARATER = '*' * 5
    

def get_start_end_position(twitter_file_path, comm_rank, comm_size):
    # Total file size
    total_file_size = os.stat(twitter_file_path).st_size
    # Split into pieces: Say we have four prpcossers, 
    chunk_size =  total_file_size // comm_size
    # Starting position
    start_position = chunk_size * comm_rank
    # Ending position
    end_postion = chunk_size * comm_rank
    if comm_rank == 0:
        # Master processor
        start_position, end_position = 0, chunk_size
    elif comm_rank != 0 and comm_rank != (comm_size - 1):
        # Other processor. Not the last one
        start_position, end_position = chunk_size * comm_rank, (chunk_size * (comm_rank + 1))
    else:
        # Other processor. The last one will read till the the EOF
        start_position, end_position = chunk_size * comm_rank, total_file_size
    # print(start_position, end_position)
    return start_position, end_position

def get_line_genereator(twitter_file_path, start_position, end_position):
    """
        :param a string represent the path to the json file
        :return number of items in the json file

        |_________
                 |__________
                           |__________(ignore the very end line and return None)
    """
    with open(twitter_file_path, "r") as f:
        # Init variable
        line = 0
        # Navigate to staring position
        f.seek(start_position)
        # Only process the lines we want
        while True:
            # One line per time
            line = f.readline()
            if f.tell() < end_position:
                yield line
            else:
                return None

def get_author_id(input_string):
    # Json file have fixed format, we can use the slicing to retrive author id
    return input_string[20:-3]

def have_author_id(input_string):
    author_id_format = "      \"author_id\": "
    return author_id_format in input_string

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

def sum_two_defaultdict(dict1, dict2, datatype):
    for key in dict2.keys():
        dict1[key] += dict2[key]
    return dict1

def solve_first_question(reduced_author_counter):
    # Get first ten
    print("Question1: Top 10 tweeters in terms of the number of tweets made irrespective of where they tweeted")
    print(f'{"Rank":<13}  {"AuthorID":<25}  {"Num Of Tweet Made":<12}')
    rank = 1
    for author_id, num_of_tweet in reduced_author_counter.most_common(10):
        print(f'#{rank:<12}  {author_id:<25}  {num_of_tweet:<12}')
        rank += 1
