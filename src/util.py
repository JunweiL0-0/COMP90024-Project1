import ijson, json
from TwitterData import TwitterData

SEPARATER = '*' * 5

def get_all_tweet(twitter_file_path):
    """
    :param twitter_file_path: A string represent the path to the twitter file
    """
    with open(twitter_file_path, "rb") as f:
        result = []
        all_tweet = ijson.items(f, "item")
        for tweet in all_tweet:
            result.append(TwitterData(tweet))
        return result


def print_num_process(comm_size: int):
    """
    :param comm_size: number of processor

    Print the number of processors
    """
    print(f"{SEPARATER} Running on {comm_size} processors {SEPARATER}")

def print_elapsed_time(end_time, start_time):
    """
    :param end_time: ending time of the program
    :param start_time: starting time of the program
    """
    # Calculate and output running time
    elapsed = end_time - start_time
    print(f"Porgram elapsed time: {elapsed:.10f}")

def distribute_work_to_worker(comm, master_tweet_list):
    """
    :param comm: communicator
    :param master_tweet_list: a list of TwitterData objects which contains all the tweet info
    """
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    total_tweet = len(master_tweet_list)
    # Calculate the chunck size base on the size
    chunck_size = total_tweet // comm_size

    # Send this to the worker nodes
    for rank in range(1, comm_size):
        # Send all the rest to the last node
        if rank == comm_size - 1:
            comm.send(master_tweet_list[chunck_size*(comm_size-1):], dest=rank)
            continue
        # Evenly split and send to other nodes
        comm.send(master_tweet_list[chunck_size*rank : chunck_size*(rank+1)], dest=rank)

def get_sal_data_list(sal_file_path):
    # We only return the data we are interested in
    targeted_gcc = {'7gdar', '6ghob', '4gade', '5gper', '2gmel', '1gsyd', '8acte', '9oter', '3gbri'}
    result = dict()
    with open("sal.json", 'r') as f:
        for i in json.load(f).items():
            place_full_name = i[0]
            gcc_code = i[1]["gcc"]
            if gcc_code in targeted_gcc:
                result[gcc_code] = place_full_name
        return result


def solve_first_question(reduced_author_counter):
    """
    :param reduced_author_coutner
    """
    # Get first ten
    print("Question1: Top 10 tweeters in terms of the number of tweets made irrespective of where they tweeted")
    # Aligment for pretty priting
    print(f'{"Rank":<13}  {"AuthorID":<25}  {"Num Of Tweet Made":<12}')
    rank = 1
    for author_id, num_of_tweet in reduced_author_counter.most_common(10):
        print(f'#{rank:<12}  {author_id:<25}  {num_of_tweet:<12}')
        rank += 1
