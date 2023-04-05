import json, re
from collections import defaultdict

SEPARATER = '*' * 5


def print_num_process(comm_size: int):
    """
    :param comm_size: number of processor

    Print the number of processors
    """
    print(f"{SEPARATER} Running on {comm_size} processors {SEPARATER}")

def get_all_tweet(twitter_file_path):
    """
    :param twitter_file_path: A string represent the path to the twitter file
    """
    with open(twitter_file_path, "rb") as f:
        result = []
        all_tweet = json.load(f)
        return all_tweet

def solve_first_question(reduced_question1_counter):
    """
    :param reduced_author_coutner
    """
    # Get first ten
    print("Question1: Top 10 tweeters in terms of the number of tweets made irrespective of where they tweeted")
    # Aligment for pretty priting
    print(f'{"Rank":<13}  {"AuthorID":<25}  {"Number Of Tweet Made":<12}')
    rank = 1
    for author_id, num_of_tweet in reduced_question1_counter.most_common(10):
        print(f'#{rank:<12}  {author_id:<25}  {num_of_tweet:<12}')
        rank += 1

def solve_second_question(reduced_question2_counter):
    print("\nQuestion2: Return the number of tweets made in the various capital cities by all users.")
    print(f'{"Greater Capital City":<20}  {"Number of tweets made":<25}')
    for gcc, num_of_tweet in reduced_question2_counter.most_common(10):
        print(f'{gcc:<20}  {num_of_tweet:<25}')

def solve_third_question(question3_dict):
    print("\nQuestion3: Tweeters that have tweeted in the most Greater Capital sities and the number of times they have tweeted from those locations. The top 10..")
    print(f'#{"Rank":<12}  {"Author Id":<30}  {"Number of Unique City Locations and #Tweets":<12}')
    rank = 1
    # First by number of cities and second by the number of tweets. Get top 10
    sorted_author_id = sorted(question3_dict.items(), key=lambda x: (len(x[1].keys()), sum(x[1].values())), reverse=True)[:10]
    for author_id, tweet_counter in sorted_author_id:
        print(f'#{rank:<12}  {author_id:<30}  {len(tweet_counter.keys()):<6} (#{sum(tweet_counter.values())} tweets - {q3_output_pretty(tweet_counter)})')
        rank += 1

def q3_output_pretty(tweet_counter):
    result = []
    for gcc_code, number_of_tweet in tweet_counter.items():
        result.append(f'#{number_of_tweet}{gcc_code}')
    return (', ').join(result)

def print_elapsed_time(end_time, start_time):
    """
    :param end_time: ending time of the program
    :param start_time: starting time of the program
    """
    # Calculate and output running time
    elapsed = end_time - start_time
    print(f"Porgram elapsed time: {elapsed:.10f}")

def remove_bracket(input_string):
    # Remove ' ()' and anything inside the bracket
    target = r"\s*\([^)]*\)\s*"
    empty_string = ""
    return re.sub(target, empty_string, input_string)

def get_sal_data_list(sal_file_path):
    # We only return the data we are interested in
    targeted_gcc = {'1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar', '8acte', '9oter'}
    result = defaultdict(set)
    with open("sal.json", 'r') as f:
        for i in json.load(f).items():
            # remove bracket ' ()' and all to lowercase and remove all white spaces before and after
            place_full_name = remove_bracket(i[0]).lower().strip()
            gcc_code = i[1]["gcc"]
            if gcc_code in targeted_gcc:
                result[gcc_code].add(place_full_name)
        return result
    
def add_default_dict(dict1, dict2, datatype):
    for key in dict1.keys():
        dict1[key] += dict2[key]
    for key in dict2.keys():
        if key not in dict1.keys():
            dict1[key] = dict2[key]
    return dict1
    
def is_in_gcc(place_full_name, sal_list):
    # We can predefind the gcc names here for later referencing. Except for 9oter
    gcc = {'sydney':'1gsyd', 'melbourne':'2gmel', 'brisbane':'3gbri', 'adelaide':'4gade', 'perth':'5gper', 'hobart':'6ghob', 'darwin':'7gdar', 'canberra':'8acte'}
    # Split it into parts and remove all white spaces before and after to find the exact match
    place_full_name = [x.strip() for x in place_full_name.split(',')]
    if len(place_full_name) == 1:
        # Retrieve the full name
        place_full_name = place_full_name[0]
        if place_full_name in gcc.keys():
            # If its name is the greater captial city name
            return (True, gcc[place_full_name])
        else:
            # Else we loop through the sal list to find a match
            for gcc_code, sal_place in sal_list.items():
                if place_full_name in sal_place:
                    # Return true and its code if there is a match
                    return (True, gcc_code)
            # Return false and None if we cant find one
            return (False, None)
    elif len(place_full_name) == 2:
        # Retrieve the full name
        place_full_name1, place_full_name2 = place_full_name[0], place_full_name[1]
        if place_full_name1 in gcc.keys():
            # If its first part is the greater captial city name
            return (True, gcc[place_full_name1])
        elif place_full_name2 in gcc.keys():
            # If its second part is the greater captial city name
            return (True, gcc[place_full_name2])
        else:
            # Else we loop through the sal list to find a match
            for gcc_code, sal_place in sal_list.items():
                if place_full_name1 in sal_place:
                    return (True, gcc_code)
                elif place_full_name2 in sal_place:
                    return (True, gcc_code)
            return (False, None)
    else:
        return (False, None)
