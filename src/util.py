"""
Author: Junwei Liang, Kai xu
"""

import json, re, os
from collections import defaultdict

SEPARATER = '*' * 5

def get_lines_to_read(twitter_file_path, comm_size):
    """
    :param twitter_file_path: A string represent the path to the file
    :param comm_size: An integer represent the size of the comm

    This function will count the number of lines in the file and evenly split it according to the comm size
    """
    # Total file length
    total_file_lines = get_total_file_length(twitter_file_path)
    return total_file_lines // comm_size

def get_total_file_length(file_path):
    """
    :param file_path: A string represent the path to the file

    This function will run through the entire file and count the number of line
    """
    with open(file_path, 'r') as f:
        counter = 0
        for line in f:
            counter += 1
        return counter       

def print_num_process(comm_size: int):
    """
    :param comm_size: number of processor

    Print the number of processors
    """
    print(f"{SEPARATER} Running on {comm_size} processors {SEPARATER}")

def have_author_id(input_string):
    """
    :param input_string: A string from the json file

    Instead of using regular expression or any other methods. Benefit from the json format.
    We can direcctly using the string lookup method which has complexity of O(1).
    Return True if input_string contains "author_id :" false otherwise. By limiting the size of the string can speed up the process
    """
    if len(input_string) > 19 and len(input_string) < 100:
        if (input_string[6]=='\"' and input_string[7]=='a' and input_string[8]=='u' and input_string[9]=='t'
            and input_string[10]=='h' and input_string[11]=='o' and input_string[12]=='r' and input_string[13]=='_'
            and input_string[14]=='i' and input_string[15]=='d' and input_string[16]=='\"' and input_string[17]==':'):
            return True
    else:
        return False

def retrieve_full_place_name(input_string):
    """
    :param input_string: A string from the json file

    return anything between the double quotes
    """
    return re.findall(r'"(.*?)"', input_string)[1]

def retrieve_author_id(input_string):
    """
    :param input_string: A string from the json file

    return anything between the double quotes
    """
    return re.findall(r'"(.*?)"', input_string)[1]

def have_full_place_name(input_string):
    """
    :param input_string: A string from the json file

    Instead of using regular expression or any other methods. Benefit from the json format.
    We can direcctly using the string lookup method which has complexity of O(1).
    Return True if input_string contains "full_name :" false otherwise. By limiting the size of the string can speed up the process
    """
    if len(input_string) > 23 and len(input_string) < 100:
        if (input_string[10]=='\"' and input_string[11]=='f' and input_string[12]=='u' and input_string[13]=='l'
            and input_string[14]=='l' and input_string[15]=='_' and input_string[16]=='n' and input_string[17]=='a'
            and input_string[18]=='m' and input_string[19]=='e' and input_string[20]=='\"' and input_string[21]==':'):
            return True
        else:
            return False
    else:
        return False

def solve_first_question(reduced_question1_counter):
    """
    :param reduced_question1_counter: Coutner({author_id: number_of_tweet})

    Format and print the final result of first question
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
    """
    :param reduced_question2_counter: Counter({gcc: number_of_tweet})

    Format and print the final result of second question
    """
    print("\nQuestion2: Return the number of tweets made in the various capital cities by all users.")
    print(f'{"Greater Capital City":<20}  {"Number of tweets made":<25}')
    for gcc, num_of_tweet in reduced_question2_counter.most_common(10):
        print(f'{gcc:<20}  {num_of_tweet:<25}')

def solve_third_question(question3_dict):
    """
    :param question3_dict: dict({author_id: {gcc: num_of_tweet}}})

    Format and print the final result of third question
    """
    print("\nQuestion3: Tweeters that have tweeted in the most Greater Capital sities and the number of times they have tweeted from those locations. The top 10..")
    print(f'#{"Rank":<12}  {"Author Id":<30}  {"Number of Unique City Locations and #Tweets":<12}')
    rank = 1
    # First by number of cities and second by the number of tweets. Get top 10
    sorted_author_id = sorted(question3_dict.items(), key=lambda x: (len(x[1].keys()), sum(x[1].values())), reverse=True)[:10]
    for author_id, tweet_counter in sorted_author_id:
        print(f'#{rank:<12}  {author_id:<30}  {len(tweet_counter.keys()):<6} (#{sum(tweet_counter.values())} tweets - {q3_output_pretty(tweet_counter)})')
        rank += 1

def q3_output_pretty(tweet_counter):
    """
    :param tweet_counter: a list of (gcc: num_of_tweet)

    Format the final result of third question
    """
    result = []
    for gcc_code, number_of_tweet in tweet_counter.items():
        result.append(f'#{number_of_tweet} {gcc_code}')
    return (', ').join(result)

def print_elapsed_time(end_time, start_time):
    """
    :param end_time: ending time of the program
    :param start_time: starting time of the program

    Calculate and print the elapesd time of the program
    """
    # Calculate and output running time
    elapsed = end_time - start_time
    print(f"Porgram elapsed time: {elapsed:.10f}")

def get_sal_data_list(sal_file_path):
    """
    :param sal_file_path: A string represent the path to the sal_file

    return a dictionary which key if the gcc and value is the place_full_name
    """
    # We only return the data we are interested in
    targeted_gcc = {'1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar', '8acte', '9oter'}
    result = defaultdict(set)

    with open(sal_file_path, 'r') as f:
        for i in json.load(f).items():
            # all to lowercase and remove all white spaces before and after
            place_full_name = i[0].lower().strip()
            gcc_code = i[1]["gcc"]
            if gcc_code in targeted_gcc:
                result[gcc_code].add(place_full_name)
        return result
    
def add_default_dict(dict1, dict2, datatype):
    """
    :param dict1: {key, Counter}
    :param dict2: {key, Counter}

    This function is used in the reduce process
    """
    # Add each value together
    for key in dict1.keys():
        dict1[key] += dict2[key]
    # If there are some value that are not existing in the first dict
    for key in dict2.keys():
        if key not in dict1.keys():
            dict1[key] = dict2[key]
    # Return the joined dict
    return dict1
    
def is_in_gcc(place_full_name, sal_list):
    """
    :param place_full_name: A string represent the name of the place
    :param sal_list: [{gcc: place_names}]

    Test the place_full_name against the sal_list.
    Return true and the gcc_code if we found one. Return false and None if we hit the end of our records.
    """
    # We can predefind the gcc names here for later referencing. Except for 9oter
    gcc = {'sydney':'1gsyd', 'melbourne':'2gmel', 'brisbane':'3gbri', 'adelaide':'4gade', 'perth':'5gper', 'hobart':'6ghob', 'darwin':'7gdar', 'canberra':'8acte'}
    # Split it into parts and remove all white spaces before and after to find the exact match
    place_full_name = [x.strip() for x in place_full_name.lower().split(',')]
    # if contains comma
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
    # If not contains comma
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

def get_all_tweet(twitter_file_path, start_position, lines_to_read):
    """
    :param twitter_file_path: A string represent the path to the twitter file
    :param start_position: An integer represent the location of the file
    :param lines_to_read: An integer represent how many liens we should read

    |_________
            |__________
                    |__________(ignore the very end line and return None)
    This function will return a list of dictionary and the ending position of the read
    """
    with open(twitter_file_path, "r") as f:
        result = []
        author_id, place_full_name = "", ""
        # Navigate to starting position
        f.seek(start_position)
        counter = 0
        while counter < lines_to_read:
            line = f.readline()
            counter += 1
            if have_author_id(line):
                author_id = retrieve_author_id(line)
                # There must be a full place name after the author id. Keep reading until we find one
                while True:
                    line = f.readline()
                    counter += 1
                    if have_full_place_name(line):
                        place_full_name = retrieve_full_place_name(line)
                        result.append({"author_id": author_id, "place_full_name": place_full_name})
                        break
            # Reset to empty string
            author_id, place_full_name = "", ""
        return result, f.tell()
