import ijson, json, sys, re

def remove_bracket(input_string):
    # Remove ' ()' and anything inside the bracket
    target = r"\s*\([^)]*\)\s*"
    empty_string = ""
    return re.sub(target, empty_string, input_string)

def split_script(total_processors):
    TOTAL_JSON_OBJECT = 9092274
    max_items_per_file = (TOTAL_JSON_OBJECT // total_processors) + 1
    with open("bigTwitter.json", "rb") as f:
        result = []
        all_tweet = ijson.items(f, "item")
        index = 0
        file_count = 1
        chunk = []

        for tweet in all_tweet:
            filtered_tweet = {}
            # Only keep the data we want
            filtered_tweet['author_id'] = tweet["data"]["author_id"]
            # All to lower case
            filtered_tweet['place_full_name'] = remove_bracket(tweet["includes"]["places"][0]["full_name"]).lower()
            chunk.append(filtered_tweet)
            index += 1

            if index % max_items_per_file == 0:
                print("write")
                with open(f'chunck{file_count}.json', 'w') as output_file:
                    json.dump(chunk, output_file)
                chunk, index, file_count = [], 0, file_count+1

        # Write the last chunk
        if chunk:
            with open(f'chunck{file_count}.json', 'w') as output_file:
                json.dump(chunk, output_file)

split_script(int(sys.argv[1]))