
class TwitterData:
    """ required Dada model for Twitter data to be processed """

    def __init__(self, json_data):
        """
        :param data: twitter string
        """
        self.author_id = json_data["data"]["author_id"]
        self.place = json_data["includes"]["places"][0]["full_name"]
