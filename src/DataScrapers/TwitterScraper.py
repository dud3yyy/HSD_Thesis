import tweepy
import pandas as pd
from .BaseScraper import BaseScraper

class TwitterScraper(BaseScraper):
    """Twitter scraping class which inherits the base scraper class"""

    def __init__(self, bearer_token: str, output_path: str):
        """
        Initializes the TwitterScraper class
        
        Args:
            bearer_token (str): authentication token
            output_path (str): represents the path where the data will be stored.
        """
        super().__init__(output_path)
        self.client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

    def scrape_recent_tweets(self, query):
        """
        Method that scrapes and ingests recent tweets

        Args:
            query (str): Represents the query which will be used to query the data
            ### TODO add count (int): ###
        """
        response = self.client.search_recent_tweets(
                query=query,
                tweet_fields=["author_id", "geo", "possibly_sensitive", "created_at", "public_metrics", "text", "source"],
                user_fields=["username", "location"],
                expansions="author_id"
            ) # add count
        
        if response.data:
            attributes_cont = self.map_data(response)

            columns = ["Username", "Country_Code", "Possibly_Sensitive", "Date_Created", "No_of_Likes","Source", "Full_Text"]

            x_df = pd.DataFrame(attributes_cont, columns=columns)

            self.write_output(x_df)
            self.logger.info(f"Successfully saved {len(x_df)} tweets to {self.output_path}")
        else:
            self.logger.info("No tweets found for this query!!!")
        

    def map_data(self, response) -> list:
        """
        Maps the data accordingly

        Args:
            response (): Represents the response w the scraped data

        Returns:
            list: contains the necessary mappings
        """
        users = {user["id"]: user for user in response.includes["users"]}
        return [
            [users[tweet.author_id]["username"] if tweet.author_id in users else None,
            tweet.geo["place_id"] if tweet.geo else None,
            tweet.possibly_sensitive,
            tweet.created_at,
            tweet.public_metrics['like_count'],
            tweet.source if tweet.source else 'X',
            tweet.text]
            for tweet in response.data
        ]
         
    
    def build_df(self, columns: list, attributes: list) -> pd.DataFrame:
        """
        Builds a dataframe w the provided columns and attributes
        """
        return pd.DataFrame(attributes, columns=columns)




