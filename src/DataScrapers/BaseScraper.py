import pandas as pd
import os
import logging

class BaseScraper:
    """Base Data Scraping class that is inherited by all scrappers"""
    def __init__(self, output_path: str):
        self.output_path = output_path
        self.logger = logging.getLogger(self.__class__.__name__)

    def write_output(self, df: pd.DataFrame)-> None:
        """
        Appends the scraped data to the existing dataset.

        Args:
            df (pd.DataFrame): represents the dataframe
        """
        if not os.path.isfile(self.output_path):
            df.to_csv(self.output_path, index=False)
            self.logger.info(f"File created {self.output_path}")
        else:
            df.to_csv(self.output_path, mode='a', header=False, index=False)
            self.logger.info(f"Data appended to {self.output_path}")
