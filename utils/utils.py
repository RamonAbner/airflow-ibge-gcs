import pandas as pd


@staticmethod
def clean_columnnames(df) -> pd.DataFrame:
        """
        Cleans column names in a DataFrame to conform to BigQuery's naming conventions.
        Unidecode is used to remove accents and special characters.

        Args:
            df (pd.DataFrame): The DataFrame with raw column names.

        Returns:
            pd.DataFrame: The DataFrame with cleaned column names.
        """
        df.columns = (
            pd.Series(df.columns)
            .str.strip()
            .str.lower()
            .str.replace(r'[^\w]', '', regex=True)
            .str[:300]
        )
        return df