import os

import logger
import pandas as pd
from utils.constants import TITLE_COL_NAME, TITLE_WITHOUT_STOPWORDS_COL_NAME

from .data_engine import stop

log = logger.get_logger(__name__)


class DataEngineOth:
    def __init__(self, fandom):
        self.fandom = fandom

        # load all FFN data + AO3 data
        fandom_data_path = os.path.join("data", fandom)
        datafiles_to_load = os.listdir(fandom_data_path)
        self.df = pd.read_csv(f"{fandom_data_path}/{datafiles_to_load[0]}", low_memory=False)
        self.df = pd.concat([self.df, pd.read_csv(f"{fandom_data_path}/{datafiles_to_load[1]}")])

        self.df[TITLE_WITHOUT_STOPWORDS_COL_NAME] = self.df[TITLE_COL_NAME].apply(
            lambda x: " ".join([word for word in x.split() if word.lower() not in (stop)])
        )
        log.debug(f"Main Oth data loaded for fandom: {fandom} of size={len(self.df)}.")
