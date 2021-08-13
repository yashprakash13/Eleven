"""the DataEngine class"""
import ast

import pandas as pd
import stopwords
import whoosh.index as windex
from rapidfuzz import fuzz, process
from utils.constants import (
    CHARACTERS_COL_NAME,
    DEFAULT_PAIR,
    HHR_AO3_DATA_PATH,
    MAIN_EN_DATA_PATH,
    NO_CHARACTERS_COL_VALUE,
    NO_PAIRS_COL_VALUE,
    PAIR_CHARACTER_MAPPING,
    PAIRS_TO_CALC_UNA_FOR,
    STORY_ID_COL_NAME,
    TITLE_COL_NAME,
    TITLE_WITHOUT_STOPWORDS_COL_NAME,
)

stop = stopwords.get_stopwords("en")
stop.extend(["Harry", "Potter"])
stop = [word.lower() for word in stop]


class DataEngine:
    def __init__(self, pairs=[DEFAULT_PAIR, NO_PAIRS_COL_VALUE]):
        self.pairs = pairs
        self.data = {}
        self.data_una = {}

        # load all FFN data + HHr AO3 data
        self.df = pd.read_csv(MAIN_EN_DATA_PATH, low_memory=False)
        self.df = pd.concat([self.df, pd.read_csv(HHR_AO3_DATA_PATH)])

    def _get_ids_una_per_row(self, row, chars_present, list_to_append_to):
        if str(row[CHARACTERS_COL_NAME]) != NO_CHARACTERS_COL_VALUE and set(chars_present).issubset(
            ast.literal_eval(str(row[CHARACTERS_COL_NAME]))
        ):
            list_to_append_to.append(row[STORY_ID_COL_NAME])

    def _load_una_ids(self, pair):
        una_list = []
        self.data[NO_PAIRS_COL_VALUE].apply(
            lambda row: self._get_ids_una_per_row(row, PAIR_CHARACTER_MAPPING[pair], una_list),
            axis=1,
        )
        self.data_una[pair] = una_list

    def load_una_ids(self):

        for pair in self.pairs:
            if pair == NO_PAIRS_COL_VALUE:
                continue
            self._load_una_ids(pair)

        self.df = pd.concat(
            [self.df.loc[self.df[STORY_ID_COL_NAME].isin(self.data_una[DEFAULT_PAIR])], self.data[DEFAULT_PAIR]]
        )
        del self.data[NO_PAIRS_COL_VALUE]

        self.df[TITLE_WITHOUT_STOPWORDS_COL_NAME] = self.df[TITLE_COL_NAME].apply(
            lambda x: " ".join([word for word in x.split() if word.lower() not in (stop)])
        )

    def _load_data(self, pair):
        df = self.df[self.df.Pairs.str.contains(pair)]
        self.data[pair] = df

    def load_data(self):
        for pair in self.pairs:
            self._load_data(pair)

    def load_temp_data(self):
        """to load newly saved data after a contribute story happened"""

        tempdf = pd.read_csv(MAIN_EN_DATA_PATH, low_memory=False)
        tempdf = pd.concat([tempdf, pd.read_csv(HHR_AO3_DATA_PATH)])
        tempdf[TITLE_WITHOUT_STOPWORDS_COL_NAME] = tempdf[TITLE_COL_NAME].apply(
            lambda x: " ".join([word for word in x.split() if word.lower() not in (stop)])
        )
        data_temp = {}
        for pair in self.pairs:
            data_temp[pair] = tempdf[tempdf.Pairs.str.contains(pair)]

        self.df = tempdf
        self.data = data_temp

        del tempdf
        del data_temp
