"""The Index Engine class"""
import os

import logger
import whoosh.index as windex
from brain.data.data_engine import DataEngine
from utils.constants import (
    DEFAULT_PAIR,
    MEDIUM_COL_NAME,
    NO_PAIRS_COL_VALUE,
    PAIRS_COL_NAME,
    PAIRS_TO_LOOK_FOR,
    PSIE_INDEX_PATH,
    STORY_ID_COL_NAME,
)
from whoosh.fields import *
from whoosh.index import create_in

log = logger.get_logger(__name__)


class IndexEngine(DataEngine):
    def __init__(self, pairs=[DEFAULT_PAIR, NO_PAIRS_COL_VALUE]):
        self.pairs = pairs
        DataEngine.__init__(self, pairs)
        self.psieindices = {}
        # self.sieindex = None
        # self.sie_ids = None
        DataEngine.load_data(self)
        DataEngine.load_una_ids(self)

    def _load_psieindex(self, pair):
        ix = windex.open_dir(os.path.join(PSIE_INDEX_PATH, pair.lower()))
        self.psieindices[pair] = ix

    def _make_psieindex(self, pair, load_or_not=False, refresh_psieindex=False):
        schema = Schema(story_id=ID(stored=True), summary=TEXT)
        if refresh_psieindex or not os.path.exists(os.path.join(PSIE_INDEX_PATH, pair.lower())):
            os.mkdir(os.path.join(PSIE_INDEX_PATH, pair.lower()))
        else:
            return
        ix = create_in(os.path.join(PSIE_INDEX_PATH, pair.lower()), schema)
        if load_or_not:
            self.psieindices.append(ix)

        df_pair = self.data[pair]
        id_list = df_pair.story_id.to_list()
        summary_list = df_pair.summary.to_list()

        writer = ix.writer()
        for i in range(0, len(id_list)):
            writer.add_document(story_id=str(id_list[i]), summary=str(summary_list[i]))
        writer.commit()

    def make_psieindices(self, load_or_not=False):
        for pair in self.pairs:
            self._make_psieindex(pair, load_or_not)

    def load_psieindices(self):
        log.debug("Loading psie indices...")
        for pair in self.pairs:
            self._load_psieindex(pair)
        log.debug("Loaded psie indices.")

    def get_story_details(self, storyid):
        """to get story detail row from db and return to receiver"""

        log.debug("Fetching story details...")
        row = self.df.loc[self.df[STORY_ID_COL_NAME] == storyid]
        log.debug("Story details fetched.")
        return row

    def get_all_fics(self, medium=None):
        """to get and return to receiver all stories of "Medium" column value"""

        log.debug("Fetching all fics...")
        return (
            self.df.loc[(self.df[PAIRS_COL_NAME] == DEFAULT_PAIR) & (self.df[MEDIUM_COL_NAME] == medium)]
            if medium
            else self.df.loc[self.df[PAIRS_COL_NAME] == DEFAULT_PAIR]
        )

    # def _load_sie_ids(self):
    #     embed_tuple = self._read_sie_tuple()
    #     self.sie_ids = embed_tuple[0]

    # def _read_sie_tuple(self):
    #     with open(os.path.join(SIE_EMBED_PATH, SIE_EMBED_NAME), "rb") as f:
    #         embed_tuple = pickle.load(f)
    #     return embed_tuple

    # def _make_sieindex(self):
    #     # read the embed file
    #     embed_tuple = self._read_sie_tuple()

    #     # get the embeddings list
    #     embed = embed_tuple[1]

    #     # faiss quantizer
    #     quantizer = faiss.IndexFlatL2(DIMENSION)
    #     # define a new inverted index
    #     index = faiss.IndexIVFPQ(quantizer, DIMENSION, NLIST, M, BYTES)

    #     # train and then add data to index
    #     index.train(embed)
    #     index.add(embed)

    #     # save to disk
    #     faiss.write_index(index, os.path.join(SIE_INDEX_PATH, SIE_INDEX_NAME))

    # def make_sieindex(self):
    #     if os.path.exists(os.path.join(SIE_INDEX_PATH, SIE_INDEX_NAME)):
    #         print("sie index already present.")
    #         return
    #     print("sie index not found. Making one.")
    #     self._make_sieindex()
    #     print("Made sie index.")

    # def _load_sieindex(self):
    #     self.sieindex = faiss.read_index(os.path.join(SIE_INDEX_PATH, SIE_INDEX_NAME))

    # def load_sieindex(self):
    #     print("Loading sie index.")
    #     self._load_sieindex()
    #     self._load_sie_ids()
    #     print("Loaded sie index.")
