import logger
from utils.constants import (
    AUTHOR_SEARCH_TOKEN,
    LENGTH_SEARCH_TOKEN,
    SPECIAL_SEARCH_TOKENS,
    SUMM_TOKEN,
)

log = logger.get_logger(__name__)


class QueryMaker:
    def __init__(self, query):
        self.query = query
        self.query_spl = None
        self.query_full_spl = None
        self.query_special_spl = None
        self.author_search_word = None
        self.length_search_word = None
        self.summary_query = False

        # process query
        log.debug("Inside Querymaker init...")
        self._process_query()

    def _process_query(self):
        """execute processing of the query"""

        self._make_query()
        self._perform_summ_token_sweep()
        self._perform_special_token_sweep()

    def _make_query(self):
        """
        make searchable queries
        """

        log.debug(f"Making new query...for {self.query}")
        self.query_full_spl = self.query.lower().strip().split(" ")
        self.query_spl = [word for word in self.query_full_spl if word[:2] not in SPECIAL_SEARCH_TOKENS]
        self.query_special_spl = [word for word in self.query_full_spl if word[:2] in SPECIAL_SEARCH_TOKENS]
        self.query = " ".join(self.query_spl).strip()
        log.debug(
            f"Made new query: {self.query}, query split: {self.query_spl}, query spl split: {self.query_special_spl}, query full split: {self.query_full_spl}"
        )

    def _perform_special_token_sweep(self):
        """
        search for all the tokens mentioned in the query
        """

        # author name sweep
        author_search_word = [word for word in self.query_special_spl if AUTHOR_SEARCH_TOKEN in word]
        if author_search_word:
            self.author_search_word = author_search_word[0][2:]
            log.debug(f"Author given: {self.author_search_word}")
            if self.author_search_word is None or self.author_search_word == "":
                try:
                    ext_item_in_query_for_author_search_word = self.query_full_spl[
                        self.query_full_spl.index(AUTHOR_SEARCH_TOKEN) + 1
                    ]
                    log.debug(
                        f"Author search word likely has a space in between, trying this: {ext_item_in_query_for_author_search_word}"
                    )
                    if ext_item_in_query_for_author_search_word:
                        self.author_search_word = ext_item_in_query_for_author_search_word
                        log.debug(f"Author given: {self.author_search_word}")
                except Exception as e:
                    log.debug("Exception in author search word querymaker: ", e)

        # length sweep
        length_search_word = [word for word in self.query_special_spl if LENGTH_SEARCH_TOKEN in word]

        if length_search_word:
            self.length_search_word = length_search_word[0][2:]
            log.debug(f"Length given: {self.length_search_word}")
            if self.length_search_word is None or self.length_search_word == "":
                try:
                    ext_item_in_query_for_length_search_word = self.query_full_spl[
                        self.query_full_spl.index(LENGTH_SEARCH_TOKEN) + 1
                    ]
                    log.debug(
                        f"Length search word likely has a space in between, trying this: {ext_item_in_query_for_length_search_word}"
                    )
                    if ext_item_in_query_for_length_search_word:
                        self.length_search_word = ext_item_in_query_for_length_search_word
                        log.debug(f"Length given: {self.length_search_word}")
                except Exception as e:
                    log.debug("Exception in length search word querymaker: ", e)

    def _perform_summ_token_sweep(self):
        """
        search for the summary token mentioned in the query
        """

        if SUMM_TOKEN in self.query:
            self.summary_query = True
            self.query = self.query.replace(SUMM_TOKEN, "")
            log.debug(f"Summ given: {self.summary_query}")
            log.debug(f"New query={self.summary_query}")
