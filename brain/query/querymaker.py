from utils.constants import (
    AUTHOR_SEARCH_TOKEN,
    LENGTH_SEARCH_TOKEN,
    SPECIAL_SEARCH_TOKENS,
    SUMM_TOKEN,
)


class QueryMaker:
    def __init__(self, query):
        self.query = query
        self.query_spl = None
        self.query_special_spl = None
        self.author_search_word = None
        self.length_search_word = None
        self.summary_query = False

        # process query
        self._process_query()

    def _process_query(self):
        """execute processing of the query"""

        self._make_query()
        self._perform_summ_token_sweep
        self._perform_special_token_sweep()

    def _make_query(self):
        """
        make searchable queries
        """

        query_full_spl = self.query.strip().split(" ")
        self.query_spl = [word for word in query_full_spl if word[:2] not in SPECIAL_SEARCH_TOKENS]
        self.query_special_spl = [word for word in query_full_spl if word[:2] in SPECIAL_SEARCH_TOKENS]
        self.query = " ".join(self.query_spl).strip()

    def _perform_special_token_sweep(self):
        """
        search for all the tokens mentioned in the query
        """

        # author name sweep
        author_search_word = [word for word in self.query_special_spl if AUTHOR_SEARCH_TOKEN in word]
        if author_search_word:
            self.author_search_word = author_search_word[0][2:]

        # length sweep
        length_search_word = [word for word in self.query_special_spl if LENGTH_SEARCH_TOKEN in word]

        if length_search_word:
            self.length_search_word = length_search_word[0][2:]

    def _perform_summ_token_sweep(self):
        """
        search for the summary token mentioned in the query
        """

        if SUMM_TOKEN in self.query:
            self.summary_query = True
            self.query = self.query.replace(SUMM_TOKEN, "")
