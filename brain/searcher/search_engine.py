import logger
from brain.data.data_engine import stop
from rapidfuzz import fuzz, process
from utils.constants import (
    ALL_LENGTHS,
    DEFAULT_PAIR,
    NO_PAIRS_COL_VALUE,
    STANDARD_LENGTHS_TO_RETURN,
    STANDARD_SCORE_CUTOFF,
    STORY_ID_COL_NAME,
    SUMMARY_COL_NAME,
    SUMMARY_SCORE_CUTOFF,
    TITLE_WITHOUT_STOPWORDS_COL_NAME,
)
from whoosh import qparser
from whoosh.qparser import MultifieldParser, QueryParser

log = logger.get_logger(__name__)


class SearchEngine:
    def __init__(self, queryclass_obj, indexclass_obj) -> None:
        self.queryclass_obj = queryclass_obj
        self.indexclassobj = indexclass_obj

        # to store and give result ids to the next in line
        self.type_2_result_ids = None
        self.type_1_title_result_ids = None
        self.type_1_length_result_name = None
        self.type_1_author_result_names = None

        # execute the search
        log.debug("Beginning search...")
        self._begin_search()

    def _begin_search(self):
        """the search executes"""

        # check if the summ token was provided -- TYPE 2 SEARCH
        if self.queryclass_obj.summary_query:
            # Type 2 search only
            log.debug("Doing type 2 search...")
            self._perform_type2_search()
        else:
            # Type 1 search only
            log.debug("Doing type 1 search...")
            self._perform_type1_search()

    def _perform_type2_search(self):
        """type 2 search"""

        ix_hhr = self.indexclassobj.psieindices[DEFAULT_PAIR]
        ix_nopairs = self.indexclassobj.psieindices[NO_PAIRS_COL_VALUE]

        result_ids = []

        # search both indices
        for index in list([ix_hhr, ix_nopairs]):
            with index.searcher() as searcher:
                or_group = qparser.OrGroup.factory(SUMMARY_SCORE_CUTOFF)
                query = QueryParser(SUMMARY_COL_NAME, index.schema, group=or_group).parse(self.queryclass_obj.query)

                results = searcher.search(query)

                ids = []
                for hit in results:
                    ids.append(int(hit[STORY_ID_COL_NAME]))

            # save the story ids found
            if ids:
                result_ids.extend(ids)
                log.debug(f"Found result ids: {len(result_ids)}")

        self.type_2_result_ids = result_ids

    def _perform_type1_search(self):
        """
        Type 1 search is executed here.
        Divided into three parts:
        1. Special token sweep (already done in QueryMaker class), filter result ids here
        2. Title search , filter result ids here
        3. Pass ids to ResultMaker class for making display appropriate results
        """
        # check if author token present
        if self.queryclass_obj.author_search_word:
            self._do_author_filtering(self.queryclass_obj.author_search_word)
        # check if length token present
        if self.queryclass_obj.length_search_word:
            self._do_length_filtering(self.queryclass_obj.length_search_word)

        # search for title
        if len(self.queryclass_obj.query_spl) > 0:
            self._do_title_filtering(self.queryclass_obj.query)

    def _do_author_filtering(self, author_query):
        """author filtering"""

        # list of all authors in df
        all_authors = self.indexclassobj.df.author_name.unique()

        res_r = process.extract(
            author_query,
            all_authors,
            scorer=fuzz.ratio,
            limit=STANDARD_LENGTHS_TO_RETURN,
            score_cutoff=STANDARD_SCORE_CUTOFF,
        )
        res_WR = process.extract(
            author_query,
            all_authors,
            scorer=fuzz.WRatio,
            limit=STANDARD_LENGTHS_TO_RETURN,
            score_cutoff=STANDARD_SCORE_CUTOFF,
        )

        # if no results
        if not res_r or not res_WR:
            return None, None

        if res_r[0][0] != res_WR[0][0]:
            # if WRatio and ratio return different top results, combine them
            authors_found = [res_r[0][0], res_WR[0][0]]
        else:
            # Otherwise, just keep one of them
            authors_found = [res_r[0][0]]

        authors_found.extend([r[0] for r in res_r])
        authors_found.extend([r[0] for r in res_WR])

        # remove duplicate author names
        seen = set()
        authors_found = [x for x in authors_found if not (x in seen or seen.add(x))]

        log.debug(f"Author filtering done. Authors found: {len(authors_found)}")

        self.type_1_author_result_names = authors_found

    def _do_length_filtering(self, length_query):
        """
        return the length mentioned in the query as the Column value of length from df
        """

        length_to_search_for = [le for le in ALL_LENGTHS if le.lower() in length_query.lower()]
        if length_to_search_for:
            self.type_1_length_result_name = length_to_search_for[0]

    def _do_title_filtering(self, title_query):
        """title search happens here"""

        log.debug(f"Doing title filtering for query: {title_query}")
        df = self.indexclassobj.df

        res_r = process.extract(
            " ".join([word for word in title_query.split() if word.lower() not in (stop)]),
            df[TITLE_WITHOUT_STOPWORDS_COL_NAME],
            scorer=fuzz.ratio,
            limit=STANDARD_LENGTHS_TO_RETURN,
            score_cutoff=STANDARD_SCORE_CUTOFF,
        )
        res_WR = process.extract(
            " ".join([word for word in title_query.split() if word.lower() not in (stop)]),
            df[TITLE_WITHOUT_STOPWORDS_COL_NAME],
            scorer=fuzz.WRatio,
            limit=STANDARD_LENGTHS_TO_RETURN,
            score_cutoff=STANDARD_SCORE_CUTOFF,
        )

        # if no results, do nothing
        if not res_r or not res_WR:
            return

        if res_r[0][2] != res_WR[0][2]:
            # if WRatio and ratio return different top results, combine them
            index_list = [res_r[0][2], res_WR[0][2]]
        else:
            # Otherwise, just keep one of them
            index_list = [res_r[0][2]]

        # then, append all other results fetched too
        index_list.extend([r[2] for r in res_WR if r[2] not in index_list])
        index_list.extend([r[2] for r in res_r if r[2] not in index_list])

        log.debug(f"Title filtering index list length= {len(index_list)}")

        story_ids_to_return = [df[df.index == i][STORY_ID_COL_NAME].iloc[0].item() for i in index_list]
        log.debug(f"Title filtering done. Titles found: {len(story_ids_to_return)}")

        self.type_1_title_result_ids = story_ids_to_return
