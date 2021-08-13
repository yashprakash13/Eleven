from brain.index.index_engine import IndexEngine
from brain.query.querymaker import QueryMaker
from brain.result.resultmaker import ResultMaker
from brain.searcher.search_engine import SearchEngine


class SearcherReceiver:
    def __init__(self):
        self.query = None

        # indicator of whether data + indices have been prepared
        self.loaded_db = False

        # all the db class instances
        self.indexclass_obj = None
        self.querymakerclass_obj = None

        # results
        self.result_to_return = None

    def load_db(self):
        """load the db in case I'm reloading the API"""

        indices = IndexEngine()
        indices.load_psieindices()
        self.indexclass_obj = indices

    def search(self, query):
        """the func to execute search"""

        self.query = query

        # search fics
        self._search_fics()

        # return results
        return self._get_result_as_list_of_dicts()

    def _search_fics(self):
        """search and return results"""

        # make the query
        querymaker = QueryMaker(self.query)
        self.querymakerclass_obj = querymaker
        # search
        search_engine = SearchEngine(self.querymakerclass_obj, self.indexclass_obj)
        # make the results
        results_list = ResultMaker(
            self.indexclass_obj,
            search_engine.type_1_title_result_ids,
            search_engine.type_1_author_result_names,
            search_engine.type_1_length_result_name,
            search_engine.type_2_result_ids,
        )
        result_list_of_dict = results_list.get_df_result_as_dict()

        self.result_to_return = result_list_of_dict

    def _get_result_as_list_of_dicts(self):
        """to return list of dict of results gotten"""

        return self.result_to_return
