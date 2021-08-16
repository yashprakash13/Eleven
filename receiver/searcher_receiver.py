import json

from brain.index.index_engine import IndexEngine
from brain.query.querymaker import QueryMaker
from brain.result.resultmaker import ResultMaker
from brain.searcher.search_engine import SearchEngine
from utils.constants import MEDIUM_AO3_COL_VALUE, MEDIUM_FFN_COL_VALUE


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
            search_engine.type_3_is_complete,
        )
        result_list_of_dict = results_list.get_df_result_as_dict()

        self.result_to_return = result_list_of_dict

    def _get_result_as_list_of_dicts(self):
        """to return list of dict of results gotten"""

        return self.result_to_return

    def get_story_details(self, story_id):
        """get story details from db and return dataframe row as json"""

        return json.dumps(self.indexclass_obj.get_story_details(story_id).to_dict("records"))

    def get_all_fics(self, choice):
        """return all fics with "Medium" of "choice"""

        if choice == 0:
            # all fics
            allfics = self.indexclass_obj.get_all_fics()
        elif choice == 1:
            # only FFN fics
            allfics = self.indexclass_obj.get_all_fics(MEDIUM_FFN_COL_VALUE)
        else:
            # only AO3 fics
            allfics = self.indexclass_obj.get_all_fics(MEDIUM_AO3_COL_VALUE)

        return json.dumps(allfics.to_dict("records"))
