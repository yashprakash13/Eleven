import json

import logger
from brain.index.index_engine import IndexEngine
from brain.index.index_engine_oth import IndexEngineOth
from brain.query.querymaker import QueryMaker
from brain.result.resultmaker import ResultMaker
from brain.searcher.search_engine import SearchEngine
from utils.constants import (
    FANDOM_TAG_PRIDE_AND_PREJUDICE,
    FANDOM_TAG_SEX_EDUCATION,
    FANDOM_TAG_STRANGER_THINGS,
    MEDIUM_AO3_COL_VALUE,
    MEDIUM_FFN_COL_VALUE,
    OTHER_SUPPORTED_FANDOMS,
)

log = logger.get_logger(__name__)


class SearcherReceiver:
    def __init__(self):
        self.query = None

        # indicator of whether data + indices have been prepared
        self.loaded_db = False

        # all the db class instances
        self.indexclass_obj = None
        self.querymakerclass_obj = None
        self.indices_to_use = None

        # results
        self.result_to_return = None

    def load_db(self):
        """load the db in case I'm reloading the API"""

        indices = IndexEngine()
        indices.load_psieindices()
        self.indexclass_obj = indices

        # load other supported fandom's indices
        self.indexclass_obj_oth_st = IndexEngineOth(FANDOM_TAG_STRANGER_THINGS)
        self.indexclass_obj_oth_se = IndexEngineOth(FANDOM_TAG_SEX_EDUCATION)
        self.indexclass_obj_oth_pp = IndexEngineOth(FANDOM_TAG_PRIDE_AND_PREJUDICE)

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

        # search with the correct indices
        self.indices_to_use = self.indexclass_obj
        if self.querymakerclass_obj.is_other_fandom_maker_present:
            log.debug(f"Calling search engine with fandom {self.querymakerclass_obj.other_fandom_marker}...")
            if self.querymakerclass_obj.other_fandom_marker == FANDOM_TAG_PRIDE_AND_PREJUDICE:
                search_engine = SearchEngine(self.querymakerclass_obj, self.indexclass_obj_oth_pp)
                self.indices_to_use = self.indexclass_obj_oth_pp
            elif self.querymakerclass_obj.other_fandom_marker == FANDOM_TAG_SEX_EDUCATION:
                search_engine = SearchEngine(self.querymakerclass_obj, self.indexclass_obj_oth_se)
                self.indices_to_use = self.indexclass_obj_oth_se
            elif self.querymakerclass_obj.other_fandom_marker == FANDOM_TAG_STRANGER_THINGS:
                search_engine = SearchEngine(self.querymakerclass_obj, self.indexclass_obj_oth_st)
                self.indices_to_use = self.indexclass_obj_oth_st
        else:
            search_engine = SearchEngine(self.querymakerclass_obj, self.indexclass_obj)

        # make the results
        results_list = ResultMaker(
            self.indices_to_use,
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

        row_found = None
        for indices in [
            self.indexclass_obj,
            self.indexclass_obj_oth_pp,
            self.indexclass_obj_oth_st,
            self.indexclass_obj_oth_se,
        ]:
            found = indices.get_story_details(story_id)
            if len(found) > 0:
                row_found = found
                break

        return json.dumps(row_found.to_dict("records"))

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

    def save_new_story(self, storyid, medium):
        """save new story to csvdb"""

        if medium == 1:
            # save as FFN story
            self.indexclass_obj.save_story(storyid, MEDIUM_FFN_COL_VALUE)
        elif medium == 2:
            # save as AO3 story
            self.indexclass_obj.save_story(storyid, MEDIUM_AO3_COL_VALUE)
