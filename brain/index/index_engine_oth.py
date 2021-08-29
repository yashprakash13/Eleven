import logger
from brain.data.data_engine_oth import DataEngineOth
from utils.constants import MEDIUM_COL_NAME, STORY_ID_COL_NAME

log = logger.get_logger(__name__)


class IndexEngineOth(DataEngineOth):
    def __init__(self, fandom):
        DataEngineOth.__init__(self, fandom)

    def get_story_details(self, storyid):
        """to get story detail row from db and return to receiver"""

        log.debug("Fetching story details...")
        row = self.df.loc[self.df[STORY_ID_COL_NAME] == int(storyid)]
        log.debug(f"Story details fetched: {row[STORY_ID_COL_NAME]}")
        return row

    def get_all_fics(self, medium=None):
        """to get and return to receiver all stories of "Medium" column value"""

        log.debug("Fetching all fics...")
        return self.df.loc[self.df[MEDIUM_COL_NAME] == medium] if medium else self.df
