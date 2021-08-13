import os

MAIN_EN_DATA_PATH = os.path.join("data", "en", "metahp_nonullvalues_noduplicates_addedcols_touseforbackend_en.csv")
HHR_AO3_DATA_PATH = os.path.join("data", "ao3", "df_ao3_harmony.csv")

PSIE_INDEX_PATH = os.path.join("data", "en_psie", "indices")
# SIE_INDEX_PATH = os.path.join("data", "en_sie", "index")
# SIE_INDEX_NAME = "sie_id_embedsumm_tuple_index"
# SIE_EMBED_PATH = os.path.join("data", "en_sie", "embeds")
# SIE_EMBED_NAME = "id_summembed_tuple_en.pickle"
# ST_MODEL_NAME = "distilbert-base-nli-mean-tokens"

# search things
ALL_DF_COLUMNS = [
    "story_id",
    "author_id",
    "author_name",
    "title",
    "summary",
    "updated",
    "published",
    "language",
    "genres",
    "rated",
    "num_chapters",
    "num_words",
    "status",
    "characters",
    "num_reviews",
    "num_favs",
    "num_follows",
    "Pairs",
    "Lengths",
    "title_without_stopwords",
]

# column values
NO_CHARACTERS_COL_VALUE = "NoCharacters"
MEDIUM_FFN_COL_VALUE = "FFN"
MEDIUM_AO3_COL_VALUE = "AO3"
NOT_AVAILABLE_AUTHOR_COL_VALUE = "NotAvailable"
NO_PAIRS_COL_VALUE = "NoPairs"

# column names
CHARACTERS_COL_NAME = "characters"
STORY_ID_COL_NAME = "story_id"
TITLE_WITHOUT_STOPWORDS_COL_NAME = "title_without_stopwords"
TITLE_COL_NAME = "title"
SUMMARY_COL_NAME = "summary"
AUTHOR_COL_NAME = "author_name"
LENGTH_COL_NAME = "Lengths"


DEFAULT_PAIR = "Harmony"  # Yes, because that's MY ship. Period.
PAIRS_TO_LOOK_FOR = [
    "Harmony",
    "Haphne",
    "Jily",
    "Dramione",
    "Tomione",
    "Volmione",
    "Tomarry",
    "Drarry",
    "Jamione",
    "Snamione",
    "Sirimione",
    "Snarry",
    "Remione",
    "NoPairs",
    "OtherPair",
]  # Volmione + Tomione is in one.
PAIRS_TO_CALC_UNA_FOR = ["Harmony", "Jily", "Haphne", "Dramione", "Tomione"]
PAIR_CHARACTER_MAPPING = {
    "Harmony": ["Harry P.", "Hermione G."],
    "Jily": ["James P.", "Lily Evans P."],
    "Haphne": ["Harry P.", "Daphne G."],
    "Dramione": ["Draco M.", "Hermione G."],
    "Tomione": ["Tom R. Jr.", "Hermione G."],
}


ALL_LENGTHS = ["Small", "Medium", "Long", "VeryLong"]
SPECIAL_SEARCH_TOKENS = ["a:", "l:"]
AUTHOR_SEARCH_TOKEN = "a:"
LENGTH_SEARCH_TOKEN = "l:"
SUMM_TOKEN = "s:"

STANDARD_LENGTHS_TO_RETURN = 10
STANDARD_SCORE_CUTOFF = 75
NUM_MAX_RESULT_FETCH = 57
NUM_SUBSEQUENT_ROW_APPENDS = 3
SUMMARY_SCORE_CUTOFF = 0.8


# FAISS THINGS
M = 128
DIMENSION = 768
NLIST = 50
BYTES = 8


# APP THINGS
COLS_TO_SHOW_STORY_DETAIL = [
    "story_id",
    "title",
    "author_name",
    "rated",
    "summary",
    "genres",
    "num_chapters",
    "num_words",
    "status",
    "characters",
    "Medium",
]
COLS_TO_SEND_BY_HS_API = [
    "story_id",
    "title",
    "author_name",
    "rated",
    "summary",
    "genres",
    "num_chapters",
    "num_words",
    "status",
    "characters",
    "published",
    "updated",
]
COLS_TO_SAVE_STORY = ["story_id", "title", "author_name"]
COLS_TO_SHOW_THUMB = ["title", "author_name"]
COL_NAME_STORY = ["title"]


# API THINGS
API_URL_TO_FETCH_STORIES_META_FROM = "https://weaver.fanfic.dev/v0/ffn/crawl"
