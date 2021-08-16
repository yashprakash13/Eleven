import os
import sys

ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
# os.environ.update({'ROOT_PATH': ROOT_PATH})
sys.path.append(ROOT_PATH)

import logger

log = logger.setup_applevel_logger(file_name="app_debug.log")

from fastapi import FastAPI

from receiver.searcher_receiver import SearcherReceiver

app = FastAPI()
searcher_receiver = SearcherReceiver()
log.debug("Loading data...")
searcher_receiver.load_db()
log.debug("Loaded data.")


@app.get("/ping")
async def root():
    """because this is always important ;)"""

    log.debug("Pinging...")
    return {"hell yeah": "Pong"}


@app.get("/search")
async def search(query: str):
    """the search point"""

    log.debug(f"Calling searcher with query: {query}")
    results = searcher_receiver.search(query=query)
    return {"results": results}


@app.get("/get_story_details")
async def get_stort_details(storyid: int):
    """return story details from id"""

    log.debug(f"Calling story detail endpoint with storyid= {storyid}")
    details = searcher_receiver.get_story_details(story_id=int(storyid))
    return {"story_details": details}
