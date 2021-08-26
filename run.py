import os
import sys

ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
# os.environ.update({'ROOT_PATH': ROOT_PATH})
sys.path.append(ROOT_PATH)

import logger

log = logger.setup_applevel_logger(file_name="app_debug.log")

from typing import Any, Dict

import uvicorn
from fastapi import FastAPI

from receiver.searcher_receiver import SearcherReceiver
from scraper.scraper import Scraper

app = FastAPI()
searcher_receiver = SearcherReceiver()
log.debug("Loading data...")
searcher_receiver.load_db()
log.debug("Loaded data.")


@app.get("/ping")
async def ping():
    """because this is always important ;)"""

    log.debug("Pinging...")
    return {"hell yeah": "Pong"}


@app.get("/search")
async def search(query: str):
    """The search point"""

    log.debug(f"Calling searcher with query: {query}")
    results = searcher_receiver.search(query=query)
    return results


@app.get("/get_story_details")
async def get_story_details(storyid: int):
    """Return story details from id"""

    log.debug(f"Calling story detail endpoint with storyid= {storyid}")
    details = searcher_receiver.get_story_details(story_id=int(storyid))
    return details


@app.get("/get_all_fics")
async def get_all_fics(choice=0):
    """
    Return all HHr fics with the criteria:
    0 = All
    1 = FFN only
    2 = AO3 only
    """

    log.debug(f"Getting all fics with option = {choice}")
    allfics = searcher_receiver.get_all_fics(choice=choice)
    return {"all_fics": allfics}


@app.get("/save_new_story")
async def save_new_story(storyid, medium):
    """
    To save story to csvdb, with medium criteria:
    1: FFN
    2: AO3
    """

    log.debug(f"Saving story with id = {storyid}, and medium= {medium}")
    # searcher_receiver.save_new_story(storyid, medium)
    return {"Done": "OK"}


@app.get("/scrape")
async def scrape(base_url, name, start_page, end_page):
    """to scrape metadata and save"""

    log.debug(f"Calling scraper with url: {base_url}, startpage={start_page}, endpage = {end_page}")
    scraper = Scraper(base_url, name, start_page, end_page)
    # return {"Done": "OK"}


# run the API
if __name__ == "__main__":
    uvicorn.run(app, port=8080, host="localhost")
