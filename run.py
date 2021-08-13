import os
import sys

ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
# os.environ.update({'ROOT_PATH': ROOT_PATH})
sys.path.append(ROOT_PATH)

from fastapi import FastAPI

from receiver.searcher_receiver import SearcherReceiver

app = FastAPI()
searcher_receiver = SearcherReceiver()
searcher_receiver.load_db()


@app.get("/ping")
async def root():
    """because this is always important ;)"""

    return {"hell yeah": "Pong"}


@app.get("/search")
async def search(query: str):
    """the search point"""

    results = searcher_receiver.search(query=query)
    return {"results": results}
