import os
import time

import AO3
import logger
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from utils.constants import (
    ALL_DF_COLUMNS,
    AO3_START_STRING,
    API_URL_TO_FETCH_STORIES_META_FROM,
    CHARACTERS_COL_NAME,
    FFN_START_STRING,
    GENRES_COL_NAME,
    LENGTH_COL_NAME,
    MEDIUM_AO3_COL_VALUE,
    MEDIUM_COL_NAME,
    MEDIUM_FFN_COL_VALUE,
    NO_PAIRS_COL_VALUE,
    PAIRS_COL_NAME,
    RATE_LIMIT,
)

load_dotenv()
log = logger.get_logger(__name__)


class Scraper:
    def __init__(self, base_url, name, start_page_no, end_page_no):
        self.base_url = base_url
        self.name = name
        self.start_page = int(start_page_no)
        self.end_page = int(end_page_no)

        self.master_df = None

        self._start_scrape()

    def _start_scrape(self):
        """start the scrape acc to website"""

        if FFN_START_STRING in self.base_url:
            log.debug("Starting scrape for ffn...")
            self._start_scrape_ffn()
        elif AO3_START_STRING in self.base_url:
            log.debug("Starting scrape for ao3...")
            self._start_scrape_ao3()
        log.debug("Scrape finished.")

    def _do_post_scraping_actions(self):
        """
        do necessary post scrape and pre-saving actions to dataframe and then save to disk
        """

        log.debug("Performing post scraping actions...")
        self.master_df[MEDIUM_COL_NAME] = MEDIUM_FFN_COL_VALUE

        # make the Lengths column
        self.master_df[LENGTH_COL_NAME] = self.master_df.apply(lambda row: self.get_length(row), axis=1)

        # make the pairing column N # TODO later change this, if I like.
        self.master_df[PAIRS_COL_NAME] = NO_PAIRS_COL_VALUE

        # Remove nan values
        self.master_df[GENRES_COL_NAME] = self.master_df[GENRES_COL_NAME].fillna("NoGenres")
        self.master_df[CHARACTERS_COL_NAME] = self.master_df[CHARACTERS_COL_NAME].fillna("NoCharacters")
        self.master_df["num_reviews"] = self.master_df["num_reviews"].fillna("NoReviews")
        self.master_df["num_favs"] = self.master_df["num_favs"].fillna("NoFavourites")
        self.master_df["num_follows"] = self.master_df["num_follows"].fillna("NoFollows")
        self.master_df["rated"] = self.master_df["rated"].fillna("NoRated")

        # save the dataframe as a csv file
        log.debug("Saving scraped file...")
        os.makedirs(os.path.join("data", self.name), exist_ok=True)
        filenameandpath = os.path.join("data", self.name, f"meta{self.name}_{self.start_page}_{self.end_page}.csv")
        self.master_df.to_csv(filenameandpath, index=False)
        log.debug("Saved scraped file.")

    def _start_scrape_ffn(self):
        """start ffn scrape"""

        rate_limit = 2
        master_metadata_df = pd.DataFrame(columns=ALL_DF_COLUMNS)
        for i in range(self.start_page, self.end_page + 1):
            # log which page
            log.debug(f"Scraping page {i}.")

            # build the url
            url = f"{self.base_url}{i}"

            # get page's metadata dataframe
            page_meta_df = self.scrape_all_stories_on_page(url)
            # print(page_meta_df)
            # append page's dataframe to the master dataframe
            master_metadata_df = master_metadata_df.append(page_meta_df)

            log.debug(f"DF size is now: {len(master_metadata_df)}")
            # sleep to be good about terms of service
            time.sleep(RATE_LIMIT)

        self.master_df = master_metadata_df

        self._do_post_scraping_actions()

    def _start_scrape_ao3(self):
        """start ao3 scrape"""

        feeddict = []
        for i in range(self.start_page, self.end_page + 1):
            log.debug(f"Scraping page {i}.")
            self._scrape_all_stories_on_page_ao3(i, feeddict)
            log.debug(f"DF size now= {len(feeddict)}")

        result = [d for d in feeddict if d is not None]
        log.debug("Saving scraped file...")
        df = pd.DataFrame(result)
        df[MEDIUM_COL_NAME] = MEDIUM_AO3_COL_VALUE

        os.makedirs(os.path.join("data", self.name), exist_ok=True)
        filenameandpath = os.path.join("data", self.name, f"meta{self.name}_{self.start_page}_{self.end_page}_ao3.csv")
        df.to_csv(filenameandpath, index=False)
        log.debug("Saved scraped file.")

    def _scrape_all_stories_on_page_ao3(self, page, feeddict):
        while True:
            html = requests.get(f"{self.base_url}{page}").content
            soup = BeautifulSoup(html, "html.parser")
            try:
                all_stories_ol = soup.find("ol", {"class": "work index group"}).findAll("li", recursive=False)
                a = time.time()
                for li in all_stories_ol:
                    workid = li.get("id")[5:]
                    classtext = str(li.get("class"))
                    try:
                        author_id = classtext[classtext.index("user-") + 5 : classtext.index("]") - 1]
                    except:
                        author_id = "NotAvailable"
                    try:
                        work = AO3.Work(int(workid))
                        work = self._get_work_dict(author_id, work)
                        feeddict.append(work)
                    except:
                        pass
                    time.sleep(1)
                break
            except:
                pass

    def _get_work_dict(self, author_id, work):
        author_names_list = []
        for author in work.authors:
            author_names_list.append(author.username)

        if work.complete:
            status = "Complete"
        else:
            status = "Incomplete"
        try:
            workdict = {
                "story_id": work.id,
                "title": work.title,
                "author_name": "!".join(author_names_list),
                "author_id": author_id,
                "num_chapters": work.nchapters,
                "characters": ", ".join(work.characters[:7]),  # THIS IS DIFF THAN FFNET
                "status": status,
                "language": work.language,
                "rated": work.rating,
                "num_words": work.words,
                "genres": "!".join(work.tags[:7]),
                "summary": work.summary,
                "updated": work.date_updated,
                "published": work.date_published,
                "Pairs": "Harmony",
                "Lengths": self.get_length_ao3(int(work.words)),
                # AO3 EXTRA FIELDS
                "categories": "!".join(work.categories[:7]),
                "num_kudos": work.kudos,
            }
        except Exception as e:
            workdict = None

        return workdict

    def scrape_all_stories_on_page(self, url):
        # names of the classes on fanfiction.net
        story_root_class = "z-list zhover zpointer"

        # get response from Weaver API
        while True:
            response = requests.get(
                API_URL_TO_FETCH_STORIES_META_FROM,
                params={"q": url},
                data={"apiKey": os.environ.get("WEAVER_DATA_VALUE")},
                auth=(
                    os.environ.get("WEAVER_AUTH_USERNAME"),
                    os.environ.get("WEAVER_AUTH_PASSWORD"),
                ),
            )
            if response.status_code == 200:
                break
            time.sleep(1)

        # html = requests.get(url).text
        soup = BeautifulSoup(response.text, features="html.parser")
        # get all the stories on the page
        all_stories_on_page = soup.find_all("div", {"class": story_root_class})

        # the dataframe for the page
        page_metadata_df = pd.DataFrame(columns=ALL_DF_COLUMNS)

        for story in all_stories_on_page:
            # get story metadata dict to convert to df
            story_id, metadata_story_dict = self.scrape_story_blurb(story)
            metadata_story_dict["story_id"] = story_id

            # convert story's metadata to dataframe
            story_metadata_df = pd.DataFrame.from_dict(metadata_story_dict)

            # append the story dataframe to the page's dataframe
            page_metadata_df = page_metadata_df.append(story_metadata_df)

        # return full page metadata dataframe
        return page_metadata_df

    def scrape_story_blurb(self, story):
        # names of the classes on fanfiction.net
        title_class = "stitle"
        metadata_div_class = "z-padtop2 xgray"
        backup_metadata_div_class = "z-indent z-padtop"

        title = story.find(class_=title_class).get_text()
        story_id = story.find(class_=title_class)["href"].split("/")[2]

        # some steps to get to the author id
        links = story.find_all("a")
        author_url = [link["href"] for link in links if "/u/" in link["href"]]
        author_name = [link.text for link in links if "/u/" in link["href"]]

        try:
            author_id = author_url[0].split("/")[2]
        except:
            author_id = "NOTAVAILABLE"

        metadata_div = story.find("div", class_=metadata_div_class)

        # this happened once, on story ID 268931
        if metadata_div is None:
            metadata_div = story.find("div", class_=backup_metadata_div_class)
            start_idx = metadata_div.text.index("Rated")
            metadata_div_text = metadata_div.text[start_idx:]
        else:
            metadata_div_text = metadata_div.get_text()

        summ_metadata_div = story.find("div", class_=backup_metadata_div_class)
        summary = summ_metadata_div.text[: summ_metadata_div.text.index("Rated")]

        # get publish/update times
        times = metadata_div.find_all(attrs={"data-xutime": True})
        if len(times) == 2:
            updated = times[0].text
            published = times[1].text
        else:
            updated = times[0].text
            published = updated

        # get the rest of the metadata
        metadata_parts = metadata_div_text.split("-")
        genres = self.get_genres(metadata_parts[2].strip())

        language = metadata_parts[1].strip()

        metadata = {
            "author_id": author_id,
            "author_name": author_name,
            "title": title,
            "summary": summary,
            "updated": updated,
            "published": published,
            "language": language,
            "genres": genres,
        }

        # thanks a lot to the fanfic library!
        for parts in metadata_parts:
            parts = parts.strip()
            # already dealt with language and genres- everything else should have name: value
            tag_and_val = parts.split(":")
            if len(tag_and_val) != 2:
                continue
            tag, val = tag_and_val
            tag = tag.strip().lower()
            if tag not in metadata:
                val = val.strip()
                try:
                    val = int(val.replace(",", ""))
                    metadata["num_" + tag] = val
                except:
                    metadata[tag] = val

        # see if we have characters and/or completion
        last_part = metadata_parts[len(metadata_parts) - 1].strip()
        if last_part == "Complete":
            metadata["status"] = "Complete"
            # have to get the second to last now
            metadata["characters"] = str(self.get_characters_from_string(metadata_parts[len(metadata_parts) - 2]))
        else:
            metadata["status"] = "Incomplete"
            if last_part.startswith("Published"):
                metadata["characters"] = [None]
            else:
                metadata["characters"] = str(self.get_characters_from_string(last_part))

        return story_id, metadata

    def get_genres(self, genre_text):
        if genre_text.startswith("Chapters"):
            return [None]
        genres = genre_text.split("/")
        # Hurt/Comfort is annoying because of the '/'
        corrected_genres = []
        for genre in genres:
            if genre == "Hurt":
                corrected_genres.append("Hurt/Comfort")
            elif genre == "Comfort":
                continue
            else:
                corrected_genres.append(genre)

        genres_string = "!".join(corrected_genres)
        return genres_string

    def get_characters_from_string(self, string):
        stripped = string.strip()
        if stripped.find("[") == -1:
            return stripped.split(", ")
        else:
            characters = []
            num_pairings = stripped.count("[")
            for idx in range(0, num_pairings):
                open_bracket = stripped.find("[")
                close_bracket = stripped.find("]")
                characters.append(self.get_characters_from_string(stripped[open_bracket + 1 : close_bracket]))
                stripped = stripped[close_bracket + 1 :]
            if stripped != "":
                singles = self.get_characters_from_string(stripped)
                [characters.append(character) for character in singles]

            return characters

    def get_length(self, row):
        if row.num_words <= 50000:
            return "Small"
        elif row.num_words <= 100000 and row.num_words >= 50000:
            return "Medium"
        elif row.num_words <= 200000 and row.num_words >= 100000:
            return "Long"
        else:
            return "VeryLong"

    def get_length_ao3(self, num_words):
        if num_words <= 50000:
            return "Small"
        elif num_words <= 100000 and num_words >= 50000:
            return "Medium"
        elif num_words <= 200000 and num_words >= 100000:
            return "Long"
        else:
            return "VeryLong"
