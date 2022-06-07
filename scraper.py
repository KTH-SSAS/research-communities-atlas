import requests, json, re
import pickle, sys
from storage import *
from graph_analyzer import CITATION_TRUNCATION_THRESHOLD
import time, sys, os
from datetime import datetime
import urllib.parse

CURRENT_YEAR = datetime.today().year
SCRAPE_START_YEAR = 1945
SCRAPE_END_YEAR = CURRENT_YEAR
YEAR_MAX_SCRAPE = 4999
YAC_FILE = 'yac.json'
APY_FILE = 'apy.json'
KEYWORDS_FILE = 'in_scope_keywords.txt'
QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
QUERIES_BETWEEN_SAVES = 3000

class Scraper:
    def __init__(self, api_key, scraping_persisitence=1, automated=True, datastore_default_kind=True, datastore_kind_suffix=None, yac_file=YAC_FILE):
        self.api_key = api_key
        self.__user_agent = "elsapy-v%s"
        self.scraping_persisitence = scraping_persisitence

        # Scopus only allows (May 2017, but they are revising this restriction) 5,000 records to be downloaded from any
        # search. Because we want 300,000 records, we split the search into years. (This still doesn't give us 300,000,
        # but a lot more than 5,000). However, we would skew the results if we downloaded 5,000 from 2015, which had 25.000
        # articles, as well as 1992, which had about 5,000 articles. 1992 would influence the results too much. therefore,
        # we collect from each year only as many as are proportionally right. To do so, we need to know the total number of
        # records per year. The YearlyCountScraper class does that. Results are then stored on the yac.json file.
        # As of 2019 this restriction is no longer in place and we should investigate scraping further in the future!
 
        if os.path.isfile(yac_file):
            print("Reading from " + yac_file + " file...")
            self.yearly_publication_count = json.load(open(yac_file))
        else:
            print("ERROR on class Scraper: yac.json file not found! Terminating...")
            sys.exit(1)

        print("Scraper now initiating storage...")
        self.storage = CloudStorage(automated=automated, datastore_default_kind=datastore_default_kind, datastore_kind_suffix=datastore_kind_suffix)
        self.database = self.storage.database

        self.database.read_keywords_from_file(KEYWORDS_FILE)

    def exec_request(self, url, n_attempts=3, sleep_time=0, request_timeout=60):
        if n_attempts <= 0:
            print("Returning None from exec_request. No more attempts.")
            return None
        else:
            if sleep_time > 0:
                print("Sleeping for " + str(sleep_time) + " seconds.")
            time.sleep(sleep_time)
            if self.storage.keystroke_list:
                print("Detected keystroke. Terminating.")
                sys.exit()

            headers = {
                "X-ELS-APIKey": self.api_key,
                "User-Agent": self.__user_agent,
                "Accept": 'application/json'
            }
            print("|.", end=' ')
            start_req = time.time()
            try:
                r = requests.get(url, headers=headers, timeout=request_timeout)
            except requests.exceptions.ReadTimeout:
                print("Failed request after trying " + str(request_timeout) + " seconds.")
                return self.exec_request(url, n_attempts=n_attempts - 1, sleep_time=30, request_timeout = 2*request_timeout)
            except Exception as e:
                print("Unspecified exception.")
                print(type(e).__name__)
                print(e.args)
                print(e)
                return self.exec_request(url, n_attempts=n_attempts - 1)
            print(".|")
            # Print the remaining limit for Scopus API key
            limit_remain = r.headers.get('X-RateLimit-Remaining')
            print("Remaining limit for current API key: ", end=' ')
            print(limit_remain, end=' ')
            print(" out of ", end=' ')
            print(r.headers.get('X-RateLimit-Limit'))
            print("Limit resetting on: ", end=' ')
            print(r.headers.get('X-RateLimit-Reset'))
            try:
                if int(limit_remain) == 0:
                    print("WARNING: Query quota exceeded!")
                    return QUOTA_EXCEEDED
            except TypeError:
                print("WARNING: Query quota = None!")
            if r.status_code == 200:
                if r.text == "":
                    print("On exec_request: FATAL ERROR, response is empty!")
                    return None
                print("Successful HTTP request took " + str(time.time() - start_req) + " seconds.")
                return json.loads(r.text)
            else:
                if r.status_code == 400:
                    print("Error 400: Are you calling from a legitimate IP address?")
                    print(url)
                    print(r.text)
                    return self.exec_request(url, n_attempts=n_attempts - 1, sleep_time=300, request_timeout=2*request_timeout)
                if r.status_code == 404:
                    print("Error 404: Resource not found.")
                    print(url)
                    print(r.text)
                    return self.exec_request(url, n_attempts=n_attempts - 1, sleep_time=0, request_timeout=2*request_timeout)
                if r.status_code == 429:
                    print("Error 429: Too many request. Quota exceeded?")
                    print(url)
                    print(r.text)
                    return QUOTA_EXCEEDED
                else:
                    print("Unexpected HTTP error:")
                    print(str(r.status_code))
                    print("Error from ")
                    print(url)
                    print("using headers ")
                    print(headers)
                    print(r.text)
                    return self.exec_request(url, n_attempts=n_attempts - 1, sleep_time=0, request_timeout=2*request_timeout)

    def scrape_eid_based_on_author_and_title(self, first_author_surname, title):
        __uri_base = 'https://api.elsevier.com/content/search/scopus?&query='
        mangled_title = re.sub('[^A-Za-z0-9]+', ' ', title)
        mangled_surname = re.sub('[^A-Za-z0-9]+', ' ', first_author_surname)
        sorting = "&sort=citedby-count"
        uri = __uri_base + "title(" + mangled_title + ")%20and%20authlastname(" + mangled_surname + ")" + sorting
        print("Trying to scrape EID of " + first_author_surname + ", " + title)
        api_response = self.exec_request(uri, n_attempts=2*self.scraping_persisitence, request_timeout=15)
        if api_response is None:
            print("Something went wrong when attempting to retrieve EID of " + first_author_surname + ", " + title)
            return None
        elif api_response == QUOTA_EXCEEDED:
            print("INFO: Scraping EID based on author and title failed due to quota limit!")
            return QUOTA_EXCEEDED
        try:
            eid = api_response['search-results']['entry'][0]['eid']
            if isinstance(eid, str):
                try:
                    print("Successfully retrieved EID (" + eid + ") of " + first_author_surname + ", " + title)
                except Exception as e:
                    print("Unspecified error")
                    print(e)
                return eid
            else:
                print("Didn't find key eid in scraped results due to type error, EID doesn't seem to be a string.")
                print(api_response['search-results']['entry'][0]['eid'])
                return None

        except (KeyError, TypeError, UnicodeEncodeError) as e:
            print("Didn't find key eid in scraped results due to key, type or unicode error.")
            print(e)
            print(api_response['search-results'])
            return None

    def append_eid(self, article, rescraping=False):
        if (article.title is not None and article.first_author_surname is not None):
            # Either find the EID in the database...
            print("Article has author name and title.")
            if not rescraping: # Removing the article and adding it again (in a new position) is not needed when rescraping since the hash should remain the same
                a = self.database.get_article_based_on_description(article)
                if a is not None:
                    return a
                # ... or scrape from Scopus
                eid = self.scrape_eid_based_on_author_and_title(article.first_author_surname, article.title)
                if eid is not None:
                    # Adding the EID, and moving to new location in dictionary
                    if article in self.database.articles:
                        a = self.database.articles.pop(article)
                    else:
                        print("Weirdly, the article was not found in the database.")
                    article.eid = eid
                    if self.database.get_article(article) is None:
                        self.database.articles[hash(article)] = article
                    return True
                else:
                    print("Didn't find key eid in scraped results.")
                    return False
            elif rescraping: # Do this instead
                eid = self.scrape_eid_based_on_author_and_title(article.first_author_surname, article.title)
                if eid is not None:
                    article.eid = eid
                    return True
                else:
                    print("Didn't find key eid in scraped results.")
                    return False
        else:
            print("Article does not have any useful information for retrieving its EID.")
            return False

    def scrape_json_from_eid(self, article):
        __uri_base = 'https://api.elsevier.com/content/abstract/eid/'
        try:
            uri = __uri_base + article.eid
        except TypeError:
            print("WARNING: TypeError on 'scrape_json_from_eid'!")
            return None
        api_response = self.exec_request(uri, n_attempts=3*self.scraping_persisitence, request_timeout=15)
        if api_response == QUOTA_EXCEEDED:
            print("INFO: Scraping from EID failed due to quota limit!")
            return QUOTA_EXCEEDED
        elif api_response is not None:
            data = api_response['abstracts-retrieval-response']
            return data
        else:
            print("Failed to scrape from EID.")
            return None

    def get_authors(self, article, data, rescraping=False):
        no_database_add = rescraping
        try:
            for author_record in data['authors']['author']:
                self.add_author(article, author_record, no_database_add)
            if not rescraping: # Supress output when rescraping
                print("Found authors ", end=' ')
                for a in article.authors:
                    print(a.surname + ", ", end=' ')
                print()
        except (KeyError, TypeError):
            try:
                self.add_author(article, data['coredata']['dc:creator']['author'], no_database_add)
                if not rescraping: # Supress output when rescraping
                    print("Found author " + article.get_first_author_surname())
            except (KeyError, TypeError):
                print("Couldn't find any authors in record.")
                return None

    # This method should be used if for any reason the Datastore storing procedure gets interupted after the article storing but before the authror/keyword storing.
    def scrape_complete_incomplete_articles(self):
        print("Scraping missing author(s) and keywords...")
        articles_to_rescrape = self.storage.get_articles_to_rescrape()
        if articles_to_rescrape:
            for article in articles_to_rescrape:
                print("Now resraping missing author(s)/keywords for article with eid = " + article.eid + "...")
                self.complete_only_author_and_keywords_from_eid(article)
            print("Now saving on Datastore...")
            self.storage.store()
        else:
            print("Great news: No missing author(s) and/or keywords detected!")
        print("Finished! Terminating...")

    # This method should be used in order to try to scrape the most highly articles that are not already fully scraped (i.e. the ones with an asterisk (*) on analysis resutls).
    def scrape_highly_cited_not_scraped_articles(self):
        print("First, update article records and citations...")
        self.storage.database.update_author_article_records()
        self.storage.database.update_article_citation_records()
        print("Scraping all highly cited but not fully scraped articles...")
        for article, citations in sorted([(a, len(a.citations)) for k, a in self.storage.database.articles.items()], key=operator.itemgetter(1), reverse=True):
            if citations >= CITATION_TRUNCATION_THRESHOLD and article.eid is None:
                hash_before = hash(article)
                # print("Article before completion:")
                # print(article.full_descriptive_string())
                self.complete_article(article, assume_in_scope=False, rescraping=True) # If assumne_in_scope = True then all the asterisked articles will be completed
                hash_after = hash(article)
                if hash_after != hash_before:
                    print("WARNING: Hash violation during rescraping detected for article: " + article.description())
                    sys.exit(1)
                # print("Article after completion:")
                # print(article.full_descriptive_string())
        print("Finished scraping!")
        print("Now saving on Datastore...")
        self.storage.store()
        # self.storage.store(article_kind="Article-complete", author_kind="Author-complete", keyword_kind="Keyword-complete")
        print("Exiting...")

    def add_author(self, article, author_record, no_database_add=False):
        try:
            auid = author_record['@auid']
        except (KeyError, TypeError) as e:
            print("Strange AUID. Aborting.")
            print(e)
            return None
        a = Author(auid)
        if 'ce:given-name' in author_record:
            a.given_name = author_record['ce:given-name']
        else:
            if 'preferred-name' in author_record and 'ce:given-name' in author_record['preferred-name']:
                a.given_name = author_record['preferred-name']['ce:given-name']
            else:
                print("Couldn't add author given name. KeyError.")
        if 'ce:surname' in author_record:
            a.surname = author_record['ce:surname']
        else:
            print("Couldn't add author surname.")
        if 'affiliation' in author_record:
            if isinstance(author_record['affiliation'], dict):
                affdict = author_record['affiliation']
            else:
                affdict = author_record['affiliation'][0]
            aff = Affiliation(affdict['@id'])
            a.affiliation = aff
        article.authors.append(a)
        if not no_database_add:
            self.database.add_author(a)

    def get_keywords(self, article, data):
        if 'idxterms' in data and data['idxterms'] is not None:
            for mainterm in data['idxterms']['mainterm']:
                if mainterm is not None:
                    if isinstance(mainterm, str):
                        print("mainterm is now a string: " + mainterm)
                    else:
                        m = Keyword(mainterm['$'].lower())
                        article.keywords.add(m)
                        self.database.add_keywords(m)
        if 'authkeywords' in data and data['authkeywords'] is not None:
            try:
                for author_keyword in data['authkeywords']['author-keyword']:
                    if author_keyword is not None:
                        ak = Keyword(author_keyword['$'].lower())
                        article.keywords.add(ak)
                        self.database.add_keywords(ak)
            except TypeError:
                if len(data['authkeywords']['author-keyword']) == 2:
                    if data['authkeywords'] is not None:
                        keywords_string = data['authkeywords']['author-keyword']['$'].lower()
                        keywords = keywords_string.split(",")
                        for keyword in keywords:
                            ak = Keyword(keyword)
                            article.keywords.add(ak)
                            self.database.add_keywords(ak)

    def scrape_references(self, article, data):
        print("Attempting to scrape references of " + article.title)
        try:
            for ref in data['item']['bibrecord']['tail']['bibliography']['reference']:
                reference_article = Article(None)
                self.set_reference_title(ref, reference_article)
                self.set_reference_author(ref, reference_article)
                self.set_reference_year(ref, reference_article)
                print("Scraped reference " + reference_article.description())
                reference_article = self.database.add_article(reference_article)
                if reference_article is not None:
                    article.references.add(reference_article)
        except (TypeError, KeyError) as e:
            print(article.title + " seems to have no references, because there is a key error or type error in the scraped record.")
            print(data)
            print("Except inside 'scrape_references'")
            print(e)
            print("/Except inside 'scrape_references'")

    def set_reference_title(self, ref, reference_article):
        try:
            reference_article.title = ref['ref-info']['ref-title']['ref-titletext']
        except:
            try:
                reference_article.title = ref['ref-info']['ref-sourcetitle']
            except:
                print("Could not find title of reference. ", end=' ')
                print(ref)

    def set_reference_author(self, ref, reference_article):
        if 'ref-info' in ref and 'ref-authors' in ref['ref-info'] and 'author' in ref['ref-info']['ref-authors']:
            ref_auths = ref['ref-info']['ref-authors']
            auths = ref_auths['author']
            if isinstance(auths, dict):
                reference_article.first_author_surname = auths['ce:surname']
            else:
                reference_article.first_author_surname = auths[0]['ce:surname']
        else:
            print("Could not find authors of reference.", end=' ')
            print(ref)

    def set_reference_year(self, ref, reference_article):
        try:
            reference_article.date = ref['ref-info']['ref-publicationyear']['@first']
        except:
            print("Could not find publication year of reference. ", end=' ')
            # print(ref)

    def needs_to_be_completed(self, article, assume_in_scope=False):
        if article.description() == "Anonymous, No title" and article.eid is not None:
            print("Assessing the completeness of article with EID=" + article.eid + ":")
        else:
            print("Assessing the completeness of \'" + article.description() + "\':")
        if article.fully_scraped:
            print("Article was already scraped, so there is no more to collect from Scopus.")
            return False
        if article.not_in_scopus:
            print("Article is not in Scopus.")
            return False
        if article.out_of_scope and not assume_in_scope:
            print("Article already assessed as out of scope.")
            return False
        print("Artile may be in Scopus")
        if article.eid is None:
            print("Article has no EID, so needs to be completed.")
            return True
        print("Article has EID: " + article.eid)
        if article.is_complete():
            print("Article is already complete.")
            return False
        print("Article is incomplete.")
        return True

    def complete_article_from_eid(self, article, update_references=False, assume_in_scope=False, rescraping=False):
        print("Completing article from EID.")
        data = self.scrape_json_from_eid(article)
        if data == QUOTA_EXCEEDED:
            print("Stopping article completion...")
            self.query_exceeded_handler()
        elif data is not None:
            # If rescraping or updating references check that the completed article matches lexicographically the original one and if not restore the original one
            if rescraping or update_references:
                # Create a new article with the completed data in order to check for matching
                new_article = Article(article.eid)
                new_article.title = data['coredata']['dc:title']
                try:
                    new_article.date = data['coredata']['prism:coverDate']
                except (KeyError, TypeError):
                    pass
                self.get_authors(new_article, data, rescraping=True)
                if new_article.get_authors() is not None:
                    new_article.first_author_surname = new_article.authors[0].surnamef()
                if not Database.articles_lexicographically_same(article, new_article):
                    print("Warning: Completed article does not lexicographically match the original one, reverting to original!")
                    print("Article after false completion:")
                    print(new_article.full_descriptive_string())
                    # Revert article to the original one
                    article.eid = None
                    print("Original reverted article:")
                    print(article.full_descriptive_string())
                    return False
                else:
                    print("Info: Completed article will pass the lexicographical check, proceeding with completion...")
            self.get_keywords(article, data)
            print("Found keywords: ", end=' ')
            print([a.name for a in article.keywords])
            if self.database.is_in_scope(article) or assume_in_scope:
                if not assume_in_scope:
                    print("Article is in scope!")
                else:
                    print("Article is in scope (or is assumed to be).")
                self.get_authors(article, data)
                if not rescraping: # Prevent first_author_surname and article title to change because this will affect the uniquness of the hash! (Alternative solution: Update all existing citations with the new hash!)
                    if article.get_authors() is not None:
                        article.first_author_surname = article.authors[0].surnamef()
                    try: # This tries to solve/pass the issue where an article did not have 'dc:title'...
                        article.title = data['coredata']['dc:title']
                    except (KeyError, TypeError):
                        return False
                print("Found title of article: " + article.title)
                try:
                    article.source = Source(data['coredata']['prism:publicationName'])
                    print("Found source: " + article.source.name)
                except (KeyError, TypeError) as e:
                    print("Could not find source. KeyError or TypeError.")
                try:
                    article.date = data['coredata']['prism:coverDate']
                    print("Found date: " + article.date)
                except (KeyError, TypeError) as e:
                    print("Could not find date. KeyError or TypeError.")
                # Scrape all the references (unless ordered not to do so)
                self.scrape_references(article, data)
                if update_references:
                    print("Now updating each reference individually.")
                    self.scrape_references_of_article(article, save_to_database=False)
                return True
            else:
                print("Aborting further flesh-out. " + article.description() + " is out of scope.")
                article.print_keywords()
                article.out_of_scope = True
                return False
        else:
            print("Failed to scrape json from EID.")
            return False

    def complete_only_author_and_keywords_from_eid(self, article):
        print("Completing article's author(s) and keywords from EID.")
        data = self.scrape_json_from_eid(article)
        if data == QUOTA_EXCEEDED:
            print("Stopping article completion...")
            self.query_exceeded_handler()
        elif data is not None:
            self.get_keywords(article, data)
            print("Found keywords: ", end=' ')
            print([a.name for a in article.keywords])
            self.get_authors(article, data)
        else:
            print("Failed to scrape json from EID.")
            return False

    def complete_article(self, article, update_references=False, assume_in_scope=False, rescraping=False):
        # Check that the article really needs to be completed
        if not self.needs_to_be_completed(article, assume_in_scope):
            print("Article doesn't need to be completed.")
            return "Unchanged"
        # If the article is incomplete, check that it has an EID
        if article.eid is None:
            # If not, try to add the EID.
            print("Article has no EID.")
            eid_was_appended = self.append_eid(article, rescraping)
            if not eid_was_appended:
                print("Failed to find an EID for the article. Marking article as not_in_scopus. Aborting further attempts to flesh out.")
                article.not_in_scopus = True
                return "Failed"
        # Now that the article has an EID, scrape all its details.
        successful_completion = self.complete_article_from_eid(article, update_references, assume_in_scope, rescraping)
        # If the scraping wasn't successful, the article record is incomplete, and we must return
        if not successful_completion:
            print("Did not complete article.")
            return "Failed"
        # If completed
        print("Completed " + article.description() + ".")
        # Add the article on the articles_with_eid and update it on database
        self.database.articles_with_eid.append(article)
        if rescraping: # Only if rescraping, because otherwise it is not needed since it does not already exist (and no overwrite is needed)
            self.database.articles[hash(article)] = article
        if not rescraping: # Avoid getting the hash of the article changed when rescraping highly cited yet not fully scraped articles
            article.fully_scraped = True
        return "Completed"

    # This is only used when the 'update_references' flag is set to True
    def scrape_references_of_article(self, article, save_to_database = False):
        i = 0
        for reference in article.references:
            i += 1
            # If the article already is stored, return the stored copy, else store the new
            reference = self.database.add_article(reference)
            if reference is not None:
                print("Updating reference " + str(i) + " of " + article.title + ":")
                results = self.complete_article(reference, update_references=False, assume_in_scope=False)
                if results == "Completed":
                    if save_to_database:
                        print("Saving database after addition of " + reference.description())
                        self.storage.store()
        article.references_are_updated = True # This is a flag that means that all the references_are_scraped

    def scrape_all_references(self):
        at_least_one_reference_updated = False
        for key, article in list(self.database.articles.items()):
            print("Considering scraping " + article.description() + ". Are references already updated?")
            if not article.references_are_updated:
                print("No. Is article out of scope?")
                if not article.out_of_scope:
                    print("No. Is article in Scopus?")
                    if not article.not_in_scopus:
                        print("As far as I know.")
                        self.scrape_references_of_article(article, save_to_database=True)
                        at_least_one_reference_updated = True
        return at_least_one_reference_updated

    def scrape_all_references_repeatedly(self):
        at_least_one_reference_updated = True
        while at_least_one_reference_updated:
            at_least_one_reference_updated = self.scrape_all_references()

    # Not used (so far)
    def scrape_these_articles(self, missing_articles):
        counter = 0
        unsaved = 0
        for missing_article in missing_articles:
            counter += 1
            print("\nTrying to scrape missing article number " + str(counter) + "/" + str(
                len(missing_articles)) + ": " + missing_article)
            author = missing_article.partition(', ')[0]
            title = missing_article.partition(', ')[2]
            eid = self.scrape_eid_based_on_author_and_title(author, title)
            if eid is not None:
                print("EID is " + eid)
                article = Article(eid)
                article = self.database.add_article(article)
                print("The database already (before scraping) has this information on the article:")
                print(article.full_descriptive_string())
                if article is not None:
                    article.fully_scraped = False
                    results = self.complete_article(article, update_references=False, assume_in_scope=True)
                    if results == "Completed":
                        unsaved += 1
                        if unsaved > QUERIES_BETWEEN_SAVES:
                            unsaved = 0
                            print("Saving database after addition of " + article.description())
                            self.storage.store()
            else:
                print("Could not find EID.")
        print("Final database save")
        self.storage.store()
        print("Iterated through all articles, saving them to the database.")

    def scrape_200_articles_from_query(self, query, start_index=0, upper_limit=YEAR_MAX_SCRAPE):
        if upper_limit - start_index > 200:
            count = 200
        else:
            count = upper_limit - start_index
        uri = 'https://api.elsevier.com/content/search/scopus?count=' + str(count) + '&query=' + query + '&start=' + str(start_index) + '&sort=citedby-count'
        print("Scraping search results starting from record " + str(start_index) + ".")
        search_result = self.exec_request(uri, n_attempts=5*self.scraping_persisitence, sleep_time=0, request_timeout=60)
        if search_result == QUOTA_EXCEEDED:
            print("Stopping article scraping...")
            self.query_exceeded_handler()
        elif search_result is None:
            print("Something went wrong in self.exec_request(uri) when attempting to retrieve articles matching the query " + query + ".")
            print("Cannot continue.")
            self.query_exceeded_handler()
        else:
            return search_result["search-results"]

    def scrape_200_articles_from_query_using_cursor(self, query, count=200, cursor_index='*', upper_limit=YEAR_MAX_SCRAPE):
        uri = 'https://api.elsevier.com/content/search/scopus?count=' + str(count) + '&query=' + query + '&cursor=' + urllib.parse.quote(cursor_index) + '&sort=citedby-count'
        print("Scraping search results starting from record " + str(cursor_index) + ".")
        search_result = self.exec_request(uri, n_attempts=5*self.scraping_persisitence, sleep_time=0, request_timeout=60)
        if search_result == QUOTA_EXCEEDED:
            print("Stopping article scraping...")
            self.query_exceeded_handler()
        elif search_result is None:
            print("Something went wrong in self.exec_request(uri) when attempting to retrieve articles matching the query " + query + ".")
            print("Cannot continue.")
            self.query_exceeded_handler()
        else:
            return search_result["search-results"]

    def scrape_all_articles_from_query(self, query, upper_limit, queries_between_saves=QUERIES_BETWEEN_SAVES):
        print("\n\n\nGetting articles for query " + query + ".")
        print("Size of db is " + str(len(self.database.articles)) + " articles.")
        if upper_limit <= 4999:
            search_result = self.scrape_200_articles_from_query(query, start_index=0, upper_limit=upper_limit)
        else:
            search_result = self.scrape_200_articles_from_query_using_cursor(query, cursor_index='*', upper_limit=upper_limit)
        total_results = int(search_result["opensearch:totalResults"])
        if total_results < upper_limit or upper_limit == 0:
            upper_limit = total_results
        print("Found " + str(total_results) + " for " + query)
        print("Limiting query to " + str(upper_limit) + " records")
        read_results = 0
        unsaved = 0
        start = time.time()
        while read_results < upper_limit - 1:
            for entry in search_result["entry"]:
                if 'eid' in entry:
                    print("\n\n Scraping article " + str(read_results) + " of query " + query)
                    article = Article(entry["eid"])
                    # If the article already is stored, return the stored copy, else store the new
                    article = self.database.add_article(article)
                    if article is not None:
                        results = self.complete_article(article, update_references=False, assume_in_scope=True) # If update_references is set to True, each individual reference will be fully scraped!
                        if results == "Completed":
                            unsaved += 1
                            if unsaved > queries_between_saves:
                                print("Saving database after addition of " + article.description())
                                self.storage.store()
                                unsaved = 0
                                end = time.time()
                                print("Time of processing " + str(queries_between_saves) + " queries (up to query " + str(read_results) + "): " + str(end - start))
                                start = time.time()
                read_results += 1
            if read_results < upper_limit - 1:
                if upper_limit <= 4999:
                    search_result = self.scrape_200_articles_from_query(query, start_index=(read_results + 1), upper_limit=upper_limit)
                else:
                    new_cursor_index = search_result["cursor"]["@next"]
                    search_result = self.scrape_200_articles_from_query_using_cursor(query, cursor_index=new_cursor_index, upper_limit=upper_limit)

    def scrape_all_articles_from_keywords(self, keyword):
        query = 'key(' + keyword.name + ') OR title(' + keyword.name + ')'
        self.scrape_all_articles_from_query(query, 4800)

    @DeprecationWarning
    def scrape_all_articles_from_keyword_permutations(self, keywords):
        query_start = 'KEY ( "'
        query_ending = '") AND ( SUBJAREA(COMP) OR SUBJAREA(ENGI) OR SUBJAREA(MATH) OR SUBJAREA(SOCI) OR SUBJAREA(BUSI) OR SUBJAREA(DECI) OR SUBJAREA(MULT) OR SUBJAREA(Undefined)) AND (LANGUAGE(English) )'
        max_records = YEAR_MAX_SCRAPE
        for pubyear in range(SCRAPE_START_YEAR, SCRAPE_END_YEAR):
            for keyword in keywords:
                query = query_start + keyword + query_ending + ' AND PUBYEAR IS ' + str(pubyear)
                self.scrape_all_articles_from_query(query, max_records)
            # for keyword_1 in keywords:
            #     for keyword_2 in keywords:
            #         query = query_start + keyword_1 + query_ending + u' AND PUBYEAR IS ' + str(pubyear)
            #         self.scrape_all_articles_from_query(query, max_records)
            #     for keyword_1 in keywords:
            #         for keyword_2 in keywords:
            #             for keyword_3 in keywords:
            #                 query = query_start + keyword_1 + query_ending + u' AND PUBYEAR IS ' + str(pubyear)
            #                 self.scrape_all_articles_from_query(query, max_records)

    def scrape_proportionally_per_year(self, base_query):
        max_count = YEAR_MAX_SCRAPE
        max_yearly_publication_count = max(self.yearly_publication_count.values())
        # for year in range(2017, 2018): # FOR TETING ONLY!!!
        articles_to_scrape_per_year = dict() # Dictionary to hold how many articles will be scraped every year
        for year in range(int(min(self.yearly_publication_count.keys())), int(max(self.yearly_publication_count.keys()))+1):
            full_query = '(' + base_query + ' AND PUBYEAR IS ' + str(year) + ')'
            url = 'https://api.elsevier.com/content/search/scopus?count=10&query=' + full_query + '&start=1&sort=citedby-count'
            count = int(max_count*self.yearly_publication_count[str(year)] / max_yearly_publication_count)
            articles_to_scrape_per_year[year] = int(count)
            if count > 1:
                self.scrape_all_articles_from_query(full_query, count)
        json.dump(articles_to_scrape_per_year, open(APY_FILE, 'w')) # Write the dictionary to a file
        print("Final database save.")
        self.storage.store()

    # This method scrapes all the articles for each year without restriction!
    def scrape_everything_per_year(self, base_query):
        for year in range(SCRAPE_START_YEAR, SCRAPE_END_YEAR):
            full_query = '(' + base_query + ' AND PUBYEAR IS ' + str(year) + ')'
            url = 'https://api.elsevier.com/content/search/scopus?count=10&query=' + full_query + '&start=1&sort=citedby-count'
            self.scrape_all_articles_from_query(full_query, upper_limit=0)
        print("Final database save.")
        self.storage.store()

    def store_after_signal(self):
        self.storage.store()
        return True

    def query_exceeded_handler(self):
        # First restore the stdout to the sys.stdout
        sys.stdout = sys.__stdout__
        answer = ""
        while answer not in ["y", "n"]:
            answer = input("Now stopping, do you want to store the database before exiting? [y/n] ").lower()
        print(answer)
        if answer == "y":
            print("Storing first and then exit...")
            ret = self.store_after_signal()
            if ret == False:
                print("FATAL ERROR: Something went really wrong during storing...")
                sys.exit(1)
            else:
                print("Exiting...")
                sys.exit(0)
        else:
            sys.exit(1)

class YearlyCountScraper:
    def __init__(self, api_key, base_query):
        self.baseScraper = BaseScraper(api_key)
        self.base_query = base_query
        self.yearly_count = dict()

    def getArticleCount(self, year):
        query_suffix = ' AND PUBYEAR IS '
        query = '(' + self.base_query + query_suffix + str(year) + ')'
        url = 'https://api.elsevier.com/content/search/scopus?count=10&query=' + query + '&start=1&sort=citedby-count'
        print(url)
        result = self.baseScraper.scrape(url)
        try:
            print(result['search-results']['opensearch:totalResults'])
            self.yearly_count[year] = int(result['search-results']['opensearch:totalResults'])
        except Exception as e:
            print(e)

    def getArticleCountAllYears(self):
        for year in range(SCRAPE_START_YEAR, SCRAPE_END_YEAR):
            self.getArticleCount(year)

    def getYearlyArticleCount(self):
        return self.yearly_count

# Because the main scraper doesn't find the afiliation names, but only the affiliation ids, we need a separate scrape to mathc ids to names.
class AffiliationScraper:
    def __init__(self, api_key, datastore_default_kind=True, datastore_kind_suffix=None):
        self.baseScraper = BaseScraper(api_key)
        self.storage = CloudStorage(auxiliary_use=True, datastore_default_kind=datastore_default_kind, datastore_kind_suffix=datastore_kind_suffix)

    def scrape_afiliation_from_id(self, id):
        url_intro = 'https://api.elsevier.com/content/search/affiliation?query=AF-ID('
        url_outro = ')'
        url = url_intro + str(id) + url_outro
        result = self.baseScraper.scrape(url)
        try:
            affiliation_name = result['search-results']['entry'][0]['affiliation-name']
        except Exception as e:
            if result == QUOTA_EXCEEDED:
                affiliation_name = result
            else:
                print("Unexpected formatting of scraped results. Returning 'unknown affiliation'")
                print(e)
                affiliation_name = "Unknown Affiliation"
        try:
            affiliation_country = result['search-results']['entry'][0]['country']
        except:
            print("Could not get country of affiliation. Returning 'unknown country'")
            affiliation_country = "Unknown Country"
        return affiliation_name, affiliation_country

    def scrape_affiliation_dict(self, affiliation_ids, current_affiliations_dict):
        affiliation_dict = current_affiliations_dict
        for affiliation_id in affiliation_ids:
            if (affiliation_id not in current_affiliations_dict) or (affiliation_id in current_affiliations_dict and current_affiliations_dict[affiliation_id].country is None):
                if affiliation_id in current_affiliations_dict and current_affiliations_dict[affiliation_id].country is None:
                    print("Affiliation with id=" + affiliation_id + " already in database, but completing its country...")
                aff_name, aff_country = self.scrape_afiliation_from_id(affiliation_id)
                if aff_name == QUOTA_EXCEEDED:
                    print("Stopping affiliation scraping...")
                    return affiliation_dict
                affiliation_dict[affiliation_id] = Affiliation(affiliation_id, aff_name, aff_country)
            else:
                print("Affiliation with id=" + affiliation_id + " already in database, skipping...")
        return affiliation_dict

    def store_affiliation_dict(self, affiliation_dict):
        self.storage.affiliation_store(affiliation_dict)
        return True

    def load_affiliation_dict(self):
        affiliation_dict = self.storage.database.affiliations
        affiliation_ids = set([])
        for key, author in self.storage.database.authors.items():
            try:
                affiliation_ids.add(author.affiliation.id)
            except Exception as e:
                print(e)
        return affiliation_dict, affiliation_ids

class BaseScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.__user_agent = "elsapy-v%s"

    def scrape(self, url):
        headers = {
            "X-ELS-APIKey": self.api_key,
            "User-Agent": self.__user_agent,
            "Accept": 'application/json'
        }
        print("|.", end=' ')
        start_req = time.time()
        try:
            r = requests.get(url, headers=headers, timeout=60)
        except requests.exceptions.ReadTimeout:
            print("Failed request after trying over 1 minute.")
        except Exception as e:
            print("Unspecified exception.")
            print(type(e).__name__)
            print(e.args)
            print(e)
        print(".|")
        # Print the remaining limit for Scopus API key
        limit_remain = r.headers.get('X-RateLimit-Remaining')
        print("Remaining limit for current API key: ", end=' ')
        print(limit_remain, end=' ')
        print(" out of ", end=' ')
        print(r.headers.get('X-RateLimit-Limit'))
        print("Limit resetting on: ", end=' ')
        print(r.headers.get('X-RateLimit-Reset'))
        try:
            if int(limit_remain) == 0:
                print("WARNING: Query quota exceeded!")
                return QUOTA_EXCEEDED
        except TypeError:
            print("WARNING: Query quota = None!")
        if r.status_code == 200:
            print("Successful HTTP request took " + str(time.time() - start_req) + " seconds.")
            return json.loads(r.text)
        else:
            print("Unexpected HTTP error:")
            print(str(r.status_code))
            print("Error from ")
            print(url)
            print("using headers ")
            print(headers)
            print(r.text)
