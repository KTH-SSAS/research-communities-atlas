import pickle, sys, json, os
import _thread
import operator
import gzip
import time
import unicodedata
import uuid
import jellyfish, mmh3
from threading import Thread

# Imports the Google Cloud client library
from google.cloud import datastore

# Defines for storing in Datastore
MAX_PUT_LIST_SIZE = 300 # 500 is the maximum supported by the Google Datastore API but 300 proved to be the fastest choice
JARO_DISTANCE_GENERAL_THRESHOLD = 0.95 # To be used as it is
JARO_DISTANCE_COMBINED_THRESHOLD = 0.90 # To be used with check on publication year
JARO_DISTANCE_NAME_THRESHOLD = 0.80 # To be only used with max threshold on title and publication year
JARO_DISTANCE_TITLE_THRESHOLD = 0.75 # To be only used with max threshold on surname and publication year
JARO_DISTANCE_MAX = 1.00
ARTICLE_KIND = 'Article'
AUTHOR_KIND = 'Author'
KEYWORD_KIND = 'Keyword'
AFFILIATION_KIND = 'Affiliation'
# Below flags are for debugging
DATABASE_SMOKE_TEST = False
DATABASE_READ_ONLY = False

class Author(object):
    def __init__(self, auid, surname=None, given_name=None, affiliation=None, articles=None):
        self.auid = auid
        self.surname = surname
        self.given_name = given_name
        self.affiliation = affiliation
        if articles is None:
            self.articles = set([])
        else:
            self.articles = articles
        self.citation_cnt = 0

    def __eq__(self, other):
        return self.auid == other.auid

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return mmh3.hash(self.auid, signed=False)

    def safe_unicode(self, obj, *args):
        """ return the unicode representation of obj """
        try:
            return str(obj, *args)
        except UnicodeDecodeError:
            # obj is byte string
            ascii_text = str(obj).encode('string_escape')
            return str(ascii_text)

    def safe_str(self, obj):
        """ return the byte string representation of obj """
        try:
            return str(obj)
        except UnicodeEncodeError:
            # obj is unicode
            return str(obj).encode('unicode_escape')

    def surnamef(self):
        return self.safe_str(self.surname)

    def given_namef(self):
        return self.safe_str(self.given_name)

    def affiliationf(self):
        return self.safe_str(self.affiliation)

    def copy(self):
        the_copy = Author(self.auid)
        the_copy.auid = self.auid
        the_copy.surname = self.surname
        the_copy.given_name = self.given_name
        the_copy.affiliation = self.affiliation
        the_copy.articles = self.articles
        return the_copy

    def get_cited_authors(self):
        cited_authors = set([])
        for article in self.articles:
            for reference in article.references:
                cited_authors.update(reference.authors)
        return cited_authors

    def full_name(self):
        desc = self.surname
        if self.given_name is not None:
            desc += ", " + self.given_name
        return desc

    def keywords(self):
        keywords = dict()
        for article in self.articles:
            for keyword in article.keywords:
                if keyword in keywords:
                    keywords[keyword] += 1
                else:
                    keywords[keyword] = 1
        return keywords

    def sources(self):
        sources = dict()
        for article in self.articles:
                if article.source in sources:
                    sources[article.source] += 1
                else:
                    sources[article.source] = 1
        return sources
    
    def get_keywords(self):
        keywords = dict()
        for article in self.articles:
            for keyword in article.keywords:
                if keyword.name in keywords:
                    keywords[keyword.name] += 1
                else:
                    keywords[keyword.name] = 1
        return keywords

    def get_articles(self):
        articles_array = []
        for article in self.articles:
            articles_array.append(str(hash(article)))
        if not articles_array:
            return None
        else:
            return articles_array

    def get_affiliation_id(self):
        if self.affiliation is None:
            return None
        else:
            return self.affiliation.id

class Article(object):
 
    def __init__(self, eid, title='No title', date=None, source=None, fsurname='Anonymous', authors=None, keywords=None, references=None, citations=None, refs_updated=False, not_in_scopus=False, out_of_scope=False, fully_scraped=False, references_array=None, citations_array=None):
        self.title = title
        self.eid = eid
        self.date = date
        self.source = source
        self.first_author_surname = fsurname
        if authors is None:
            self.authors = []
        else:
            self.authors = authors
        if keywords is None:
            self.keywords = set([])
        else:
            self.keywords = keywords
        if references is None:
            self.references = set([])
        else:
            self.references = references
        if references_array is None:
            self.references_array = []
        else:
            self.references_array = references_array
        if citations is None:
            self.citations = set([])
        else:
            self.citations = citations
        if citations_array is None:
            self.citations_array = []
        else:
            self.citations_array = citations_array
        self.references_are_updated = refs_updated
        self.not_in_scopus = not_in_scopus
        self.out_of_scope = out_of_scope
        self.fully_scraped = fully_scraped
        
    def __eq__(self, other):
        # First check if they have the same hashes
        if self.__hash__() == other.__hash__():
            if self.title == 'No title' and self.first_author_surname == 'Anonymous' and self.eid == None:
                # Cannot perform equality check if there is no eid, no title and no author's surname
                return False
            else:
                #print("Hashes were equal!")
                return True
        # Then check if they have the same eids
        elif (hasattr(self, 'eid') and self.eid is not None) and (hasattr(other, 'eid') and other.eid is not None) and (self.eid == other.eid):
            #print("EIDs were the same")
            return True
        else:
            # Using Jaro-Winkler distance measurement for equality!
            jaro_distance = jellyfish.jaro_distance(str(self), str(other))
            if jaro_distance >= JARO_DISTANCE_GENERAL_THRESHOLD:
                #print("Jaro told us they are the same!")
                Database.merge_articles(self, other, verbose=True)
                return True
            else:
                return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        # Using MurmurHash3 because Python's hash() is not unique among runs and because MurmurHash is really fast!
        if hasattr(self, 'eid') and self.eid is not None and (self.fully_scraped or (self.title == "No title" and self.first_author_surname == "Anonymous")):
            return mmh3.hash(self.eid, signed=False)
        else:
            if hasattr(self, 'title') and self.title is not None:
                # This is a workaround for the transformation to Python 3 in order not to change the problematic hashes
                if 'İ' in self.title:
                    t = self.title.replace('İ', 'I').lower()
                else:
                    t = self.title.lower()
            else:
                t = ''
            f = ''
            if self.get_first_author_surname() is not None:
                # This is a workaround for the transformation to Python 3 in order not to change the problematic hashes
                if 'İ' in self.get_first_author_surname():
                    f = self.get_first_author_surname().replace('İ', 'I').lower()
                else:
                    f = self.get_first_author_surname().lower()
            tf = t + f # Plese note that the hash is generated from "TitleSurname" and not from "Surname, Title"
            try:
                h = mmh3.hash(tf, signed=False)
            except UnicodeDecodeError as e:
                print(e)
                print(tf)
            return h

    def __str__(self):
        f = ''
        if self.get_first_author_surname() is not None:
            f = self.get_first_author_surname().lower()
        if hasattr(self, 'title') and self.title is not None:
            t = self.title.lower()
        else:
            t = ''
        return f + ', ' + t

    @staticmethod
    def upgrade_copy(the_original):
        the_copy = Article(the_original.eid)
        the_copy.title = the_original.title
        the_copy.date = the_original.date
        the_copy.source = the_original.source
        the_copy.first_author_surname = the_original.first_author_surname
        the_copy.authors = the_original.authors
        the_copy.keywords = the_original.keywords
        the_copy.references = the_original.references
        the_copy.citations = the_original.citations
        the_copy.references_are_updated = the_original.references_are_updated
        the_copy.not_in_scopus = the_original.not_in_scopus
        the_copy.out_of_scope = the_original.out_of_scope
        the_copy.fully_scraped = the_original.fully_scraped
        return the_copy

    def is_complete(self):
        return not (self.eid is None or
                    self.title is None or
                    self.date is None or
                    self.source is None or
                    not self.authors or
                    not self.keywords or
                    not self.references)

    def safe_unicode(self, obj, *args):
        """ return the unicode representation of obj """
        try:
            return str(obj, *args)
        except UnicodeDecodeError:
            # obj is byte string
            ascii_text = str(obj).encode('string_escape')
            return str(ascii_text)

    def safe_str(self, obj):
        """ return the byte string representation of obj """
        try:
            return str(obj)
        except UnicodeEncodeError:
            # obj is unicode
            return str(obj).encode('unicode_escape')

    def get_first_author_surname(self):
        if hasattr(self, 'first_author_surname') and self.first_author_surname and isinstance(self.first_author_surname, str) and self.first_author_surname != 'Anonymous':
            return self.safe_unicode(self.first_author_surname)
        else:
            try:
                return self.safe_unicode(self.authors[0].surname)
            except:
                return 'Anonymous'

    def description(self):
        desc = self.get_first_author_surname()
        try:
            title = self.safe_unicode(self.title)
            desc += ', '
            desc += title
        except UnicodeDecodeError as e:
            try:
                desc += ', ' + self.title
            except Exception as ee:
                print(ee)
                print("...proceeding despite exception...")
                print("desc = " + desc)
                desc += ', Untitled'
        return desc

    def description_with_EID(self):
        desc = self.get_first_author_surname()
        if self.title is not None:
            desc += ', ' + self.title
        else:
            desc += ', Untitled'
        if self.eid is not None and isinstance(self.eid, str):
            desc += ' (' + self.eid + ')'
        else:
            desc += " (no EID)"
        return desc

    def description_with_year(self):
        desc = self.get_first_author_surname()
        try:
            title = self.safe_unicode(self.title)
            desc += ', '
            desc += title
        except UnicodeDecodeError:
            try:
                desc += ', ' + self.title
            except Exception as ee:
                print(ee)
                print("...proceeding despite exception...")
                print("desc = " + desc)
                desc += ', Untitled'
        if self.date is not None:
            desc += ', '
            desc += self.date[:4]
        return desc

    def full_descriptive_string(self):
        s = ""
        if self.not_in_scopus:
            s += "Not in Scopus, "
        else:
            s += "In Scopus, "
        if self.out_of_scope:
            s += "Out of scope, "
        else:
            s += "In scope, "
        if self.is_complete():
            s += "Is complete, "
        else:
            s += "Not complete, "
        if self.get_first_author_surname() is not None:
            s += self.get_first_author_surname() + ", "
        if self.title is not None:
            s += self.safe_unicode(self.title) + ", "
        if self.eid is not None:
            s += self.safe_unicode(self.eid) + ", "
        for author in self.authors:
            if author.surname is not None and author.given_name is not None:
                s += self.safe_unicode(author.surname) + ", " + self.safe_unicode(author.given_name) + ", "
        if self.source is not None:
            s += self.safe_unicode(self.source.name) + ", "
        if self.date is not None:
            s +=  self.safe_unicode(self.date) + ", "
        for keyword in self.keywords:
            s += self.safe_unicode(keyword.name) + ", "
        for reference in self.references:
            s += "[" + reference.description() + "], "
        s += "\n"
        return self.safe_unicode(s)

    def descriptionf(self):
        return self.safe_str(self.description())

    def first_author_surnamef(self):
        return self.safe_str(self.get_first_author_surname())

    def print_keywords(self):
        for kw in self.keywords:
            print(kw.name + ",", end=' ')

    def get_keywords(self):
        keywords_array = []
        for kw in self.keywords:
            keywords_array.append(kw.name)
        if not keywords_array:
            return None
        else:
            return keywords_array

    def get_authors(self):
        authors_array = []
        for auth in self.authors:
            authors_array.append(auth.auid)
        if not authors_array:
            return None
        else:
            return authors_array

    def get_references(self):
        references_array = []
        for ref in self.references:
            references_array.append(str(hash(ref)))
        if not references_array:
            return None
        else:
            return references_array

    def get_citations(self):
        citations_array = []
        for cit in self.citations:
            citations_array.append(str(hash(cit)))
        if not citations_array:
            return None
        else:
            return citations_array

    def get_source(self):
        if self.source is None:
            return None
        else:
            return self.source.name

    def print_reference_to_article(self, first_author_surname, title):
        for reference in self.references:
            if first_author_surname == reference.first_author_surname or first_author_surname in [auth.surname for auth in reference.authors] or first_author_surname is None:
                if title == reference.title or title is None:
                    print(reference.get_first_author_surname() + ", " + reference.title)

    def match_author(self, author_surname):
        author_surnames = set([])
        if self.first_author_surname is not None:
            author_surnames.add(self.first_author_surname)
        for a in self.authors:
            author_surnames.add(a.surname)
        return author_surname in author_surnames


class Source(object):
    def __init__(self, name):
        self.name = name

    def safe_str(self, obj):
        """ return the byte string representation of obj """
        try:
            return str(obj)
        except UnicodeEncodeError:
            # obj is unicode
            return str(obj).encode('unicode_escape')

    def namef(self):
        return self.safe_str(self.name)


class Affiliation(object):
    def __init__(self, id, name=None, country=None):
        self.id = id
        self.name = name
        self.country = country

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return mmh3.hash(self.id, signed=False)


class Keyword(object):
    def __init__(self, name, count=1):
        self.name = name
        self.count = count

    def __eq__(self, other):
    #    print "inside __eq__"
        if isinstance(other, int) and other == hash(self):
            # print(type(other))
            return True
        elif isinstance(other, int):
            return False
        elif self.name.lower() == other.name.lower():
            return True
        else:
            # Using Jaro-Winkler distance measurement for equality!
            jaro_distance = jellyfish.jaro_distance(self.name.lower(), other.name.lower())
            if jaro_distance >= JARO_DISTANCE_GENERAL_THRESHOLD:
                return True
            else:
                return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return mmh3.hash(self.name, signed=False)

    def __str__(self):
        return self.name

    def safe_str(self, obj):
        """ return the byte string representation of obj """
        try:
            return str(obj)
        except UnicodeEncodeError:
            # obj is unicode
            return str(obj).encode('unicode_escape')

    def namef(self):
        return self.safe_str(self.name)

class Database(object):
    def __init__(self):
        self.authors = dict() # Authors are saved in the dictionary with the author.auid as the key
        self.articles = dict() # Articles are saved in the dictionary with their hash() as the key (i.e. that __hash__ method is used)
        # If an article has eid != None then it is fully scraped but if fully_scraped is False when eid != None then it was scraped afterwards!
        self.keywords = dict() # Keywords are saved in the dictionary with their hash as the key (same as above)
        self.affiliations = dict() # Affiliations are saved in the dictionary with the affiliation.id as the key
        self.delimiting_keywords = set([])
        self.articles_with_eid = []

    def read_keywords_from_file(self, filename):
        print("Reading delimiting keywords from file...")
        self.delimiting_keywords.clear()
        if not os.path.isfile(filename):
            print("Warning: keywords file '" + filename +"' not found!")
            return
        f = open(filename, 'r')
        for line in f:
            self.delimiting_keywords.add(line.lstrip().rstrip().lower())
        print(self.delimiting_keywords)

    def get_article(self, new_article):
        # If article had EID and already exists (i.e. it has the same hash) in database, then get it
        if new_article.eid is not None and new_article in self.articles:
            # Get the existing one and try to update its details, if missing
            old_article = self.articles.get(hash(new_article))
            return old_article
        # If the article does not exist in database with the same hash
        else:
            # Try to find an article with EID that is lexicographically simillar to this one
            a = self.get_article_based_on_description(new_article)
            # If found, return the found one!
            if a is not None:
                return a
            # Else return None so that a new article will be added
            else:
                return None

    def get_article_based_on_description(self, article):
            if article is not None:
            #  print("Checking if article with EID already exists in database.")
                if article.description().lower() != "anonymous, no title":
                    existing_records = [a for a in self.articles_with_eid if Database.articles_lexicographically_same(article, a)]
                    if existing_records:
                        if isinstance(existing_records[0], Article):
                            if existing_records[0] is not None:
            #                  print "Article with EID exists, matching the description: " + article.description().lower().encode('utf-8')
                                return existing_records[0]
            #  print("Article matching the description didn't already exist.")
            return None
    
    @staticmethod
    def articles_lexicographically_same(article_a, article_b):
        if article_b.description().lower() != "anonymous, no title":
            # First, a generic lexicographical check for both author's surname and article's title
            if jellyfish.jaro_distance(article_a.description().lower(), article_b.description().lower()) >= JARO_DISTANCE_GENERAL_THRESHOLD:
                return True
            # More relaxed lexicographical checks but only for articles with the same publication year!
            elif (article_a.date is not None and article_b.date is not None) and (article_a.date[:4] == article_b.date[:4]):
                if jellyfish.jaro_distance(article_a.description().lower(), article_b.description().lower()) >= JARO_DISTANCE_COMBINED_THRESHOLD:
                   return True
                elif jellyfish.jaro_distance(article_a.title.lower(), article_b.title.lower()) >= JARO_DISTANCE_TITLE_THRESHOLD and jellyfish.jaro_distance(article_a.first_author_surname.lower(), article_b.first_author_surname.lower()) == JARO_DISTANCE_MAX:
                    return True
                elif jellyfish.jaro_distance(article_a.title.lower(), article_b.title.lower()) == JARO_DISTANCE_MAX and jellyfish.jaro_distance(article_a.first_author_surname.lower(), article_b.first_author_surname.lower()) >= JARO_DISTANCE_NAME_THRESHOLD:
                    return True
                elif jellyfish.jaro_distance(article_a.title.lower(), article_b.title.lower()) == JARO_DISTANCE_MAX and article_a.first_author_surname == "Anonymous":
                    return True
            else:
                return False

    def add_article(self, article):
        if article.get_first_author_surname() == 'Anonymous' and article.title == 'No title' and article.eid is None:
            return None
        else:
            # Check if article already exists in database
            stored_article = self.get_article(article)
            if stored_article is None:
                self.articles[hash(article)] = article
                if article.eid is not None:
                    self.articles_with_eid.append(article)
            else:
                article = stored_article
            return article

    def add_author(self, new_author):
        if new_author.auid in self.authors:
            old_author = self.authors[new_author.auid]
            if old_author.surname is None:
                old_author.surname = new_author.surname
            if old_author.given_name is None:
                old_author.given_name = new_author.given_name
            if old_author.affiliation is None:
                old_author.affiliation = new_author.affiliation
            if not old_author.articles:
                old_author.articles = new_author.articles
        else:
            self.authors[new_author.auid] = new_author

    def add_keywords(self, keyword):
        if keyword in self.keywords: # (solved) PROBLEM with __eq__ getting an int happened here. Solved I think, see line 419. 
            old_keyword = self.keywords[hash(keyword)]
            old_keyword.count += 1
        else:
            self.keywords[hash(keyword)] = keyword

    def get_authors_articles(self, author_surname):
        articles = set([])
        for key, article in self.articles.items():
            if article.match_author(author_surname):
                 articles.add(article)
        return articles

    def update_author_article_records(self):
        print("Update author article records.")
        for key, author in self.authors.items():
            author.articles.clear()
        for key, article in self.articles.items():
            for author in article.authors:
                author.articles.add(article)
        print("Finished!")

    def ensure_authors_not_duplicated(self):
        print("Ensuring authors in database are not duplicated...")
        for key, article in self.articles.items():
            new_art_authors = []
            for art_author in article.authors:
                if art_author.auid is None:
                    print(art_author.surname + " in article.authors doesn't have an auid")
                else:
                    try:
                        if self.authors[art_author.auid]:
                            new_art_authors.append(self.authors[art_author.auid])
                    except KeyError:
                        pass
                       # print "art_author.surname has no auid in DB. Removing."
            if new_art_authors and len(article.authors) != len(new_art_authors) :
                print(("a(" + str(len(article.authors)) + ">" + str(len(new_art_authors)) + ")" ), end=' ')
                article.authors = new_art_authors
        print("Finished!")

    def ensure_references_are_in_database(self):
        print("Ensuring references are in the database...")
        for key, article in self.articles.items():
            new_references = set([])
            for reference in article.references:
                stored_article = self.articles.get(hash(reference))
                if stored_article is None:
                    print("\n\nERROR: Found a reference that was not in the database!")
                    print(reference.full_descriptive_string())
                else:
                    new_references.add(stored_article)
                    if stored_article != reference:
                        print("Replacing " + reference.full_descriptive_string() + " with " + stored_article.full_descriptive_string())
            article.references = new_references
        print("Finished!")

    def update_article_citation_records(self):
        print("Update article citation records.")
        for key, article in self.articles.items():
            for reference in article.references:
                reference.citations.add(article)
        print("Finished!")

    def same_author_and_title(self, article_a, article_b):
        if article_a.get_first_author_surname() != "Anonymous":
            if article_b.get_first_author_surname() != "Anonymous":
                if article_a.title != "":
                    if article_b.title != "":
                        if (article_a.get_first_author_surname().lower() == article_b.get_first_author_surname().lower()) and ((article_a.title.lower() in article_b.title.lower()) or (article_b.title.lower() in article_a.title.lower())) and article_a != article_b:
                            print("Found duplicate: " + article_a.descriptionf())
                            return True
                        else:
                            return False

    @staticmethod
    def check_for_circular_references(article, visited):
        for reference in article.references:
            if reference in visited:
                print("A circular reference was detected when checking the references of " + article.descriptionf())
                print(reference.descriptionf() + " refers to itself.")
            else:
                new_visited = visited
                new_visited.add(article)
                Database.check_for_circular_references(reference, new_visited)

    @staticmethod
    def merge_articles(article_a, article_b, verbose=True):
        if not verbose:
            sys.stdout = open(os.devnull, 'w') # Supress output if verbose is set to False
        print("Merging articles:", end=' ')
        print(article_a, end=' ')
        print("<with>", end=' ')
        print(article_b)
        if article_a.title == '' or article_a.title == 'No title':
            article_a.title = article_b.title
            print("Article A inherits Article B's title.")
        if article_a.eid == None or article_a.eid == "":
            article_a.eid = article_b.eid
            print("Article A inherits Article B's EID.")
        if article_a.date == None:
            article_a.date = article_b.date
            print("Article A inherits Article B's date.")
        if article_a.source == None or article_a.source == "":
            article_a.source = article_b.source
            print("Article A inherits Article B's source", end=' ')
            if article_b.source and article_b.source.name:
                print(article_b.source.namef())
        if article_a.first_author_surname == None or (article_a.first_author_surname == 'Anonymous' and article_b.first_author_surname != 'Anonymous'):
            article_a.first_author_surname = article_b.first_author_surname
            print("Article A inherits Article B's first author surname " + article_b.first_author_surnamef())
        for author in article_b.authors:
            if author not in article_a.authors:
                article_a.authors.append(author)
                if article_b in author.articles:
                    author.articles.remove(article_b)
                author.articles.add(article_a)
                print("Article A inherits Article B's author " + author.surnamef())
        for keyword in article_b.keywords:
            if keyword not in article_a.keywords:
                article_a.keywords.add(keyword)
                print("Article A inherits Article B's keyword " + keyword.namef())
        print("Merging references:")
        for reference in article_b.references:
            if reference not in article_a.references and reference != article_a:
                article_a.references.add(reference)
                Database.check_for_circular_references(article_a, set())
                if article_b in reference.citations:
                    reference.citations.remove(article_b)
                reference.citations.add(article_a)
                print("Article A inherits Article B's references " + reference.descriptionf())
        for citation in article_b.citations:
            if citation not in article_a.citations and citation != article_a:
                article_a.citations.add(citation)
                if article_b in citation.references:
                    citation.references.remove(article_b)
                citation.references.add(article_a)
                print("Article A inherits Article B's citations " + citation.descriptionf())
        if article_b.references_are_updated:
            article_a.references_are_updated = True
        if not article_b.not_in_scopus:
            article_a.not_in_scopus = False
        if article_b.out_of_scope:
            article_a.out_of_scope = True
        if article_b.fully_scraped:
            article_a.fully_scraped = True
        if not verbose:
            sys.stdout = sys.__stdout__ # Restore output

    def simhash_articles(self):
        simhashes = []
        for key, article in list(self.articles.items()):
            # simhash = fingerprint(map(hash, article.description()))
            simhash = str(mmh3.hash(article.description(), signed=False), 'utf8')
            simhashes.append(simhash)
        return simhashes

    # This is a well designed method I believe
    def identify_description_duplicates(self):
        print("Checking for duplicate entries in the database.")
        to_be_deleted = []
        orig_n_articles = len(self.articles)
        i_article = 0
        start_time = time.time()
        for key_a, article_a in self.articles.items():
            i_article += 1
            avg_time = (time.time() - start_time)/i_article
            remaining_time = avg_time * (orig_n_articles - i_article)/3600
            print("Article " + str(i_article) + " of " + str(orig_n_articles) + ". Remaining time estimated to %.2f hours" % remaining_time)
            for key_b, article_b in self.articles.items():
                if self.same_author_and_title(article_a, article_b):
                    self.merge_articles(article_a, article_b)
                    to_be_deleted.append(key_b)
        for key in to_be_deleted:
            if key in self.articles:
                print("Deleting article " + self.articles[key].descriptionf() + " because it was a duplicate.")
                if key not in self.articles:
                    print("MEGA ERROR: Key to delete not found!!!")
                del self.articles[key]
                # Delete it from Datastore also (not tested code)
                cloud_strg = CloudStorage(database=self, auxiliary_use=True)
                cloud_strg.delete_entity(ARTICLE_KIND, str(key))
        updated_n_articles = len(self.articles)
        print("The database originally contained " + str(orig_n_articles) + " articles.")
        print("The updated database contains " + str(updated_n_articles) + " articles.")
        print("Finished!")

    # This is the updated version of the is_in_scope method that checks if at least one of the in scope keywords exists in the article
    def is_in_scope(self, article):
        for kw in self.delimiting_keywords:
            if kw in article.keywords or kw in article.title.lower():
                return True
        return False

    def print_articles(self):
        for key, article in self.articles.items():
            print(article.full_descriptive_string())

    def print_keywords(self, truncation_threshold):
        print("\nKeywords:")
        truncated_keywords = {k:v for (k,v) in self.keywords.items() if v.count >= truncation_threshold}
        sorted_truncated_keywords = sorted(truncated_keywords.items(), key=lambda kv: kv[1].count, reverse=True)        
        for (key, keyword) in sorted_truncated_keywords:
            print(keyword.name + ": " + str(keyword.count))

class CloudStorage(object):
    def __init__(self, database=None, automated=True, auxiliary_use=False, datastore_default_kind=True, datastore_kind_suffix=None, start_year_filter=None, end_year_filter=None):
        global ARTICLE_KIND
        global AUTHOR_KIND
        global KEYWORD_KIND
        global AFFILIATION_KIND
        if DATABASE_READ_ONLY:
            print("WARNING: DATABASE_READ_ONLY is enabled!")
        if DATABASE_SMOKE_TEST:
            print("WARNING: DATABASE_SMOKE_TEST is enabled!")
        if not datastore_default_kind and datastore_kind_suffix is None: # Assume KTH analysis
            ARTICLE_KIND = 'Article-kth'
            AUTHOR_KIND = 'Author-kth'
            KEYWORD_KIND = 'Keyword-kth'
        elif not datastore_default_kind and isinstance(datastore_kind_suffix, str):
            ARTICLE_KIND = 'Article-' + datastore_kind_suffix
            AUTHOR_KIND = 'Author-' + datastore_kind_suffix
            KEYWORD_KIND = 'Keyword-' + datastore_kind_suffix
        self.articles_to_rescrape = set([])
        #self.filename = filename
        #self.changes_made = False
        self.auxiliary_use = auxiliary_use
        # Instantiates a client
        self.datastore_client = datastore.Client(project='security-atlas')
        if database is None:
            print("Downloading from cloud database...")
            self.database = Database()
            self.keystroke_list = []
            if not DATABASE_SMOKE_TEST:
                self.load(start_year_filter, end_year_filter)
            print("Done!")
        else:
            self.database = database
            self.keystroke_list = []

    def store(self, new_put_limit=MAX_PUT_LIST_SIZE, article_kind=None, author_kind=None, keyword_kind=None): 
        list_of_entities = []
        # First save the article entities
        print("Trying to save " + str(len(self.database.articles)) + " articles on cloud database...")
        for key, article in self.database.articles.items():
            if article_kind is not None: # Bypass the default entity kinds if needed
                key = self.datastore_client.key(article_kind, str(hash(article)))
            else:
                key = self.datastore_client.key(ARTICLE_KIND, str(hash(article)))
            article_entity = datastore.Entity(key, exclude_from_indexes=[])
            article_entity.update({
                'title': article.title,
                'eid': article.eid,
                'date': article.date,
                'source': article.get_source(),
                'first_author_surname': article.first_author_surname,
                'authors_array': article.get_authors(),
                'keywords_array': article.get_keywords(),
                'references_array': article.get_references(),
                # 'citations_array': article.get_citations(), # Removed it to save storage space and time. This might also solve the not everything is saved bug.
                'references_are_updated': article.references_are_updated,
                'not_in_scopus': article.not_in_scopus,
                'out_of_scope': article.out_of_scope,
                'fully_scraped': article.fully_scraped
            })
            list_of_entities.append(article_entity)
        self.store_list_entities(list_of_entities, new_put_limit)
        del list_of_entities[:]
        
        # Then save the author entities
        print("Trying to save " + str(len(self.database.authors)) + " authors on cloud database...")
        for key, author in self.database.authors.items():
            if (author.auid is not None):
                if author_kind is not None:
                    datastore_key = self.datastore_client.key(author_kind, str(author.auid))
                else:
                    datastore_key = self.datastore_client.key(AUTHOR_KIND, str(author.auid))
            else:
                print("FATAL ERROR: Author has not auid!!!")
                sys.exit(1)
            author_entity = datastore.Entity(datastore_key, exclude_from_indexes=[])
            author_entity.update({
                'auid': author.auid,
                'surname': author.surname,
                'given_name': author.given_name,
                'affiliation': author.get_affiliation_id()
            })
            list_of_entities.append(author_entity)
        self.store_list_entities(list_of_entities)
        del list_of_entities[:]

        # Next save the keyword entities
        print("Trying to save " + str(len(self.database.keywords)) + " keywords on cloud database...")
        for key, value in self.database.keywords.items():
            if keyword_kind is not None:
                datastore_key = self.datastore_client.key(keyword_kind, str(key))
            else:
                datastore_key = self.datastore_client.key(KEYWORD_KIND, str(key))
            keyword_entity = datastore.Entity(datastore_key, exclude_from_indexes=[])
            keyword_entity.update({
                'name': value.name,
                'count': value.count
            })
            list_of_entities.append(keyword_entity)
        
        self.store_list_entities(list_of_entities)
        del list_of_entities[:]

        print("Saved on cloud database (" + str(len(self.database.articles)) + " articles).")
        print("Saved on cloud database (" + str(len(self.database.authors)) + " authors).")
        print("Saved on cloud database (" + str(len(self.database.keywords)) + " keywords).")
        print("Number of completely captured articles is " + str(len([a for a in self.database.articles.values() if a.is_complete()])))
        print("Number of fully scraped articles (with eid) is " + str(len(self.database.articles_with_eid)))

    def affiliation_store(self, affiliation_dict):
        list_of_entities = []
        for key, value in affiliation_dict.items():
            if key is not None:
                datastore_key = self.datastore_client.key(AFFILIATION_KIND, key)
                affiliation_entity = datastore.Entity(datastore_key, exclude_from_indexes=[])
                if value is not None:
                    affiliation_entity.update({
                        'name': value.name,
                        'country': value.country
                    })
                list_of_entities.append(affiliation_entity)
        
        # Now put all the entities to the datastore
        self.store_list_entities(list_of_entities)
        print("Saved on cloud database (" + str(len(list_of_entities)) + " affiliations).")
        del list_of_entities[:]

    def store_list_entities(self, list_to_store, new_put_limit=MAX_PUT_LIST_SIZE):
        if new_put_limit != MAX_PUT_LIST_SIZE:
            print("Setting new put limit: " + str(new_put_limit))
            ACTIVE_PUT_LIMIT = new_put_limit
        else:
            ACTIVE_PUT_LIMIT = MAX_PUT_LIST_SIZE
        if not DATABASE_SMOKE_TEST and not DATABASE_READ_ONLY:
            print("Storing...")
            start_time = time.time()
            # Put all the entities to the datastore
            list_length = len(list_to_store)
            put_cnt = 0
            if (list_length > ACTIVE_PUT_LIMIT):
                print("Entities to put more than the limit per query, splitting...")
                while (put_cnt < list_length):
                    put_until = put_cnt+ACTIVE_PUT_LIMIT
                    if (put_until > list_length):
                        put_until = list_length
                    self.datastore_client.put_multi(list_to_store[put_cnt:put_until])
                    put_cnt = put_until
            else:
                self.datastore_client.put_multi(list_to_store)
            end_time = time.time()
            print("Time to store to Datastore: " + str(end_time - start_time))
            return True
        else:
            print("Skip actual storing...")

    def delete_entity(self, entity_type, key_to_del):
        key = self.datastore_client.key(entity_type, key_to_del)
        self.datastore_client.delete(key)
            
    def load(self, start_year_filter=None, end_year_filter=None):  
        start_time = time.time()
        # Loading all affiliations from cloud datastore
        self.fetch_affiliations()
        # Load authors keywords and articles in parallel!
        thread_list = []
        # Loading all the authors from cloud datastore
        t = Thread(target=self.fetch_authors)
        thread_list.append(t)
        t.start()
        # Loading all the keywords from cloud datastore
        t = Thread(target=self.fetch_keywords)
        thread_list.append(t)
        t.start()
        
        result_list = []
        # queries = [('min', 'B') ,('B', 'G'), ('G', 'L'), ('L', 'P'), ('P', 'T'), ('T', 'max')] # This achieved aprox. 200 secs locally with 178074 entries
        queries = [('min', 'C') ,('C', 'H'), ('H', 'L'), ('L', 'P'), ('P', 'U'), ('U', 'max')] # This achieved aprox. 133 (90) secs locally with 178074 entries (and 86 (63) on GCP)
        # queries = [('min', 'B'), ('B', 'C'), ('C', 'F'), ('F', 'H'), ('H', 'L'), ('L', 'N'), ('N', 'Q'), ('Q', 'U'), ('U', 'max')] # This achieved aprox. 152 (99) secs locally with 178074 entries
        # queries = [('min', 'Ao'), ('Ao', 'C'), ('C', 'F'), ('F', 'H'), ('H', 'L'), ('L', 'N'), ('N', 'R'), ('R', 'U'), ('U', 'max')] # This achieved aprox. 170 (98) secs locally with 178074 entries (and 89 on GCP)
        for query in queries:
            t = Thread(target=self.partially_fetch_articles, args=(query[0], query[1], result_list))
            thread_list.append(t)
            t.start()
        # Wait for all threads to finish in order to continue
        for t in thread_list:
            t.join()
        fetch_end_time = time.time()
        print("Time to fetch affiliations, authors, keywords and articles from Datastore: " + str(fetch_end_time - start_time))
        # print("DONE! Return list size is=" + str(len(result_list)))

        # Now put the articles in the local database
        total_fetched_articles = 0

        if start_year_filter is not None and end_year_filter is not None:
            print("INFO: Filtering out all fully scraped articles with dates outside range: " + str(start_year_filter) + " - " + str(end_year_filter))

        for fetched_articles in result_list:
            total_fetched_articles += len(fetched_articles)
            print("Loading " + str(len(fetched_articles)) + " fetched articles...")
            for article in fetched_articles:
                f_title = article['title']
                f_eid = article['eid']
                f_date = article['date']
                if article['source'] == None:
                    f_source = article['source']
                else:
                    f_source = Source(article['source'])
                f_first_author_surname = article['first_author_surname']
                authors_array = article['authors_array']
                f_authors = []
                if authors_array is not None:
                    for author in authors_array:
                        # If the below is true, then skip adding authors for this article on the database so that it will not be included in the analysis results!
                        if start_year_filter is not None and end_year_filter is not None:
                            if f_date is not None and len(f_date) > 4 and (f_date < str(start_year_filter) or f_date > str(end_year_filter)):
                                break
                        auth = self.database.authors.get(author)
                        if isinstance(auth, int): # Debug check that can be removed in final version
                            print("MEGA-ERROR: author is integer!!!")
                        elif auth is None:
                            print("Author '" + author + "' was not found for article '" + f_eid + "' and returned None.")
                            self.articles_to_rescrape.add(Article(f_eid))
                        else:
                            f_authors.append(auth)
                keywords_array = article['keywords_array']
                f_keywords = set([])
                if keywords_array is not None:
                    for keyword in keywords_array:
                        kwrd_to_search = Keyword(keyword)
                        kwrd = self.database.keywords.get(hash(kwrd_to_search))
                        if kwrd is not None:
                            f_keywords.add(kwrd)
                        else:
                            print("Keyword '" + keyword + "' was not found for article '" + f_eid + "' and returned None.")
                            self.articles_to_rescrape.add(Article(f_eid))
                references_array = article['references_array']
                # citations_array = article['citations_array']
                f_references_are_updated = article['references_are_updated']
                f_not_in_scopus = article['not_in_scopus']
                f_out_of_scope = article['out_of_scope']
                f_fully_scraped = article['fully_scraped']
                # new_article = Article(eid=f_eid, title=f_title, date=f_date, source=f_source, fsurname=f_first_author_surname, authors=f_authors, keywords=f_keywords, references_array=references_array, citations_array=citations_array, refs_updated=f_references_are_updated, not_in_scopus=f_not_in_scopus, out_of_scope=f_out_of_scope, fully_scraped=f_fully_scraped)
                new_article = Article(eid=f_eid, title=f_title, date=f_date, source=f_source, fsurname=f_first_author_surname, authors=f_authors, keywords=f_keywords, references_array=references_array, refs_updated=f_references_are_updated, not_in_scopus=f_not_in_scopus, out_of_scope=f_out_of_scope, fully_scraped=f_fully_scraped)
                self.database.articles[hash(new_article)] = new_article
        print("Done adding on DB " + str(total_fetched_articles) + " articles!")
        if self.auxiliary_use:
            if len(result_list) == 0:
                print("Warning: Database has no article entries!")
        del result_list[:] # Clears the list used for parallel fetching

        # Now complete the articles with refereces and citations and get the articles_with_eid
        for key, article in self.database.articles.items():
            if article.references_array is not None:
                f_references = set([])
                for reference in article.references_array:
                    ref = self.database.articles.get(int(reference))
                    if ref is not None:
                        f_references.add(ref) # (solved) PROBLEM: reference from array not found. This is because of duplicate article removal! If load and save is run again, it dissapears!
                    else:
                        print("Reference '" + reference + "' was not found for article '" + str(hash(article)) + "' and returned None.")
                article.references = f_references
            # if article.citations_array is not None:
            #     f_citations = set([])
            #     for citation in article.citations_array:
            #         cit = self.database.articles.get(int(citation))
            #         if cit is not None:
            #             f_citations.add(cit)
            #         else:
            #             print("Citation '" + citation + "' was not found for article '" + str(hash(article)) + "' and returned None.")
            #     article.citations = f_citations
            if (article.eid is not None):
                self.database.articles_with_eid.append(article)
        print("Number of completely captured articles is " + str(len([a for a in self.database.articles.values() if a.is_complete()])))
        print("Number of fully scraped articles (with eid) is " + str(len(self.database.articles_with_eid)))

        end_time = time.time()
        print("Time to completely load articles, authors, keywords and affiliations from Datastore: " + str(end_time - start_time))

    def fetch_affiliations(self):
        print("Now fetching affiliations...")
        affiliations_query = self.datastore_client.query(kind=AFFILIATION_KIND)
        fetched_affiliations = list(affiliations_query.fetch())
        for aff in fetched_affiliations:
            f_id = aff.key.name
            f_name = aff['name']
            try:
                f_country = aff['country']
            except KeyError: # This is also a workaround for the intermiadate state
                f_country = None
            aff_obj = Affiliation(f_id, f_name, f_country)
            self.database.affiliations[f_id] = aff_obj
        if self.auxiliary_use:
            if len(fetched_affiliations) == 0:
                print("Warning: Database has no affiliation entries!")
        del fetched_affiliations[:]

    def fetch_authors(self):
        print("Now fetching authors...")
        auhtors_query = self.datastore_client.query(kind=AUTHOR_KIND)
        fetched_authors = list(auhtors_query.fetch())
        for author in fetched_authors:
            f_surname = author['surname']
            f_auid = author['auid']
            f_given_name = author['given_name']
            try: # This is to retain the functionality of Scraper.list_affiliation_ids(), now moved under load_affiliation_dict()
                if author['affiliation'] is not None:
                    affiliation_obj = self.database.affiliations[author['affiliation']]
                else: # If author has no affiliation, it should be none.
                    affiliation_obj = Affiliation(None)
            except KeyError:
                print("Affiliation '" + str(author['affiliation']) + "' not yet in database. Have you scraped for affiliations?")
                affiliation_obj = Affiliation(author['affiliation'])
            new_author = Author(auid=f_auid, surname=f_surname, given_name=f_given_name, affiliation=affiliation_obj)
            self.database.authors[f_auid] = new_author
        if self.auxiliary_use:
            if len(fetched_authors) == 0:
                print("Warning: Database has no author entries!")
        del fetched_authors[:]
    
    def fetch_keywords(self):
        print("Now fetching keywords...")
        keywords_query = self.datastore_client.query(kind=KEYWORD_KIND)
        fetched_keywords = list(keywords_query.fetch())
        for keyword in fetched_keywords:
            try:
                f_name = str(keyword['name'], 'utf-8')
            except TypeError:
                f_name = keyword['name']
            f_count = keyword['count']
            keyword_obj = Keyword(name=f_name, count=f_count)
            self.database.keywords[hash(keyword_obj)] = keyword_obj
        if self.auxiliary_use:
            if len(fetched_keywords) == 0:
                print("Warning: Database has no keyword entries!")
        del fetched_keywords[:]

    def partially_fetch_articles(self, start_filter, end_filter, output):
        print("Now fetching articles with author surname from " + start_filter  + " to " + end_filter + "...")
        client = datastore.Client()
        articles_query = client.query(kind=ARTICLE_KIND)
        if start_filter != 'min':
            articles_query.add_filter('first_author_surname', '>', start_filter)
        else:
            start_filter = '(start)'
        if end_filter != 'max':
            articles_query.add_filter('first_author_surname', '<=', end_filter)
        else:
            end_filter = '(end)'
        fetched_articles = list(articles_query.fetch())
        print("In range " + start_filter  + " to " + end_filter + " fetched " + str(len(fetched_articles)) + " articles.")
        output.append(fetched_articles)

    def get_articles_to_rescrape(self):
        return self.articles_to_rescrape
        