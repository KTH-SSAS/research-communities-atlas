from datetime import datetime
import community as cmty
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
from storage import CloudStorage, Keyword, Database
import choropleth_plotter as choro_plot
import plotly_graph_plotter as graph_plot
import operator
import sys, os
import numpy
import math
import time
import json
import csv
import random

# Defines for Analysis
CITATION_TRUNCATION_THRESHOLD = 50
KEYWORD_TRUNCATION_THRESHOLD = 50
COMMUNITY_ARTICLES_TO_PRINT = 250
COMMUNITY_KEYWORDS_TO_PRINT = 20
COMMUNITY_SOURCES_TO_PRINT = 15

class Community:

    def __init__(self, database, auids, partition_id, excluded_keywords, affiliation_dict, community_names_so_far=[]):
        self.database = database
        self.authors = set([])
        self.partition_id = partition_id
        # Because the main scraper doesn't find the afiliation names, but only the affiliation ids, we produce an id-name-dictionary elsewhere (in the auxilliary_scraper.py).
        self.affiliation_dict = affiliation_dict # Removed hard coded dictionary form here!

        for auid in auids:
            auth = database.authors.get(str(auid))
            if auth is None:
                print("Author with auid='" + str(auid) + "' was not found for community '" + str(partition_id) + "' and returned None.")
            else:
                self.authors.add(auth)
        # This below line was the old and complex way of determining the community name
        # pruned_keywords = [kw[0] for kw in self.most_influential_keywords(10) if kw[0] not in excluded_keywords and kw[0] not in community_names_so_far]
        found_name = None
        name_found = False
        for kw in self.most_influential_keywords(10):
            name_found = False
            if community_names_so_far:
                for community_name in community_names_so_far:
                    # if kw[0] not in excluded_keywords and kw[0].split('(')[0] not in community_name:
                    if kw[0] not in excluded_keywords and ((" (" in kw[0] and kw[0].split(' (')[0] not in community_name) or (kw[0] not in community_name)):
                        name_found = True
                    else:
                        name_found = False
                        break
            else:
                found_name = kw[0]
                break
            if name_found:
                found_name = kw[0]
                break
        
        if found_name:
            self.name = found_name
        # if pruned_keywords:
        #     self.name = pruned_keywords[0]
        else:
            self.name = 'anonymous'

    def auids(self):
        return [author.auid for author in self.authors]

    def articles(self):
        articles = set([])
        for author in self.authors:
            for article in author.articles:
                articles.add(article)
        return articles

    def keywords(self, length):
        community_keywords = dict()
        for author in self.authors:
            author_keywords = author.keywords()
            for keyword, frequency in author_keywords.items():
                if keyword.name in community_keywords:
                    community_keywords[keyword.name] += frequency
                else:
                    community_keywords[keyword.name] = frequency
        sorted_truncated_keywords = sorted(list(community_keywords.items()), key=operator.itemgetter(1), reverse=True)[0:length]
        return sorted_truncated_keywords

    def just_keywords(self, length):
        community_keywords = dict()
        for author in self.authors:
            author_keywords = author.keywords()
            for keyword, frequency in author_keywords.items():
                if keyword.name in community_keywords:
                    community_keywords[keyword.name] += frequency
                else:
                    community_keywords[keyword.name] = frequency
        sorted_truncated_keywords = [key for key,value in sorted(list(community_keywords.items()), key=operator.itemgetter(1), reverse=True)][0:length]
        return sorted_truncated_keywords

    def sources(self, length):
        community_sources = dict()
        for author in self.authors:
            author_sources = author.sources()
            for source, frequency in author_sources.items():
                if source != None:
                    if source.name in community_sources:
                        community_sources[source.name] += frequency
                    else:
                        community_sources[source.name] = frequency
        sorted_truncated_sources = sorted(list(community_sources.items()), key=operator.itemgetter(1), reverse=True)[0:length]
        return sorted_truncated_sources

    # Implemented by me (Sotirios) but not used due to high computational complexity! It is also not needed.
    def most_by_community_cited_authors(self):
        citations = dict()
        articles = set([])
        for author in self.authors:
            for article in author.articles:
                articles.add(article)
        for article in articles:
            for reference in article.references:
                for author in reference.get_authors():
                    if author.full_name() not in citations:
                        citations[author.full_name()] += 1
                    else:
                        citations[author.full_name()] = 1
        sorted_cited_authors = sorted(list(citations.items()), key=operator.itemgetter(1), reverse=True)
        return sorted_cited_authors

    # This method returns the most cited 'everything' (i.e. articles, authors and yearly citations) by this community as well as the most by community cited articles and the intra-community cited articles.
    def most_cited_x_in_community(self, main_analysis=False):
        CITATION_CONSIDER_PRODUCED_BY_THRESHOLD = 1 # Only articles cited more than this number will be considered when intersecting cited by community and produced by community articles below.
        tmp_community_articles = set([])
        articles = dict()
        authors = dict()
        yearly_citations = dict()
        citations = dict()
        intra_citations = dict()
        for author in self.authors:
            for article in author.articles:
                tmp_community_articles.add(article) # Saving all community author articles in a set for later use
                # First articles produced by the community
                if article.description_with_year() not in articles:
                    articles[article.description_with_year()] = len(article.citations)
                # Then authors belonging to community
                if author.full_name() not in authors:
                    authors[author.full_name()] = len(article.citations)
                    if main_analysis: # If main analysis is pefromed at this call, update the citation count on community.authors
                        author.citation_cnt = len(article.citations)
                else:
                    authors[author.full_name()] += len(article.citations)
                    if main_analysis:
                        author.citation_cnt += len(article.citations)
                # Finally, yearly community citations
                if int(article.date[:4]) not in yearly_citations:
                    yearly_citations[int(article.date[:4])] = len(article.citations)
                else:
                    yearly_citations[int(article.date[:4])] += len(article.citations)
        # Now get the articles cited by the community
        for article in tmp_community_articles:
            for reference in article.references:
                # This is for the global citations (all cited articles)
                if reference.description_with_year() in citations:
                    citations[reference.description_with_year()] += 1
                else:
                    citations[reference.description_with_year()] = 1
                # And this is for the intra-community citations (community articles that are cited by the community)
                if reference.description_with_year() in articles:
                    if reference.description_with_year() in intra_citations:
                        intra_citations[reference.description_with_year()] += 1
                    else:
                        intra_citations[reference.description_with_year()] = 1
        # Update the articles produced by the community with the intersection with the ones cited by the community to only get those truly produced by the community
        # i.e. Articles produced by community: Articles produced by the community that are also (the most) cited by the community (with more than one citations).
        list_of_cited_articles = [k for (k,v) in citations.items() if v > CITATION_CONSIDER_PRODUCED_BY_THRESHOLD]
        keys = set(list_of_cited_articles).intersection(set(articles.keys()))
        result = {k:articles[k] for k in keys}
        articles = result # Updated and integrated list of articles produced by the community
        # Finally sort them to create the lists
        citation_sorted_community_articles = sorted(list(articles.items()), key=operator.itemgetter(1), reverse=True)
        # Only get authors with citations > 0
        citation_sorted_authors = sorted([items for items in authors.items() if items[1] > 0], key=operator.itemgetter(1), reverse=True)
        # citation_sorted_authors = sorted(list(authors.items()), key=operator.itemgetter(1), reverse=True)
        citation_sorted_years = sorted(list(yearly_citations.items()), key=operator.itemgetter(1), reverse=True)
        sorted_cited_articles = sorted(list(citations.items()), key=operator.itemgetter(1), reverse=True)
        sorted_intra_community_cited_articles = sorted(list(intra_citations.items()), key=operator.itemgetter(1), reverse=True)
        return [citation_sorted_community_articles, citation_sorted_authors, citation_sorted_years, sorted_cited_articles, sorted_intra_community_cited_articles]
    
    def most_cited_x_in_community_by_id(self):
        articles = dict()
        authors = dict()
        yearly_citations = dict()
        for author in self.authors:
            for article in author.articles:
                # First articles
                if article.eid not in articles:
                    articles[article.eid] = len(article.citations)
                else:
                    articles[article.eid] += len(article.citations)
                # Then authors
                if author.auid not in authors:
                    authors[author.auid] = len(article.citations)
                else:
                    authors[author.auid] += len(article.citations)
                # Finally, yearly citations
                if article.date[:4] not in yearly_citations:
                    yearly_citations[article.date[:4]] = len(article.citations)
                else:
                    yearly_citations[article.date[:4]] += len(article.citations)
        citation_sorted_articles = [key for key,value in sorted(list(articles.items()), key=operator.itemgetter(1), reverse=True)]
        citation_sorted_authors = [key for key,value in sorted(list(authors.items()), key=operator.itemgetter(1), reverse=True)]
        citation_sorted_years = [key for key,value in sorted(list(yearly_citations.items()), key=operator.itemgetter(1), reverse=True)]
        return [citation_sorted_articles, citation_sorted_authors, citation_sorted_years]

    def median_article_publication_date(self):
        publication_dates = []
        for author in self.authors:
            for article in author.articles:
                publication_dates.append(article.date)
        publication_dates.sort()
        publication_dates = [d for d in publication_dates if d]
        if publication_dates:
            return (publication_dates[0], publication_dates[int(len(publication_dates) / 2)], publication_dates[-1])
        else:
            return [None, None, None]

    def annual_article_count(self):
        article_count = dict()
        for article in self.articles():
            try:
                year = int(article.date[0:4])
                if year in article_count:
                    article_count[year] += 1
                else:
                    article_count[year] = 1
            except Exception as e:
                print(e)
                raise
        return sorted(iter(article_count.items()), key=operator.itemgetter(0))

    # This method returns the most 'everything' (i.e. common, influential and by country) affiliations of this community.
    def most_x_affiliations_in_community(self):
        # Construct two dictionaries containing the number of authors and author citations for each affiliation id.
        appearance_affiliation_dict = dict()
        influence_affiliation_dict = dict()
        for author in self.authors:
            authors_citations = 0
            if author.affiliation is not None:
                authors_citations = author.citation_cnt # For this line to work it is assumed that the "most_cited_x_in_community" method is ran before this method.
                if author.affiliation.id in influence_affiliation_dict:
                    influence_affiliation_dict[author.affiliation.id] += authors_citations
                    appearance_affiliation_dict[author.affiliation.id] += 1
                else:
                    influence_affiliation_dict[author.affiliation.id] = authors_citations
                    appearance_affiliation_dict[author.affiliation.id] = 1
        # We also need another dictionary that will hold the articles produced by each affiliation id.
        affiliation_articles_dict = dict()
        articles_so_far = []
        for author in self.authors:
            for article in author.articles:
                if article.eid in articles_so_far:
                    continue
                else:
                    articles_so_far.append(article.eid)
                    article_aff_ids = []
                    for author in article.authors:
                        if author.affiliation.id not in article_aff_ids:
                            article_aff_ids.append(author.affiliation.id)
                    for aff_id in article_aff_ids:
                        if aff_id in affiliation_articles_dict:
                            affiliation_articles_dict[aff_id] += 1
                        else:
                            affiliation_articles_dict[aff_id] = 1
        # First get the most influentials affiliations (and countries)
        sorted_most_influential_affiliation_ids = sorted(iter(influence_affiliation_dict.items()), key=operator.itemgetter(1), reverse=True)
        sorted_most_influential_affiliation_names = []
        most_influential_affiliation_countries = dict()
        for (idnum, freq) in sorted_most_influential_affiliation_ids:
            try:
                affiliation_name = self.affiliation_dict[idnum].name
            except KeyError as e:
                affiliation_name = "Unknown Affiliation"
            sorted_most_influential_affiliation_names.append((affiliation_name, freq))
            # Now get the affiliation country
            try:
                affiliation_country = self.affiliation_dict[idnum].country
            except KeyError as e:
                affiliation_country = "Unknown Country"
            if affiliation_country in most_influential_affiliation_countries:
                most_influential_affiliation_countries[affiliation_country] += freq
            else:
                most_influential_affiliation_countries[affiliation_country] = freq
        sorted_most_influential_affiliation_countries = sorted(iter(most_influential_affiliation_countries.items()), key=operator.itemgetter(1), reverse=True)
        # Then get the most appeared (based on authors) affiliations (and coutries)
        sorted_affiliation_ids = sorted(iter(appearance_affiliation_dict.items()), key=operator.itemgetter(1), reverse=True)
        sorted_affiliation_names = []
        affiliation_countries = dict()
        for (idnum, freq) in sorted_affiliation_ids:
            try:
                affiliation_name = self.affiliation_dict[idnum].name
            except KeyError as e:
                affiliation_name = "Unknown Affiliation"
            sorted_affiliation_names.append((affiliation_name, freq))
            # Now get the affiliation country
            try:
                affiliation_country = self.affiliation_dict[idnum].country
            except KeyError as e:
                affiliation_country = "Unknown Country"
            if affiliation_country in affiliation_countries:
                affiliation_countries[affiliation_country] += freq
            else:
                affiliation_countries[affiliation_country] = freq
        sorted_affiliation_countries = sorted(iter(affiliation_countries.items()), key=operator.itemgetter(1), reverse=True)
        # Finally get the nubmer of articles produced by each affiliation (and coutries)
        article_sorted_affiliation_ids = sorted(iter(affiliation_articles_dict.items()), key=operator.itemgetter(1), reverse=True)
        article_sorted_affiliation_names = []
        article_affiliation_countries = dict()
        for (idnum, freq) in article_sorted_affiliation_ids:
            try:
                affiliation_name = self.affiliation_dict[idnum].name
            except KeyError as e:
                affiliation_name = "Unknown Affiliation"
            article_sorted_affiliation_names.append((affiliation_name, freq))
            # Now get the affiliation country
            try:
                affiliation_country = self.affiliation_dict[idnum].country
            except KeyError as e:
                affiliation_country = "Unknown Country"
            if affiliation_country in article_affiliation_countries:
                article_affiliation_countries[affiliation_country] += freq
            else:
                article_affiliation_countries[affiliation_country] = freq
        article_sorted_affiliation_countries = sorted(iter(article_affiliation_countries.items()), key=operator.itemgetter(1), reverse=True)
        # And return everything
        return [article_sorted_affiliation_names, sorted_most_influential_affiliation_names, article_sorted_affiliation_countries, sorted_most_influential_affiliation_countries, sorted_affiliation_names, sorted_affiliation_countries]

    # This method should be used for the generation of better community names. It sorts keywords based on citations (i.e. how many articles cite the articles that have that keyword)
    def most_influential_keywords(self, length):
        keyword_dict = dict()
        for author in self.authors:
            for article in author.articles:
                for keyword in article.keywords:
                    if keyword.name in keyword_dict:
                        keyword_dict[keyword.name] += len(article.citations)
                    else:
                        keyword_dict[keyword.name] = len(article.citations)
        sorted_most_influential_keywords = sorted(iter(keyword_dict.items()), key=operator.itemgetter(1), reverse=True)[0:length]
        return sorted_most_influential_keywords

class Analyzer:
    def __init__(self, database=None, automated=True, start_year=1945, end_year=datetime.now().year, keyword="", randomize=False, cmt_rename_list_file="communities_rename_list.csv", excluded_communities_list_file="excluded_communities_list.csv", detailed_global_analysis=False, sub_com_analysis=False, sub2_com_analysis=False, community_size_threshold=75, sub_community_size_threshold_divider=4, modularity_threshold=None, export_graph_data=False):
        print("Initializing analyzer...")
        if os.path.isfile(cmt_rename_list_file):
            print("Reading cmt_rename_list_file...")
            try:
                self.community_rename_dict = pd.read_csv(cmt_rename_list_file, index_col=0).squeeze("columns").to_dict()
            except pd.errors.ParserError:
                print("ERROR: Parse error while reading from " + cmt_rename_list_file)
                sys.exit(1)
        else:
            print("WARNING On class Analyzer: '" + cmt_rename_list_file + "' file not found! community_rename_dict will be empty and no renames will be made!")
            self.community_rename_dict = {}

        if os.path.isfile(excluded_communities_list_file):
            print("Reading excluded_communities_list_file...")
            try:
                exclude_list = []
                with open(excluded_communities_list_file, newline='') as inputfile:
                    for row in csv.reader(inputfile):
                        exclude_list.append(row[0])
                    self.excluded_communities = exclude_list
                print("The following communities will be excluded from the analysis:")
                print(self.excluded_communities)
            except IOError:
                print("ERROR: Parse error while reading from " + excluded_communities_list_file)
                sys.exit(1)
        else:
            print("WARNING On class Analyzer: '" + excluded_communities_list_file + "' file not found! excluded_communities will be empty and no exclusions will be made!")
            self.excluded_communities = []
        
        if randomize is not None:
            self.randomize = randomize
        else:
            self.randomize = False
        if sub_com_analysis is not None:
            self.sub_com_analysis = sub_com_analysis
        else:
            self.sub_com_analysis = False
        if sub2_com_analysis is not None:
            self.sub2_com_analysis = sub2_com_analysis
        else:
            self.sub2_com_analysis = False
        self.start_year = start_year
        self.end_year = end_year
        self.community_size_threshold = community_size_threshold
        self.sub_community_size_threshold_divider = sub_community_size_threshold_divider
        self.modularity_threshold_fullfiled = True # Default is true so that it only changes value when 'modularity_threshold' is set
        self.export_graph_data = export_graph_data
        self.start_time = time.time()
        self.storage = self.initialize_storage(database=database, automated=automated)
        self.affiliation_dict = self.storage.database.affiliations
        self.print_statistics(citation_threshold=CITATION_TRUNCATION_THRESHOLD, detailed_global_analysis=detailed_global_analysis)
        self.excluded_keywords = ('cyber security', 'cyber-attacks', 'security breaches', 'security', 'information security', 
                                  'cybersecurity', 'computer security', 'cyber threats', 'network security', 'intrusion detection systems')
        self.colors = ['Gold', 'DarkTurquoise', 'Orange', 'PaleTurquoise', 'LightSalmon', 'Crimson', 'LimeGreen',
                       'MediumOrchid', 'Violet', 'GreenYellow', 'Red', 'DodgerBlue', 'Pink',
                       'Teal', 'Gold', 'GoldenRod', 'OliveDrab', 'Magenta', 'Chocolate', 'Aquamarine',
                       'SlateBlue', 'BlueViolet', 'PaleGreen', 'Violet', 'SteelBlue', 'Cyan', 'Tan',
                       'Khaki', 'RosyBrown', 'Beige', 'ForestGreen', 'RoyalBlue', 'DeepPink', 'SkyBlue']
        # self.colors = ["DeepSkyBlue", "LightSkyBlue", "LightSteelBlue", "LightBlue", "PaleTurquoise", "DarkTurquoise",
        #                "cyan", "LightCyan", "CadetBlue", "MediumAquamarine",
        #                "aquamarine", "DarkSeaGreen", "SeaGreen", "MediumSeaGreen",
        #                "LightSeaGreen", "PaleGreen", "SpringGreen", "LawnGreen", "green", "chartreuse",
        #                "MediumSpringGreen", "GreenYellow", "LimeGreen", "YellowGreen", "OliveDrab",
        #                "DarkKhaki", "khaki", "PaleGoldenrod", "LightGoldenrodYellow", "LightYellow", "yellow", "gold",
        #                "goldenrod", "DarkGoldenrod", "RosyBrown", "IndianRed",
        #                "sienna", "peru", "burlywood", "beige", "wheat", "SandyBrown", "tan", "chocolate",
        #                "DarkSalmon", "salmon", "LightSalmon", "DarkOrange", "coral", "LightCoral",
        #                "tomato", "OrangeRed", "red", "HotPink", "DeepPink", "pink", "LightPink", "PaleVioletRed",
        #                "MediumVioletRed", "magenta", "violet", "plum", "orchid", "MediumOrchid"]
        # random.Random(1805).shuffle(self.colors) # Shuffle the colors to use them in a random order but in a deterministic way (i.e. using a fixed random seed)
        self.author_graph = self.initialize_author_graph(start_year=start_year, end_year=end_year, keyword=keyword)
        print("Partitioning...")
        self.partition, self.communities = self.create_partition(self.author_graph, self.storage.database, self.excluded_keywords, self.randomize, modularity_threshold)
        print("Main initialization completed in " + str(time.time() - self.start_time) + " seconds.")
        if sub_com_analysis:
            print("INFO: Now starting sub-community analysis...")
            self.analyze_sub_communities(self.communities)
            print("Initialization of sub-graphs is complete in " + str(time.time() - self.start_time) + " seconds.")
        if sub2_com_analysis:
            print("INFO: Now starting sub^2-community analysis...")
            self.analyze_sub2_communities(self.sub_communities)
            print("Initialization of sub^2-graphs is complete in " + str(time.time() - self.start_time) + " seconds.")

    def analyze_sub_communities(self, communities):
        if self.community_size_threshold is None:
            print("FatalError: self.community_size_threshold is not defined and sub community analysis is enabled!")
            sys.exit()
        self.sub_graphs = []
        self.sub_partitions = []
        self.sub_communities = []
        self.community_names = []
        for cmt in communities:
            if len(cmt.authors) >= self.community_size_threshold:
                sub_graph = self.initialize_sub_graph(cmt.auids())
                self.sub_graphs.append(sub_graph)
                print("Partitioning sub-graph for '" + str(cmt.name) + "' community...")
                partition, communities = self.create_partition(sub_graph, self.storage.database, self.excluded_keywords, self.randomize, main_partition=False, super_partition_name=cmt.name)
                self.sub_communities.append(communities)
                self.sub_partitions.append(partition)
                self.community_names.append(cmt.name)

    def analyze_sub2_communities(self, sub_communities, single_community_name="attack graph"):
        self.sub2_graphs = []
        self.sub2_partitions = []
        self.sub2_communities = []
        self.sub2_community_names = []
        for communities in sub_communities:
            for cmt in communities:
                if len(cmt.authors) >= self.community_size_threshold/self.sub_community_size_threshold_divider and ((single_community_name is not None and cmt.name == single_community_name) or single_community_name is None):
                    sub2_graph = self.initialize_sub_graph(cmt.auids())
                    self.sub2_graphs.append(sub2_graph)
                    print("Partitioning sub^2-graph for '" + str(cmt.name) + "' sub-community...")
                    partition, communities = self.create_partition(sub2_graph, self.storage.database, self.excluded_keywords, self.randomize, main_partition=False, super_partition_name=cmt.name)
                    self.sub2_communities.append(communities)
                    self.sub2_partitions.append(partition)
                    self.sub2_community_names.append(cmt.name)

    def create_partition(self, graph, database, excluded_keywords, randomize=False, modularity_threshold=None, main_partition=True, super_partition_name=None):
        partition_start_time = time.time()
        partition = self.partition_author_graph(graph, 'best', randomize)
        self.random_seed = randomize
        print("Randomize/random_seed=" + str(self.random_seed))
        if main_partition:
            self.modularity = cmty.modularity(partition, graph)
            if modularity_threshold is not None and self.modularity < modularity_threshold:
                print("Partition's modularity=" + str(self.modularity))
                print("WARNING: Modularity of this run is < modularity_threshold")
                self.modularity_threshold_fullfiled = False
                return None, None
        print("Partition's modularity=" + str(cmty.modularity(partition, graph)))
        print("Time to partition author graph was " + str(time.time() - partition_start_time) + " seconds.")
        print("Creating communities...")
        cc_start_time = time.time()
        communities = []
        community_names_so_far = []
        # If sub-community analysis is running add the super-community name in 'super_partition_name' to prevent sub-communities with same name
        if not main_partition and super_partition_name is not None:
            community_names_so_far.append(super_partition_name)
        for i_community in range(0, self.get_n_communities(partition)):
            auids = self.get_community_authors(partition, i_community)
            community = Community(database, auids, i_community, excluded_keywords, self.affiliation_dict, community_names_so_far)
            communities.append(community)
            # If the community is larger than the size threshold (i.e. it will be presented on the results) save its name in 'community_names_so_far')
            if (main_partition and len(community.authors) >= self.community_size_threshold) or (not main_partition and len(community.authors) >= self.community_size_threshold/self.sub_community_size_threshold_divider):
                community_names_so_far.append(community.name)
                # Now check if the community needs to be "manually" renamed:
                if main_partition:
                    new_name = self.community_rename_dict.get(community.name)
                    if new_name is not None:
                        print("Community name '" + community.name + "' renamed to '" + new_name + "'")
                        community_names_so_far.append(new_name)
                        community.name = new_name
                elif not main_partition and super_partition_name is not None:
                    new_name = self.community_rename_dict.get(super_partition_name + ':' + community.name)
                    if new_name is not None:
                        print("Community name '" + super_partition_name + ":" + community.name + "' renamed to '" + new_name + "'")
                        community_names_so_far.append(new_name)
                        community.name = new_name
        print("Time to create communities was " + str(time.time() - cc_start_time) + " seconds.")
        return partition, communities

    def initialize_storage(self, database=None, automated=True):
        print("Reading database... ")
        storage = CloudStorage(database, automated=automated)
        print("Update article records and citations...")
        # storage.database.ensure_references_are_in_database() # This is removed to save time! Please check once before (with menu option 4) running analysis for the first time
        # storage.database.ensure_authors_not_duplicated() # This is removed to save even more time!
        #storage.database.identify_description_duplicates() # This was always removed because it requires a ton of time to run! Run it from menu option 4. if needed.
        storage.database.update_author_article_records()
        storage.database.update_article_citation_records()
        print("Done.")
        return storage

    def initialize_author_graph(self, start_year=1945, end_year=datetime.now().year, keyword=""):
        # Temporary variables to study the possible Chinese (and not only) to/from rest of the world citation imbalance
        analyze_possible_citation_imbalance = False
        citations_to = 0
        citations_from = 0
        citations_internal = 0
        country_to_study = "China"
        # Add authors as nodes
        graph = nx.Graph()
        # graph = Graph()
        start_initialize_time = time.time()
        print("Adding authors to graph...")
        for key, author in self.storage.database.authors.items():
            published_in_time_interval_and_on_topic=False
            for article in author.articles:
                try:
                    article_year = int(article.date[0:4])
                except Exception:
                    print("Failed to find article date. Excluding it from selection.")
                    article_year = 0
                if article_year >= start_year and article_year <= end_year and (Keyword(keyword) in article.keywords or keyword == ""):
                    published_in_time_interval_and_on_topic = True
            if published_in_time_interval_and_on_topic:
                graph.add_node(int(author.auid))
        # Add citations as edges
        print("Adding edges to graph. " +  str(time.time() - start_initialize_time) + " seconds.")
        i_edge = 0
        i_author = 0
        print("iterating over " + str(len(graph.nodes())) + " authors.")
        for author_id_int in list(graph.nodes()):
            i_author += 1
            auid = str(author_id_int)
            cited_authors = self.storage.database.authors[auid].get_cited_authors()
            for cited_author in cited_authors:
                # Below check was added because when doing analysis of a shorter period of time edges to authors not found inside the specified time period should be discarded.
                if (cited_author.auid in self.storage.database.authors and end_year-start_year >= (datetime.now().year-1)-1945) or (cited_author.auid in self.storage.database.authors and int(cited_author.auid) in graph.nodes() and end_year-start_year < datetime.now().year-1945):
                    graph.add_edge(int(auid), int(cited_author.auid))
                    if analyze_possible_citation_imbalance:
                        citing_author = self.storage.database.authors[auid]
                        if cited_author.affiliation.country is not None and citing_author.affiliation.country is not None:
                            if cited_author.affiliation.country.lower() == country_to_study.lower() and citing_author.affiliation.country.lower() != country_to_study.lower():
                                citations_to += 1
                            elif citing_author.affiliation.country.lower() == country_to_study.lower() and cited_author.affiliation.country.lower() != country_to_study.lower():
                                citations_from += 1
                            elif cited_author.affiliation.country.lower() == country_to_study.lower() and citing_author.affiliation.country.lower() == country_to_study.lower():
                                citations_internal += 1
                    i_edge += 1
        #           print ".",
        #    print "Author " + str(i_author) + "/" + str(len(graph.nodes())) + " contributed to " + str(i_edge) + " edges."
        print(str(i_edge) + " edges. Time is " +  str(time.time() - start_initialize_time) + " seconds.")
        if analyze_possible_citation_imbalance:
            print("Presenting citation imbalance analysis results for " + country_to_study + ":")
            print("- citations towards " + country_to_study + " from the rest of the world: " + str(citations_to))
            print("- citations from " + country_to_study + " to the rest of the world: " + str(citations_from))
            print("- internal " + country_to_study + " citations: " + str(citations_internal))
        if self.export_graph_data:
            self.export_author_graph(graph)
        return graph

    def export_author_graph(self, graph):
        if graph:
            start_export_time = time.time()
            print("Info: Exporting author graph to files: 'author_graph_nodes.csv' and 'author_graph_edges.csv'")
            stdout_old = sys.stdout
            # First print the nodes
            sys.stdout = open("author_graph_nodes.csv", 'w')
            print("Id,Label")
            for node_id in graph.nodes:
                print(str(node_id) + ',"' + self.storage.database.authors[str(node_id)].surname + '"')
            # Then print the edges
            sys.stdout = open("author_graph_edges.csv", 'w')
            print("Source,Target")
            for edge in list(graph.edges):
                print(str(edge[0]) + ',' + str(edge[1]))
            # Restore the stdout
            sys.stdout = stdout_old
            print("Author graph export completed in " + str(time.time() - start_export_time) + " seconds")
        else:
            print("FatalError: While exporting author graph, graph was None!")
            sys.exit()

    def plot_global_choropleth(self):
        print("Plotting global choropleth graph...")
        # Create a dict with the infuence of each affiliation
        influence_affiliation_dict = dict()
        for author in list(self.storage.database.authors.values()):
            if author.affiliation is not None:
                authors_citations = author.citation_cnt # For this line to work it is assumed that the "most_cited_x_in_community" method is ran before this method.
                if author.affiliation.id in influence_affiliation_dict:
                    influence_affiliation_dict[author.affiliation.id] += authors_citations
                else:
                    influence_affiliation_dict[author.affiliation.id] = authors_citations
        # Then create a dictionary with the article count of each affiliation
        affiliation_articles_dict = dict()
        for article in self.storage.database.articles_with_eid:
            article_aff_ids = []
            for author in article.authors:
                if author.affiliation.id not in article_aff_ids:
                    article_aff_ids.append(author.affiliation.id)
            for aff_id in article_aff_ids:
                if aff_id in affiliation_articles_dict:
                    affiliation_articles_dict[aff_id] += 1
                else:
                    affiliation_articles_dict[aff_id] = 1
        # First get the most influentials affiliations (and countries)
        sorted_most_influential_affiliation_ids = sorted(iter(influence_affiliation_dict.items()), key=operator.itemgetter(1), reverse=True)
        # sorted_most_influential_affiliation_names = []
        most_influential_affiliation_countries = dict()
        for (idnum, freq) in sorted_most_influential_affiliation_ids:
            # try:
            #     affiliation_name = self.affiliation_dict[idnum].name
            # except KeyError as e:
            #     affiliation_name = "Unknown Affiliation"
            # sorted_most_influential_affiliation_names.append((affiliation_name, freq))
            # Now get the affiliation country
            try:
                affiliation_country = self.affiliation_dict[idnum].country
            except KeyError as e:
                affiliation_country = "Unknown Country"
            if affiliation_country in most_influential_affiliation_countries:
                most_influential_affiliation_countries[affiliation_country] += freq
            else:
                most_influential_affiliation_countries[affiliation_country] = freq
        sorted_most_influential_affiliation_countries = sorted(iter(most_influential_affiliation_countries.items()), key=operator.itemgetter(1), reverse=True)
        # Then get the most appeared affiliations (and coutries)
        article_sorted_affiliation_ids = sorted(iter(affiliation_articles_dict.items()), key=operator.itemgetter(1), reverse=True)
        # article_sorted_affiliation_names = []
        article_affiliation_countries = dict()
        for (idnum, freq) in article_sorted_affiliation_ids:
            # try:
            #     affiliation_name = self.affiliation_dict[idnum].name
            # except KeyError as e:
            #     affiliation_name = "Unknown Affiliation"
            # article_sorted_affiliation_names.append((affiliation_name, freq))
            # Now get the affiliation country
            try:
                affiliation_country = self.affiliation_dict[idnum].country
            except KeyError as e:
                affiliation_country = "Unknown Country"
            if affiliation_country in article_affiliation_countries:
                article_affiliation_countries[affiliation_country] += freq
            else:
                article_affiliation_countries[affiliation_country] = freq
        article_sorted_affiliation_countries = sorted(iter(article_affiliation_countries.items()), key=operator.itemgetter(1), reverse=True)

        choro_plot.ChoroplethPlotter("Articles produced by each country", article_sorted_affiliation_countries)
        choro_plot.ChoroplethPlotter("Most influential countries", sorted_most_influential_affiliation_countries)

    def initialize_sub_graph(self, auids):
        # Add authors as nodes
        sub_graph = nx.Graph()
        start_initialize_time = time.time()
        print("Adding authors to sub-graph...")
        for auid in auids:
            sub_graph.add_node(int(auid))
        # Add citations as edges
        print("Adding edges to sub-graph. " +  str(time.time() - start_initialize_time) + " seconds.")
        i_edge = 0
        i_author = 0
        print("iterating over " + str(len(sub_graph.nodes())) + " authors.")
        authors_in_graph = list(sub_graph.nodes())
        for author_id_int in authors_in_graph:
            i_author += 1
            auid = str(author_id_int)
            cited_authors = self.storage.database.authors[auid].get_cited_authors()
            for cited_author in cited_authors:
                if cited_author.auid in self.storage.database.authors and int(cited_author.auid) in authors_in_graph:
                    sub_graph.add_edge(int(auid), int(cited_author.auid))
                    i_edge += 1
        print(str(i_edge) + " edges. Time is " +  str(time.time() - start_initialize_time) + " seconds.")
        return sub_graph

    def partition_author_graph(self, graph, partition_type, randomize=False):
        # first compute the best partition
        print("Partitioning into communitites.")
        if partition_type == 'best':
            if isinstance(randomize, bool):
                return cmty.best_partition(graph, resolution=1.0, randomize=randomize, random_state=None)
            elif isinstance(randomize, int):
                return cmty.best_partition(graph, resolution=1.0, randomize=None, random_state=randomize)
        elif isinstance(partition_type, int):
            dendrogram = cmty.generate_dendrogram(graph)
            return cmty.partition_at_level(dendrogram, partition_type)

    def get_community_authors(self, partition, i):
        return [nodes for nodes in list(partition) if partition[nodes] == i]

    def get_community(self, partition_id, communities=None):
        if communities is None: # If None global analysis is carried out, otherwise sub-community ananalyis
            return [community for community in self.communities if community.partition_id == partition_id][0]
        else:
            return [community for community in communities if community.partition_id == partition_id][0]

    def get_n_communities(self, partition):
        if list(partition.values()):
            return max(list(partition.values())) + 1
        else:
            return 0

    def citations_from_community_a_to_b(self, community_a, community_b):
        n_citations = 0
        for a_author in community_a.authors:
            for article in a_author.articles:
                for reference in article.references:
                    for b_author in reference.authors:
                        if b_author in community_b.authors:
                            n_citations += 1
        return n_citations

    def print_intercommunity_citations(self, community_size_threshold):
        print("Below are listed highly unequal citing relations between comunitites, if there are any such relations:")
        for community_a in [c for c in self.communities if len(c.authors) > community_size_threshold]:
            for community_b in [c for c in self.communities if len(c.authors) > community_size_threshold]:
                if community_a != community_b:
                    cits_a_b = self.citations_from_community_a_to_b(community_a, community_b)
                    cits_b_a = self.citations_from_community_a_to_b(community_b, community_a)
                    if max(cits_a_b, cits_b_a) > CITATION_TRUNCATION_THRESHOLD:
                        if cits_b_a > 0 and float(cits_a_b*len(community_b.authors))/float(cits_b_a*len(community_a.authors)) > 2.0:
                            print(community_a.name + " (" + str(len(community_a.authors)) + ") cites " + community_b.name + " (" + str(len(community_b.authors)) + ") " + str(cits_a_b) + " times, while " + community_b.name + " cites " + community_a.name + " " + str(cits_b_a) + " times.")

    def propagate_citations(self, target, citation_value, discount_factor):
        target.inherited_citations += citation_value
        if citation_value > 0.1:
            for reference in target.references:
                self.propagate_citations(reference, citation_value*discount_factor, discount_factor)

    def print_statistics(self, citation_threshold=30, detailed_global_analysis=False):
        print("Number of articles is " + str(len(self.storage.database.articles)))
        print("Number of authors is " + str(len(self.storage.database.authors)))
        print("Number of keywords is " + str(len(self.storage.database.keywords)))
        print("Number of articles not found in scopus is " + str(len([a for a in self.storage.database.articles.values() if a.not_in_scopus])))
        print("Number of completely captured articles is " + str(len([a for a in self.storage.database.articles.values() if a.is_complete()])))
        print("Number of fully scraped articles (with eid) is " + str(len(self.storage.database.articles_with_eid)))
        print("Number of out-of-scope articles is " + str(len([a for a in self.storage.database.articles.values() if a.out_of_scope])))
        print("Now printing articles with citations > " + str(citation_threshold) + " :")
        for article, citations in sorted([(a, len(a.citations)) for k, a in self.storage.database.articles.items()], key=operator.itemgetter(1), reverse=True):
            if citations >= citation_threshold and not article.out_of_scope:
                if article.eid is not None:
                    print(str(citations) + ":  " + article.description_with_year())
                elif article.eid is None:
                    if article.not_in_scopus:
                        print(str(citations) + "*: " + article.description_with_year()) # Removed different output because during rescraping not_in_scopus is not reained
                    else:
                        print(str(citations) + "*: " + article.description_with_year())
            elif citations >= citation_threshold and article.out_of_scope:
                print(str(citations) + "**: " + article.description_with_year())
        print("(Where *: Article not fully scraped in database / Article not in Scopus if rescraping is already run)")
        print("(and **: Article is considered as out of scope)")
        n_links = 0
        for article in list(self.storage.database.articles.values()):
            n_links += len(article.references)
        print("Total number of citations is " + str(n_links))
        print("Yearly fully scraped article count:")
        print(self.global_annual_article_count())
        if detailed_global_analysis:
            truncate_threshold = 20
            print("Global top " + str(truncate_threshold) + " productive authors:")
            self.print_ranked_authors_by_articles(truncate_threshold=truncate_threshold)
            print("Global top " + str(truncate_threshold) + " productive sources:")
            print(self.global_sorted_sources(truncate_threshold=truncate_threshold))
            print("Global top " + str(truncate_threshold) + " productive affiliations:")
            print(self.global_sorted_affiliations_and_countries(truncate_threshold=truncate_threshold)[0])
            print("Global top " + str(truncate_threshold) + " productive affiliation countries:")
            print(self.global_sorted_affiliations_and_countries(truncate_threshold=truncate_threshold)[1])

    def print_ranked_authors_by_articles(self, truncate_threshold=None):
        authors = dict()
        for key, author in self.storage.database.authors.items():
            if author.get_articles() is not None:
                if author.full_name() not in authors:
                    authors[author.full_name()] = len(author.get_articles())
                else:
                    authors[author.full_name()] += len(author.get_articles())
        citation_sorted_authors = sorted(list(authors.items()), key=operator.itemgetter(1), reverse=True)
        if truncate_threshold is not None:
            for (key, publications) in citation_sorted_authors[:truncate_threshold]:
                print(key + ": " + str(publications))
        else:
            for (key, publications) in citation_sorted_authors:
                print(key + ": " + str(publications))
        return citation_sorted_authors

    def global_sorted_sources(self, truncate_threshold=None):
        global_sources = dict()
        for key, author in self.storage.database.authors.items():
            author_sources = author.sources()
            for source, frequency in author_sources.items():
                if source != None:
                    if source.name in global_sources:
                        global_sources[source.name] += frequency
                    else:
                        global_sources[source.name] = frequency
        if truncate_threshold is not None:
            sorted_truncated_sources = sorted(list(global_sources.items()), key=operator.itemgetter(1), reverse=True)[0:truncate_threshold]
        else:
            sorted_truncated_sources = sorted(list(global_sources.items()), key=operator.itemgetter(1), reverse=True)
        return sorted_truncated_sources

    def global_sorted_affiliations_and_countries(self, truncate_threshold=None):
        # A dictionary that will hold the articles produced by each affiliation id.
        affiliation_articles_dict = dict()
        articles_so_far = []
        for key, author in self.storage.database.authors.items():
            for article in author.articles:
                if article.eid in articles_so_far:
                    continue
                else:
                    articles_so_far.append(article.eid)
                    article_aff_ids = []
                    for author in article.authors:
                        if author.affiliation.id not in article_aff_ids:
                            article_aff_ids.append(author.affiliation.id)
                    for aff_id in article_aff_ids:
                        if aff_id in affiliation_articles_dict:
                            affiliation_articles_dict[aff_id] += 1
                        else:
                            affiliation_articles_dict[aff_id] = 1
        # Then get the nubmer of articles produced by each affiliation (and coutries)
        article_sorted_affiliation_ids = sorted(iter(affiliation_articles_dict.items()), key=operator.itemgetter(1), reverse=True)
        article_sorted_affiliation_names = []
        article_affiliation_countries = dict()
        for (idnum, freq) in article_sorted_affiliation_ids:
            try:
                affiliation_name = self.affiliation_dict[idnum].name
            except KeyError as e:
                affiliation_name = "Unknown Affiliation"
            article_sorted_affiliation_names.append((affiliation_name, freq))
            # Now get the affiliation country
            try:
                affiliation_country = self.affiliation_dict[idnum].country
            except KeyError as e:
                affiliation_country = "Unknown Country"
            if affiliation_country in article_affiliation_countries:
                article_affiliation_countries[affiliation_country] += freq
            else:
                article_affiliation_countries[affiliation_country] = freq
        article_sorted_affiliation_countries = sorted(iter(article_affiliation_countries.items()), key=operator.itemgetter(1), reverse=True)
        # And return everything
        if truncate_threshold is not None:
            return [article_sorted_affiliation_names[0:truncate_threshold], article_sorted_affiliation_countries[0:truncate_threshold]]
        else:
            return [article_sorted_affiliation_names, article_sorted_affiliation_countries]

    def global_annual_article_count(self):
        article_count = dict()
        for article in list(self.storage.database.articles_with_eid):
            try:
                year = int(article.date[0:4])
                if year in article_count:
                    article_count[year] += 1
                else:
                    article_count[year] = 1
            except Exception as e:
                pass

        return sorted(iter(article_count.items()), key=operator.itemgetter(0))

    def print_community_info(self, community_size_threshold, communities_to_print=None, n_keywords=COMMUNITY_KEYWORDS_TO_PRINT, n_sources=COMMUNITY_SOURCES_TO_PRINT, n_articles=COMMUNITY_ARTICLES_TO_PRINT):
        if self.partition or communities_to_print is not None:
            self.main_analysis = False
            if communities_to_print is None: # If communities_to_print is None then it means that the global analysis is carried out, otherwise sub-community analysis
                communities_to_print = self.communities
                self.main_analysis = True
            for community in communities_to_print:
                if community.name not in self.excluded_communities:
                    if len(community.authors) >= community_size_threshold:
                        sorted_truncated_keywords = community.keywords(n_keywords)
                        most_influential_keywords = community.most_influential_keywords(n_keywords)
                        sorted_truncated_sources = community.sources(n_sources)
                        most_cited_x = community.most_cited_x_in_community(main_analysis=self.main_analysis)
                        most_cited_articles_belonging_to_community = most_cited_x[0]
                        most_cited_authors = most_cited_x[1]
                        most_cited_years = most_cited_x[2]
                        # most_cited_authors = community.most_cited_authors_in_community()
                        # most_cited_articles_belonging_to_community = community.most_cited_article_in_community()
                        # most_cited_articles = community.most_by_community_cited_articles()
                        most_cited_articles = most_cited_x[3]
                        intra_community_cited_articles = most_cited_x[4]
                        publication_date = community.median_article_publication_date()
                        # sorted_affiliations = community.ranked_affiliations()[:n_sources]
                        # influential_affiliations = community.most_influential_affiliations()[:n_sources]
                        sorted_affiliations = community.most_x_affiliations_in_community()[0][:n_sources]
                        influential_affiliations = community.most_x_affiliations_in_community()[1][:n_sources]
                        sorted_affiliation_countries = community.most_x_affiliations_in_community()[2]
                        influential_affiliation_countries = community.most_x_affiliations_in_community()[3]
                        if len(sorted_truncated_keywords) > 0 and len(sorted_truncated_sources) > 0:
                            # if len(community.authors) >= community_size_threshold:
                            print("Community " + str(community.partition_id) + " = '" + str(community.name) + "' (" + str(len(community.authors)) + " members, " + str(publication_date) + "): ")
                            print("Most influential keywords:")
                            print(most_influential_keywords)
                            print("Most used keywords:")
                            print(sorted_truncated_keywords)
                            print("Sources:")
                            print(sorted_truncated_sources)
                            print("These are the most cited authors that belong to community " + str(community.partition_id) + ":")
                            print(most_cited_authors[:n_articles*2])
                            # print(most_cited_authors) # Uncomment to print all authors (except the ones with zero publications)
                            print("These are the most highly cited articles produced by community " + str(community.partition_id) + ":")
                            print(most_cited_articles_belonging_to_community[:n_articles])
                            print("Sorted number of citations of yearly publications by community " + str(community.partition_id) + ":")
                            print(most_cited_years)
                            print("Community " + str(community.partition_id) + " cites these articles the most:")
                            print(most_cited_articles[:n_articles-150])
                            print("Community " + str(community.partition_id) + " intra-community citations (i.e. articles produced and most cited by the community):")
                            print(intra_community_cited_articles[:n_articles-150])
                            print("Community " + str(community.partition_id) + " - Annual article count: ", end=' ')
                            print(community.annual_article_count())
                            print("Most productive affiliations (i.e. The number of articles per affiliation):")
                            print(sorted_affiliations)
                            print("Most influential affiliations (i.e. Affiliations in the community with the most citations by articles in the whole data set):")
                            print(influential_affiliations)
                            print("Most productive affiliation countries (i.e. The number of articles per country):")
                            print(sorted_affiliation_countries)
                            print("Most influential affiliation countries (i.e. Countries in the community with the most citations by articles in the whole data set):")
                            print(influential_affiliation_countries)
                            print()
                    elif len(community.authors) > community_size_threshold/2 and community.name != "anonymous":
                        print("Community " + str(community.partition_id) + " = '" + str(community.name) + "' (" + str(len(community.authors)) + " members): ")
                        print("Note: This community was excluded because community_size < community_size_threshold")
                        print()
                elif community.name in self.excluded_communities and len(community.authors) >= community_size_threshold:
                    print("Community " + str(community.partition_id) + " = '" + str(community.name) + "' (" + str(len(community.authors)) + " members): ")
                    print("Note: This community was excluded because community name is in excluded_communities")
                    print()
        else:
            print("Partition failed.")
            sys.exit()

    def print_community_json(self, community_size_threshold, community_json_output, n_keywords=COMMUNITY_KEYWORDS_TO_PRINT, n_sources=COMMUNITY_SOURCES_TO_PRINT, n_articles=COMMUNITY_ARTICLES_TO_PRINT):
        if self.partition:
            stdout_old = sys.stdout
            sys.stdout = open(community_json_output, 'w')
            print("{")
            print('"filename": "' + community_json_output + '",')
            print('"modularity": "' + str(self.modularity) + '",')
            print('"random_seed": "' + str(self.random_seed) + '",')
            print('"communities": [')
            not_first_community = False
            for community in self.communities:
                if community.name not in self.excluded_communities:
                    if len(community.authors) >= community_size_threshold:
                        if not_first_community:
                            print(",")
                        not_first_community = True
                        print("{")
                        print('"community_id": "' + str(community.partition_id) + '",')
                        print('"community_name": "' + str(community.name) + '",')
                        print('"member_count": "' +  str(len(community.authors)) + '",')
                        sorted_truncated_keywords = community.keywords(n_keywords)
                        sorted_truncated_just_keywords = community.just_keywords(n_keywords)
                        sorted_truncated_sources = community.sources(n_sources)
                        most_cited_x = community.most_cited_x_in_community_by_id()
                        most_cited_articles_belonging_to_community = most_cited_x[0]
                        most_cited_authors = most_cited_x[1]
                        if len(sorted_truncated_keywords) > 0 and len(sorted_truncated_sources) > 0:
                            print('"keywords": ' + json.dumps(sorted_truncated_just_keywords) + ',')
                            print('"authors": ' + json.dumps(most_cited_authors) + ',')
                            print('"articles": ' + json.dumps(most_cited_articles_belonging_to_community[:n_articles]) + '')
                        print("}")
            print("]")
            print("}")
            # Restore the stdout
            sys.stdout = stdout_old
        else:
            sys.exit()

    def print_community_csv(self, community_size_threshold, communities_to_print=None, sub_community_name=None, n_keywords=COMMUNITY_KEYWORDS_TO_PRINT, n_sources=COMMUNITY_SOURCES_TO_PRINT, n_articles=COMMUNITY_ARTICLES_TO_PRINT, csv_output=True, line_graphs=True, community_line_graphs_filename_prefix='main'):
        MANUAL_AXIS_LIMITS=True
        ARTICLE_LOW=1993
        ARTICLE_MAX=self.end_year # Get the end year for the plots from Analyzer's configuration
        ARTICLE_PERCENT_LOW=1972
        ARTICLE_PERCENT_MAX=self.end_year # Get the end year for the plots from Analyzer's configuration
        if self.partition:
            articles_df = pd.DataFrame(list(range(self.start_year, self.end_year+1)), columns =['Year'])
            citations_df = pd.DataFrame(list(range(self.start_year, self.end_year+1)), columns =['Year'])
            if communities_to_print is None: # If communities_to_print is None then it means that the global analysis is carried out, otherwise sub-community analysis
                communities_to_print = self.communities
            for community in communities_to_print:
                if community.name not in self.excluded_communities:
                    if len(community.authors) >= community_size_threshold:
                        articles_column_name = "Annual article count " + str(community.name) + ", community " + str(community.partition_id)
                        citations_column_name = "Citations of yearly publications by " + str(community.name) + ", community " + str(community.partition_id)
                        yearly_citations = dict(community.most_cited_x_in_community(main_analysis=False)[2])
                        yearly_articles = dict(community.annual_article_count())
                        articles_df[articles_column_name] = articles_df['Year'].map(yearly_articles)
                        citations_df[citations_column_name] = citations_df['Year'].map(yearly_citations)
            if csv_output:
                if sub_community_name is None:
                    articles_df.to_csv(path_or_buf=community_line_graphs_filename_prefix + "_community_articles.csv")
                    citations_df.to_csv(path_or_buf=community_line_graphs_filename_prefix + "_community_citations.csv")
                else:
                    articles_df.to_csv(path_or_buf=sub_community_name + "_community_articles.csv")
                    citations_df.to_csv(path_or_buf=sub_community_name + "_community_citations.csv")
            if line_graphs:
                # First create an annual article per community graph
                ax = plt.gca()
                if sub_community_name is None:
                    ax.title.set_text("Annual article count per community")
                else:
                    ax.title.set_text("Annual article count per sub-community")
                for index, col in enumerate(articles_df.iloc[:,1:].columns):
                        articles_df.plot(kind='line', style='.-', x='Year', y=str(col), label=str(col).replace("Annual article count", "").split(",")[0], ax=ax, color=self.colors[index % len(self.colors)])
                # Auto adjust the x axis (years)
                min_year=articles_df.iloc[:,0].get(articles_df.iloc[:,1:].first_valid_index())
                last_year=articles_df.iloc[:,0].get(articles_df.iloc[:,1:].last_valid_index())
                if not MANUAL_AXIS_LIMITS:
                    ax.set_xlim([min_year-1,last_year+1])
                else:
                    ax.set_xlim([ARTICLE_LOW,ARTICLE_MAX])
                plt.legend(loc='upper left', prop={'size': 8})
                if sub_community_name is None:
                    plt.savefig(community_line_graphs_filename_prefix + '_community_articles.png', dpi=1000)
                else:
                    plt.savefig(sub_community_name.replace(' ', '_') + "_community_articles.png", dpi=1000)
                plt.clf()
                # Then create the annual community article publications percentage graph
                ax = plt.gca()
                if sub_community_name is None:
                    ax.title.set_text("Annual publication percentage per community")
                else:
                    ax.title.set_text("Annual publication percentage per sub-community")
                for index, row in articles_df.iterrows():
                    articles_df.iloc[index,1:] = articles_df.iloc[index,1:]/articles_df.iloc[index,1:].sum()
                for index, col in enumerate(articles_df.iloc[:,1:].columns):
                    articles_df.plot(kind='line', style='.-', alpha=0.5, x='Year', y=str(col), label=str(col).replace("Annual article count", "").split(",")[0], ax=ax, color=self.colors[index % len(self.colors)])
                if not MANUAL_AXIS_LIMITS:
                    ax.set_xlim([min_year-1,last_year+1])
                else:
                    ax.set_xlim([ARTICLE_PERCENT_LOW-1,ARTICLE_PERCENT_MAX])
                plt.legend(loc='best', prop={'size': 8})
                ax.set_yticklabels(['{:,.2%}'.format(x) for x in ax.get_yticks()])
                plt.autoscale(enable=True, axis='y', tight=False)
                if sub_community_name is None:
                    plt.savefig(community_line_graphs_filename_prefix + '_community_articles_percent.png', dpi=1000)
                else:
                    plt.savefig(sub_community_name.replace(' ', '_') + "_community_articles_percent.png", dpi=1000)
                plt.close()
        else:
            sys.exit()

    def plot_author_graph(self, included_communities, show_edges=True, min_node_size=2.0, node_size_factor=1.0, citation_threshold=1, label_citation_threshold=15, node_distance_factor=3.0, edge_color='black', font_size=2, filename='author_graph.png', author_country_filter=None):
        print("Plotting author graph...", end=' ')
        included_author_auids = set([])
        for community in included_communities:
            for author in community.authors:
                if author.citation_cnt >= citation_threshold:
                    if author_country_filter is None:
                        included_author_auids.add(author.auid)
                    elif isinstance(author_country_filter, str) and author.affiliation.country is not None:
                        if author.affiliation.country.lower() == author_country_filter.lower():
                            included_author_auids.add(author.auid)
        print("Author graph displays " + str(len(included_author_auids)) + " authors.", end=' ')
        pruned_author_graph = self.author_graph.copy()
        nodes_to_remove = []
        for node in pruned_author_graph.nodes():
            if str(node) not in included_author_auids:
                nodes_to_remove.append(node)
                #pruned_author_graph.remove_node(node)
        for node in nodes_to_remove:
            pruned_author_graph.remove_node(node)
        pos = nx.spring_layout(pruned_author_graph, k=node_distance_factor/math.sqrt(len(pruned_author_graph.nodes())))
        if show_edges:
            nx.draw_networkx_edges(pruned_author_graph, pos, edge_color=edge_color, alpha=0.5, width=0.1)
        for index, community in enumerate(included_communities):
            list_nodes = [node for node in pruned_author_graph.nodes() if str(node) in community.auids()]

            citation_list = []
            node_name_dict = dict()
            for auid in list_nodes:
                # citations = 0
                # for article in self.storage.database.authors[str(auid)].articles:
                #     citations += len(article.citations)
                citations = self.storage.database.authors[str(auid)].citation_cnt
                citation_list.append(citations)
                if citations >= label_citation_threshold:
                    node_name_dict[auid] = self.storage.database.authors[str(auid)].surname
                else:
                    node_name_dict[auid] = ""
            node_size_list = [node_size_factor * c + min_node_size for c in citation_list]
            nx.draw_networkx_nodes(pruned_author_graph, pos, list_nodes, node_size=node_size_list,
                                   node_color=self.colors[index % len(self.colors)], edgecolors="#878787", linewidths=0.1)
            nx.draw_networkx_labels(pruned_author_graph, pos, node_name_dict, font_size=font_size)
        plt.axis('off')
        plt.tight_layout()
        plt.savefig(filename, dpi=1000)
        print("Done.")
        plt.close()

    def plot_community_graph(self, graph, partition, community_size_threshold, communities_to_graph=None, node_size_factor=3.0, edge_width_factor=0.1, node_distance_factor=1.0, font_size=5, filename='community_graph.png'):
        print("Plotting community graph...", end=' ')
        if communities_to_graph is None: # If communities_to_graph is None then it means that the global analysis is carried out, otherwise sub-community analysis
                communities_to_graph = self.communities
        induced_graph = cmty.induced_graph(partition, graph)
        exclude_list = []
        for community in communities_to_graph:
            if len(community.authors) < community_size_threshold:
                exclude_list.append(community.partition_id)
            if community.name in self.excluded_communities:
                exclude_list.append(community.partition_id)
        induced_graph.remove_nodes_from(exclude_list)

        if len(induced_graph.nodes()) == 0:
            print("induced_graph.nodes() is empty, aborting plotting of community graph!")
            return

        edge_list = []
        width_list = []
        weight_list = []
        for i_edge, j_edge_dict in induced_graph.adj.items():
            for j_edge, weight in j_edge_dict.items():
                weight_list.append(float(weight['weight']))
        normalized_edge_width_factor = edge_width_factor/max(weight_list)
        for i_edge, j_edge_dict in induced_graph.adj.items():
            for j_edge, weight in j_edge_dict.items():
                edge_list.append((i_edge, j_edge))
                if communities_to_graph is not None and (normalized_edge_width_factor*float(weight['weight'])>=edge_width_factor*0.50) and (len(self.get_community(i_edge, communities=communities_to_graph).authors)<=555 or len(self.get_community(j_edge, communities=communities_to_graph).authors)<=555) and (i_edge != j_edge): # If we are plotting sub-community graphs, perform one more check to verify edges will not be too thick for small nodes.
                    width_list.append(normalized_edge_width_factor/2*float(weight['weight']))
                elif communities_to_graph is not None and (normalized_edge_width_factor*float(weight['weight'])>=edge_width_factor*0.35) and (len(self.get_community(i_edge, communities=communities_to_graph).authors)<=350 or len(self.get_community(j_edge, communities=communities_to_graph).authors)<=350) and (i_edge != j_edge): # And a second check for even smaller communities (especially on smaller analyses).
                    width_list.append(normalized_edge_width_factor/3*float(weight['weight']))
                else:
                    width_list.append(normalized_edge_width_factor*float(weight['weight']))
        try:
            pos = nx.spring_layout(induced_graph, k=node_distance_factor/math.sqrt(len(induced_graph.nodes())), iterations=400)
        except ZeroDivisionError:
            pos = nx.fruchterman_reingold_layout(induced_graph, iterations=400)
        if communities_to_graph is None: # The same, check for global analysis or sub-graph analysis
            max_community_auhthors = max([len(self.get_community(partition_id).authors) for partition_id in induced_graph.nodes()])
            normalized_node_size_factor = node_size_factor/max_community_auhthors
            node_size_list = [normalized_node_size_factor*len(self.get_community(partition_id).authors) for partition_id in induced_graph.nodes()]
        else: # Here is sub-communities graphs
            max_community_auhthors = max([len(self.get_community(partition_id, communities=communities_to_graph).authors) for partition_id in induced_graph.nodes()])
            normalized_node_size_factor = node_size_factor/max_community_auhthors
            node_size_list = [normalized_node_size_factor*len(self.get_community(partition_id, communities=communities_to_graph).authors) for partition_id in induced_graph.nodes()]
        node_name_dict = dict()
        for partition_id in induced_graph.nodes():
            if communities_to_graph is None:  # The same, check for global analysis or sub-graph analysis
                node_name_dict[partition_id] = self.get_community(partition_id).name
            else:
                node_name_dict[partition_id] = self.get_community(partition_id, communities=communities_to_graph).name

        # Below lines should be uncommented if plotly should be used for the generation of community graphs
        # new_node_size_list = [x * 1/normalized_node_size_factor for x in node_size_list]
        # new_widht_list = [x * 1/edge_width_factor for x in width_list]
        # graph_plot.PlotlyGraphPlotter("Community Graph", induced_graph, pos, new_node_size_list, new_widht_list, edge_list, node_name_dict)

        color_list = [self.colors[index % len(self.colors)] for index, community in enumerate(induced_graph.nodes())]
        nx.draw_networkx_edges(induced_graph, pos, edgelist=edge_list, width=width_list,  alpha=0.1)
        nx.draw_networkx_nodes(induced_graph, pos, induced_graph.nodes(), node_size=node_size_list,
                               node_color=color_list, edgecolors="#878787", linewidths=0.5)
        nx.draw_networkx_labels(induced_graph, pos, node_name_dict, font_size=font_size)
        plt.axis('off')
        # Use this to adjust the plot size so that lebels do not get cropped in the edges
        plt.tight_layout()
        plt.margins(0.165)
        plt.savefig(filename, dpi=1000)
        print("Done.")
        plt.close()
        if self.export_graph_data:
            self.export_community_graph(edge_list, width_list, list(induced_graph.nodes), node_name_dict, node_size_list, filename)

    def export_community_graph(self, edge_list, width_list, node_list, node_name_dict, node_size_list, filename):
        start_export_time = time.time()
        stdout_old = sys.stdout
        # First print the nodes
        sys.stdout = open(filename.split('.')[0] + "_nodes.csv", 'w')
        print("Id,Label,Size")
        i = 0
        for node_id in node_list:
            print(str(node_id) + ',"' + node_name_dict[node_id] + '",' + str(node_size_list[i]))
            # print(str(node_id) + ',' + node_name_dict[node_id])
            i += 1
        # Then print the edges
        sys.stdout = open(filename.split('.')[0] + "_edges.csv", 'w')
        print("Source,Target,Weight")
        i = 0
        for edge in edge_list:
            print(str(edge[0]) + ',' + str(edge[1]) + ',' + str(width_list[i]))
            i += 1
        # Restore the stdout
        sys.stdout = stdout_old
        print("Community graph export completed in " + str(time.time() - start_export_time) + " seconds")
        print(node_size_list)

    def analyze_all(self,
                    community_graph_filename='community_graph.png',
                    community_graph_node_size_factor=1,
                    community_graph_edge_width_factor=0.5,
                    community_graph_node_distance_factor=3,
                    community_graph_font_size=5,
                    main_author_graph=False,
                    main_author_graph_show_edges=False,
                    main_author_graph_min_node_size=0.5,
                    main_author_graph_node_size_factor=1,
                    main_author_graph_citation_threshold=1,
                    main_author_graph_label_citation_threshold=15,
                    main_author_graph_node_distance_factor=6,
                    main_author_graph_font_size=2,
                    main_author_graph_edge_color='lightgray',
                    main_author_graph_filename='author_graph.png',
                    main_author_graph_country_filter=None,
                    main_choropleth_graph=False,
                    community_author_graphs=False,
                    community_author_graph_show_edges=True,
                    community_author_graph_min_node_size=2.0,
                    community_author_graph_node_size_factor=18,
                    community_author_graph_label_citation_threshold=3,
                    community_author_graph_node_distance_factor=0.5,
                    community_author_graph_edge_color='black',
                    community_author_graph_font_size=5,
                    community_json_output=False,
                    community_csv_output=False,
                    community_line_graphs=True,
                    community_line_graphs_filename_prefix='main'):
        self.print_community_info(self.community_size_threshold)
        if community_json_output:
            self.print_community_json(self.community_size_threshold, community_json_output)
        if community_csv_output or community_line_graphs:
            self.print_community_csv(self.community_size_threshold, csv_output=community_csv_output, line_graphs=community_line_graphs, community_line_graphs_filename_prefix=community_line_graphs_filename_prefix)
        self.print_intercommunity_citations(self.community_size_threshold)
        included_communities = [community for community in self.communities if
                                len(community.authors) >= self.community_size_threshold and community.name not in self.excluded_communities]
        self.plot_community_graph(self.author_graph, self.partition, self.community_size_threshold,
                                  node_size_factor=community_graph_node_size_factor,
                                  edge_width_factor=community_graph_edge_width_factor,
                                  node_distance_factor=community_graph_node_distance_factor,
                                  font_size=community_graph_font_size,
                                  filename = community_graph_filename)
        if main_author_graph:
            author_graph_plot_start_time = time.time()
            self.plot_author_graph(included_communities,
                                   show_edges=main_author_graph_show_edges,
                                   min_node_size=main_author_graph_min_node_size,
                                   node_size_factor=main_author_graph_node_size_factor,
                                   citation_threshold=main_author_graph_citation_threshold,
                                   label_citation_threshold=main_author_graph_label_citation_threshold,
                                   node_distance_factor=main_author_graph_node_distance_factor,
                                   edge_color=main_author_graph_edge_color,
                                   font_size=main_author_graph_font_size,
                                   filename=main_author_graph_filename,
                                   author_country_filter=main_author_graph_country_filter)
            print("Time to plot author graph was " + str(time.time() - author_graph_plot_start_time) + " seconds.")
        if community_author_graphs:
            for community in included_communities:
                self.plot_author_graph([community],
                                       show_edges=community_author_graph_show_edges,
                                       min_node_size=community_author_graph_min_node_size,
                                       node_size_factor=community_author_graph_node_size_factor,
                                       label_citation_threshold=community_author_graph_label_citation_threshold,
                                       node_distance_factor=community_author_graph_node_distance_factor,
                                       edge_color=community_author_graph_edge_color,
                                       font_size=community_author_graph_font_size,
                                       filename=community.name.replace("/", "_") + ".png")
        if self.sub_com_analysis:
            print("\n##### Sub-community analyses below this line #####")
            i = 0
            new_community_size_threshold = int(self.community_size_threshold/self.sub_community_size_threshold_divider)
            print("sub-community size threshold = " + str(new_community_size_threshold))
            for cmt in self.sub_communities:
                # First print the sub-communities info
                print("\n--- Sub-community analysis for community: " + self.community_names[i] + " ---")
                self.print_community_info(new_community_size_threshold, communities_to_print=cmt)
                if community_csv_output or community_line_graphs:
                    self.print_community_csv(new_community_size_threshold, communities_to_print=cmt, sub_community_name=self.community_names[i], csv_output=community_csv_output, line_graphs=community_line_graphs)
                print("--- /End of sub-community analysis for community: " + self.community_names[i] + " ---")
                # Then plot the sub-community graph
                self.plot_community_graph(self.sub_graphs[i], self.sub_partitions[i], new_community_size_threshold, communities_to_graph=cmt,
                                  node_size_factor=community_graph_node_size_factor,
                                  edge_width_factor=community_graph_edge_width_factor,
                                  node_distance_factor=community_graph_node_distance_factor/self.sub_community_size_threshold_divider,
                                  font_size=community_graph_font_size,
                                  filename = "sub_community_graph_" + self.community_names[i].replace(' ', '_') + ".png")
                i += 1
        if self.sub2_com_analysis:
            print("\n##### Sub^2-community analyses below this line #####")
            i = 0
            new_community_size_threshold = int(self.community_size_threshold/(self.sub_community_size_threshold_divider*4.0))
            print("sub^2-community size threshold = " + str(new_community_size_threshold))
            for cmt in self.sub2_communities:
                # First print the sub^2-communities info
                print("\n--- Sub^2-community analysis for sub-community: " + self.sub2_community_names[i] + " ---")
                self.print_community_info(new_community_size_threshold, communities_to_print=cmt)
                print("--- /End of sub^2-community analysis for sub-community: " + self.sub2_community_names[i] + " ---")
                # Then plot the sub^2-community graph
                self.plot_community_graph(self.sub2_graphs[i], self.sub2_partitions[i], new_community_size_threshold, communities_to_graph=cmt,
                                node_size_factor=community_graph_node_size_factor,
                                edge_width_factor=community_graph_edge_width_factor,
                                node_distance_factor=community_graph_node_distance_factor/self.sub_community_size_threshold_divider,
                                font_size=community_graph_font_size,
                                filename = "sub^2_community_graph_" + self.sub2_community_names[i].replace(' ', '_') + ".png")
                i += 1
        if main_choropleth_graph:
            self.plot_global_choropleth()               
        self.storage.database.print_keywords(KEYWORD_TRUNCATION_THRESHOLD)
        if str((time.time() - self.start_time) < 3600):
            print("Total analysis time was " + str((time.time() - self.start_time)/60) + " minutes.")
        else:
            print("Total analysis time was " + str((time.time() - self.start_time)/3600) + " hours.")
