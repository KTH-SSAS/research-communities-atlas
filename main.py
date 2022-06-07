from scraper import Scraper, YearlyCountScraper, AffiliationScraper
from storage import CloudStorage, Article
from graph_analyzer import Analyzer, nx, cmty
from atlas_config import GOOGLE_KEY_PATH, API_KEY
import sys, os.path, time, json
import signal
from functools import partial
import numpy as np
from datetime import datetime

CURRENT_YEAR = datetime.today().year
YAC_FILE = 'yac.json'
PRINT_TO_FILE = True # Enable this to write output to a file
SENS_ANAL_RUNS = 100 # This specifies the number of runs for the sensitivity analysis of the analysis results
general_query ='KEY("Security Of Data") OR KEY("Information Security") OR KEY("Cyber Security") OR KEY("Network Security") OR KEY("Computer Crime") OR KEY("Cryptography") OR KEY("Security Systems") OR KEY("Cybersecurity") OR KEY("Authentication") OR KEY("Intrusion Detection") OR (KEY("Access Control") AND TITLE-ABS-KEY ("Security")) OR (KEY( "Mobile Security") AND NOT KEY("Cytology")) OR KEY("Cyber-attacks") OR KEY("Malware") OR KEY("Computer Security") OR (KEY("Privacy") AND TITLE-ABS-KEY ("Security")) OR KEY("Steganography") OR KEY("Computer Viruses") OR KEY("Security Requirements") OR KEY("Security Policy") OR (KEY("Digital Watermarking") AND TITLE-ABS-KEY ("Security")) AND (SUBJAREA(COMP) OR SUBJAREA(ENGI) OR SUBJAREA(MATH) OR SUBJAREA(SOCI) OR SUBJAREA(BUSI) OR SUBJAREA(DECI) OR SUBJAREA(MULT) OR SUBJAREA(Undefined)) AND (LANGUAGE(English))'
ag_query = '(KEY ("Attack Graph") AND TITLE-ABS-KEY ("Security")) OR (KEY ("Threat Model*") AND TITLE-ABS-KEY ("Security")) OR (KEY ("Attack Tree") AND TITLE-ABS-KEY ("Security")) OR (KEY ("Bayesian Networks") AND (TITLE-ABS-KEY ("Cyber Security") OR TITLE-ABS-KEY ("Information Security"))) OR (KEY ("Attack Path") AND TITLE-ABS-KEY ("Security")) OR (KEY ("Markov Processes") AND (TITLE-ABS-KEY ("Cyber Security") OR TITLE-ABS-KEY ("Information Security"))) OR (KEY ("Attack Model*") AND TITLE-ABS-KEY ("Security")) OR (KEY ("Attack Simulations") AND TITLE-ABS-KEY ("Security")) AND (SUBJAREA (COMP) OR  SUBJAREA (ENGI) OR SUBJAREA (MATH) OR SUBJAREA (SOCI) OR SUBJAREA (BUSI) OR SUBJAREA (DECI) OR SUBJAREA (MULT) OR SUBJAREA (Undefined)) AND (LANGUAGE (English))'
mlai_query ='(KEY("Security Of Data") OR KEY("Information Security") OR KEY("Cyber Security") OR KEY("Network Security") OR KEY("Computer Crime") OR KEY("Cryptography") OR KEY("Security Systems") OR KEY("Cybersecurity") OR KEY("Authentication") OR KEY("Intrusion Detection") OR (KEY("Access Control") AND TITLE-ABS-KEY ("Security")) OR (KEY( "Mobile Security") AND NOT KEY("Cytology")) OR KEY("Cyber-attacks") OR KEY("Malware") OR KEY("Computer Security") OR (KEY("Privacy") AND TITLE-ABS-KEY ("Security")) OR KEY("Steganography") OR KEY("Computer Viruses") OR KEY("Security Requirements") OR KEY("Security Policy") OR (KEY("Digital Watermarking") AND TITLE-ABS-KEY ("Security"))) AND (KEY("machine learning") OR KEY("artificial intelligence") OR KEY("deep learning") OR KEY("neural network")) AND (SUBJAREA(COMP) OR SUBJAREA(ENGI) OR SUBJAREA(MATH) OR SUBJAREA(SOCI) OR SUBJAREA(BUSI) OR SUBJAREA(DECI) OR SUBJAREA(MULT) OR SUBJAREA(Undefined)) AND (LANGUAGE(English))'
pentest_query = '("Vuln* research" OR "Vuln* assess*" OR "Vuln* analysis" OR "Security test*" OR "Penetration test*" OR "Ethical hack*" OR "Pentest*" OR "Red team*" OR "Offensive Security") AND (SUBJAREA("COMP"))'

def retrieve_from_scopus(automated=True):
    print("You will need a Scopus API key, which can be generated at https://dev.elsevier.com/apikey/manage. Replace the placeholder API key in the atlas_config.py file.")
    print("The data will be stored in Google Datastore. The path to the Google API key should be set as an environmental variable with name 'GOOGLE_APPLICATION_CREDENTIALS'. This is done programmatically on this script, but the path is specified on the atlas_config.py file.")
    print("The current Scopus query is:")
    print(general_query)
    print("This can also be changed in the main.py script.")
    print("scraping_persistence=1 will terminate after five failed attempts. scraping_persistence=2 will terminate after ten, etc.")

    # First check if the yac file exists
    check_yac_file(base_query=general_query)
    # Then initialize a Scraper instance
    scraper_inst = Scraper(API_KEY, scraping_persisitence=2, automated=automated, datastore_default_kind=False, datastore_kind_suffix='complete')
    signal.signal(signal.SIGINT, partial(scraper_signal_handler, scraper_inst))
    # Then scrape!
    scraper_inst.scrape_proportionally_per_year(general_query)

def retrieve_kth_only_from_scopus(automated=True):
    kth_query = general_query + 'AND (AF-ID(60002014))' # 60002014 is the ID of "The Royal Institute of Technology KTH" on Scopus
    # First check if the yac file exists
    check_yac_file("yac_kth.json", kth_query)
    print("The current Scopus query is:")
    print(kth_query)
    # Then initialize a Scraper instance
    scraper_inst = Scraper(API_KEY, scraping_persisitence=2, automated=automated, datastore_default_kind=False, datastore_kind_suffix='kth', yac_file="yac_kth.json")
    signal.signal(signal.SIGINT, partial(scraper_signal_handler, scraper_inst))
    # Then scrape!
    scraper_inst.scrape_everything_per_year(kth_query)

def retrieve_swedish_only_from_scopus(automated=True):
    swe_query = general_query + 'AND (AFFILCOUNTRY(sweden))'
    # First check if the yac file exists
    check_yac_file("yac_swe.json", swe_query)
    print("The current Scopus query is:")
    print(swe_query)
    # Then initialize a Scraper instance
    scraper_inst = Scraper(API_KEY, scraping_persisitence=2, automated=automated, datastore_default_kind=False, datastore_kind_suffix='swe',yac_file="yac_swe.json")
    signal.signal(signal.SIGINT, partial(scraper_signal_handler, scraper_inst))
    # Then scrape!
    scraper_inst.scrape_everything_per_year(swe_query)

def retrieve_ag_from_scopus(automated=True):
    # First check if the yac file exists
    check_yac_file("yac_ag.json", ag_query)
    print("The current Scopus query is:")
    print(ag_query)
    # Then initialize a Scraper instance
    scraper_inst = Scraper(API_KEY, scraping_persisitence=2, automated=automated, datastore_default_kind=False, datastore_kind_suffix='ag',yac_file="yac_ag.json")
    signal.signal(signal.SIGINT, partial(scraper_signal_handler, scraper_inst))
    # Then scrape!
    scraper_inst.scrape_everything_per_year(ag_query)

def retrieve_mlai_from_scopus(automated=True):
    # First check if the yac file exists
    check_yac_file("yac_mlai.json", mlai_query)
    print("The current Scopus query is:")
    print(mlai_query)
    # Then initialize a Scraper instance
    scraper_inst = Scraper(API_KEY, scraping_persisitence=2, automated=automated, datastore_default_kind=False, datastore_kind_suffix='mlai', yac_file="yac_mlai.json")
    signal.signal(signal.SIGINT, partial(scraper_signal_handler, scraper_inst))
    # Then scrape!
    scraper_inst.scrape_proportionally_per_year(mlai_query)

def retrieve_pentest_from_scopus(automated=True, query_to_use=pentest_query):
    # First check if the yac file exists
    check_yac_file("yac_pentest.json", query_to_use)
    print("The current Scopus query is:")
    print(query_to_use)
    # Then initialize a Scraper instance
    scraper_inst = Scraper(API_KEY, scraping_persisitence=2, automated=automated, datastore_default_kind=False, datastore_kind_suffix='pentest',yac_file="yac_pentest.json")
    signal.signal(signal.SIGINT, partial(scraper_signal_handler, scraper_inst))
    # Then scrape!
    scraper_inst.scrape_everything_per_year(query_to_use)

def check_yac_file(yac_filename=YAC_FILE, base_query=general_query):
    if not os.path.isfile(yac_filename):
        print("Creating yearly article count (yac) file...")
        ycs = YearlyCountScraper(API_KEY, base_query)
        ycs.getArticleCountAllYears()
        yac = ycs.getYearlyArticleCount()
        json.dump(yac, open(yac_filename, 'w'))
    # If existing file is older than one month
    elif os.path.isfile(yac_filename) and (time.time() - os.path.getmtime(yac_filename) > (30 * 24 * 60 * 60)):
        print("\nWARNING: File yac.json already exists but it is older than one month.\nUpdating it...\n")
        ycs = YearlyCountScraper(API_KEY, base_query)
        ycs.getArticleCountAllYears()
        yac = ycs.getYearlyArticleCount()
        json.dump(yac, open(yac_filename, 'w'))
    else:
        print("A recent yac.json file already exists, skipping...")

def scraper_signal_handler(scraper_inst, sig, frame):
    # First restore the stdout to the sys.stdout
    sys.stdout = sys.__stdout__
    print(">>> You pressed Ctrl+C! <<<")
    answer = ''
    while answer not in ["y", "n"]:
        answer = input("Do you want to store the database before exiting? [y/n] ").lower()
    if answer == "y":
        print("Storing first and then exit...")
        ret = scraper_inst.store_after_signal()
        if ret == False:
            print("FATAL ERROR: Something went really wrong during storing...")
            sys.exit(1)
        else:
            print("Exiting...")
            sys.exit(0)
    else:
        sys.exit(1)

def print_versions():
    print("Timestamp: " + str(datetime.now()))
    print("INFO: Versions")
    print("Python's version: ", end='')
    print(sys.version)
    print("Python-louvain(community) module version: ", end='')
    print(cmty.__version__)
    print("Networkx module version: ", end='')
    print(nx.__version__)

def retrieve_affiliations_from_scopus(datastore_default_kind=True, datastore_kind_suffix=None):
    affiliationScraper = AffiliationScraper(API_KEY, datastore_default_kind=datastore_default_kind, datastore_kind_suffix=datastore_kind_suffix)
    affiliation_dict, affiliation_ids = affiliationScraper.load_affiliation_dict()
    affiliation_dict = affiliationScraper.scrape_affiliation_dict(affiliation_ids, affiliation_dict)
    affiliationScraper.store_affiliation_dict(affiliation_dict)
    #print affiliation_dict

def initialize_storage(datastore_default_kind=True, datastore_kind_suffix=None, start_year_filter=None, end_year_filter=None):
    datastore = CloudStorage(datastore_default_kind=datastore_default_kind, datastore_kind_suffix=datastore_kind_suffix, start_year_filter=start_year_filter, end_year_filter=end_year_filter)
    return datastore

def analyze(datastore, automated=True, randomize=False, community_json_output=False, detailed_global_analysis=False, sub_com_analysis=False, sub2_com_analysis=False, modularity_threshold=None, start_year=1945, end_year=2025):
    print(">>> Initiating Security Atlas analysis procedure... <<<")
    print("Three different main types of graphs can be generated.")
    print("In the community graph (CG), a node represents a community, and the relations represent the extent to which community authors cite each other. Node size is determined by community size in terms of authors.")
    print("In the main author graph (AG), each node represents an author, and relations represent citations. Node size is proportional to the number of citations.")
    print("The community author graphs (CAGs) are similar to the main author graph, but limited to the authors within a certain community.")
    print("Additionaly to those graphs two choropleth graphs can be generated that present the articles produced by each country and the globally most influential countries.")
    print("Finally, line graphs presenting the annual article count per community or sub-community can also be generated.")
    # print("Various analysis options can be adjusted in main.py.")
    # The 'automated' argument is currently not used and providing a value to it will not have an effect!
    # print("automated should be True if the program is invoked with command line parameters and without user interaction.")
    # print("start_year and end_year determine the time window to be considered. Thus, the analysis can be limited to a certain eange of years.")
    # print("community_graph_node_side_factor affects the size of the nodes in the community graph. In addition to this factor, the number of authors in each community determines its size.")
    # print("community_graph_edge_width_factor affects the width of edges in the community graph.")
    # print("community_graph_node_distance_factor affects the distance betweeen nodes. The greater the number, the greater the node separation.")
    # print("main_author_graph_min_node_size specifies the size of nodes representing authors with no citations. The more citations, the bigger the nodes.")
    # print("main_author_graph_node_size_factor is similar to community_graph_node_side_factor.")
    # print("main_author_graph_label_citation_threshold specifies the number of citations required by an author to deserve her name as a label on the node (to many labels make the graph cluttered).")
    # print("main_author_graph_node_distance_factor is similar to community_graph_node_distance_factor.")
    # print("main_author_graph_font_size sets the font size of labels.")
    # print("main_author_graph_edge_color can assume any color in matplotlib, e.g. as specified here https://i.stack.imgur.com/k2VzI.png.")
    # print("main_author_graph_filename specifies the name of the main author graph.")
    # print("community_author_graph_min_node_size specifies the size of the smallest nodes in the community author graphs, i.e. those that represent authors without citations.")
    # print("community_author_graph_node_size_factor is similar to community_graph_node_side_factor.")
    # print("community_author_graph_label_citation_threshold is similar to main_author_graph_label_citation_threshold.")
    # print("community_author_graph_node_distance_factor is similar to community_graph_node_distance_factor.")
    # print("community_author_graph_edge_color is similar to main_author_graph_edge_color.")
    # print("community_author_graph_font_size is similar to main_author_graph_font_size.")
    # print("community_size_threshold delimits the analysis to communities of at least the threshold number of authors.")

    if start_year is not None:
        start_year = int(start_year[:4])
    else:
        start_year = 1945 # Default
    if end_year is not None:
        end_year = int(end_year[:4])
    else:
        end_year = CURRENT_YEAR # Default

    community_graph_filename='community_graph.png'
    if community_json_output:
        community_graph_filename = community_json_output.split(".json")[0] + "_community_graph.png"

    # Community Graph (CG) configuration variables
    COMMUNITY_SIZE_THRESHOLD = 500  # Default was 300 and I use 500 (for the KTH dataset use 20)
    CG_NODE_SIZE_FACTOR = 1800      # Default was 0.1, but I use 0.18 (1800)
    CG_EDGE_WIDTH_FACTOR = 80       # Default was 0.001, but I use 0.0002 (80) (for the KTH dataset use 0.1)
    CG_NODE_DISTANCE_FACTOR = 15    # Default was 8, I use 10 or 15 (for the KTH dataset use 100)

    analyzer = Analyzer(database=datastore.database,
                        automated=automated,
                        start_year=start_year,
                        end_year=end_year,
                        keyword="",
                        randomize=randomize,
                        detailed_global_analysis=detailed_global_analysis,
                        sub_com_analysis=sub_com_analysis,
                        sub2_com_analysis=sub2_com_analysis,
                        modularity_threshold=modularity_threshold,
                        community_size_threshold=COMMUNITY_SIZE_THRESHOLD,
                         sub_community_size_threshold_divider=2.5, # 500/2.5 = 200
                        export_graph_data=False)
    if not analyzer.modularity_threshold_fullfiled:
        print("Aborting this run...")
        return
    analyzer.analyze_all(community_graph_filename=community_graph_filename,
                         community_graph_node_size_factor=CG_NODE_SIZE_FACTOR,
                         community_graph_edge_width_factor=CG_EDGE_WIDTH_FACTOR,
                         community_graph_node_distance_factor=CG_NODE_DISTANCE_FACTOR,
                         community_graph_font_size=8,                   # Default was 8 or 9/5
                         main_author_graph=False,                       # Default is True (of course)
                         main_author_graph_show_edges=False,            # (best for print/best for analysis)
                         main_author_graph_min_node_size=0.0001,        # Default was 0.1/0.0001
                         main_author_graph_node_size_factor=0.005,      # Default was 0.1/0.005
                         main_author_graph_citation_threshold=100,      # Default was 12/100 (for small analyses use either 0 or 1)
                         main_author_graph_label_citation_threshold=800,# Default was 800
                         main_author_graph_node_distance_factor=40,     # Default was 6/40
                         main_author_graph_font_size=0.01,              # Default was 2
                         main_author_graph_edge_color='lightgray',
                         main_author_graph_filename='author_graph.png',
                         main_choropleth_graph=True,
                         community_author_graphs=False,
                         community_author_graph_show_edges=True,
                         community_author_graph_min_node_size=0.1,
                         community_author_graph_node_size_factor=1.0,       # Default was 1.0
                         community_author_graph_label_citation_threshold=67,# Default was 67
                         community_author_graph_node_distance_factor=8,     # Default was 8
                         community_author_graph_edge_color='lightgray',
                         community_author_graph_font_size=5,                # Default was 5
                         community_json_output=community_json_output,
                         community_line_graphs_filename_prefix='main',
                         community_csv_output=False,
                         community_line_graphs=True)

def analyze_kth(datastore, automated=True, community_json_output=False):
    analyzer = Analyzer(database=datastore.database,
                        automated=automated,
                        start_year=1945,
                        end_year=2021,
                        keyword="",
                        cmt_rename_list_file="kth_communities_rename_list.csv",
                        community_size_threshold=20,
                        sub_community_size_threshold_divider=2.5)
    analyzer.analyze_all(community_graph_filename='kth_community_graph.png',
                         community_graph_node_size_factor=1800,
                         community_graph_edge_width_factor=200,
                         community_graph_node_distance_factor=100,
                         community_graph_font_size=6,
                         main_author_graph=True,
                         main_author_graph_show_edges=True,
                         main_author_graph_min_node_size=0.5,
                         main_author_graph_node_size_factor=1.5,
                         main_author_graph_citation_threshold=0,
                         main_author_graph_label_citation_threshold=1,
                         main_author_graph_node_distance_factor=10,
                         main_author_graph_font_size=2.5,
                         main_author_graph_edge_color='lightgray',
                         main_author_graph_filename='kth_author_graph.png',
                         community_author_graphs=False,
                         community_author_graph_show_edges=True,
                         community_author_graph_min_node_size=0.1,
                         community_author_graph_node_size_factor=2.0,
                         community_author_graph_label_citation_threshold=1,
                         community_author_graph_node_distance_factor=15,
                         community_author_graph_edge_color='lightgray',
                         community_author_graph_font_size=5,
                         community_json_output=community_json_output,
                         community_line_graphs_filename_prefix='kth',
                         community_csv_output=False,
                         community_line_graphs=False)

def analyze_swe(datastore, automated=True, community_json_output=False):
    analyzer = Analyzer(database=datastore.database,
                        automated=automated,
                        start_year=1945,
                        end_year=2021,
                        keyword="",
                        cmt_rename_list_file="swe_communities_rename_list.csv",
                        community_size_threshold=20,
                         sub_community_size_threshold_divider=2.5)
    analyzer.analyze_all(community_graph_filename='swe_community_graph.png',
                         community_graph_node_size_factor=1800,
                         community_graph_edge_width_factor=200,
                         community_graph_node_distance_factor=150,
                         community_graph_font_size=6,
                         main_author_graph=True,
                         main_author_graph_show_edges=True,
                         main_author_graph_min_node_size=0.5,
                         main_author_graph_node_size_factor=1,
                         main_author_graph_citation_threshold=0,
                         main_author_graph_label_citation_threshold=1,
                         main_author_graph_node_distance_factor=8,
                         main_author_graph_font_size=1.5,
                         main_author_graph_edge_color='lightgray',
                         main_author_graph_filename='swe_author_graph.png',
                         main_author_graph_country_filter="Sweden",
                         community_author_graphs=False,
                         community_author_graph_show_edges=True,
                         community_author_graph_min_node_size=0.1,
                         community_author_graph_node_size_factor=2.0,
                         community_author_graph_label_citation_threshold=1,
                         community_author_graph_node_distance_factor=15,
                         community_author_graph_edge_color='lightgray',
                         community_author_graph_font_size=5,
                         community_json_output=community_json_output,
                         community_line_graphs_filename_prefix='swe')

def analyze_ag(datastore, automated=True, community_json_output=False, randomize=False):
    analyzer = Analyzer(database=datastore.database,
                        automated=automated,
                        randomize=randomize,
                        start_year=1945,
                        end_year=2021,
                        keyword="",
                        cmt_rename_list_file="ag_communities_rename_list.csv",
                        community_size_threshold=15,
                         sub_community_size_threshold_divider=2.5)
    analyzer.analyze_all(community_graph_filename='ag_community_graph.png',
                         community_graph_node_size_factor=1800,
                         community_graph_edge_width_factor=180,
                         community_graph_node_distance_factor=300,
                         community_graph_font_size=6,
                         main_author_graph=True,
                         main_author_graph_show_edges=True,
                         main_author_graph_min_node_size=0.5,
                         main_author_graph_node_size_factor=1,
                         main_author_graph_citation_threshold=0,
                         main_author_graph_label_citation_threshold=1,
                         main_author_graph_node_distance_factor=10,
                         main_author_graph_font_size=1.5,
                         main_author_graph_edge_color='lightgray',
                         main_author_graph_filename='ag_author_graph.png',
                         community_author_graphs=False,
                         community_author_graph_show_edges=True,
                         community_author_graph_min_node_size=0.1,
                         community_author_graph_node_size_factor=2.0,
                         community_author_graph_label_citation_threshold=1,
                         community_author_graph_node_distance_factor=15,
                         community_author_graph_edge_color='lightgray',
                         community_author_graph_font_size=5,
                         community_json_output=community_json_output,
                         community_line_graphs_filename_prefix='ag')

def analyze_mlai(datastore, automated=True, community_json_output=False, randomize=False):
    analyzer = Analyzer(database=datastore.database,
                        automated=automated,
                        randomize=randomize,
                        start_year=1945,
                        end_year=2021,
                        keyword="",
                        cmt_rename_list_file="mlai_communities_rename_list.csv",
                        community_size_threshold=300,
                        sub_community_size_threshold_divider=2.5)
    analyzer.analyze_all(community_graph_filename='mlai_community_graph.png',
                         community_graph_node_size_factor=1800,
                         community_graph_edge_width_factor=200,
                         community_graph_node_distance_factor=100,
                         community_graph_font_size=6,
                         main_author_graph=True,
                         main_author_graph_show_edges=True,
                         main_author_graph_min_node_size=0.5,
                         main_author_graph_node_size_factor=1,
                         main_author_graph_citation_threshold=10,
                         main_author_graph_label_citation_threshold=100,
                         main_author_graph_node_distance_factor=15,
                         main_author_graph_font_size=1.5,
                         main_author_graph_edge_color='lightgray',
                         main_author_graph_filename='mlai_author_graph.png',
                         community_author_graphs=False,
                         community_author_graph_show_edges=True,
                         community_author_graph_min_node_size=0.1,
                         community_author_graph_node_size_factor=2.0,
                         community_author_graph_label_citation_threshold=1,
                         community_author_graph_node_distance_factor=15,
                         community_author_graph_edge_color='lightgray',
                         community_author_graph_font_size=5,
                         community_json_output=community_json_output,
                         community_line_graphs_filename_prefix='mlai')

def analyze_pentest(datastore, automated=True, community_json_output=False, randomize=False, detailed_global_analysis=False, sub_com_analysis=False):
    analyzer = Analyzer(database=datastore.database,
                        automated=automated,
                        randomize=randomize,
                        detailed_global_analysis=detailed_global_analysis,
                        sub_com_analysis=sub_com_analysis,
                        start_year=1945,
                        end_year=2021,
                        keyword="",
                        cmt_rename_list_file="pentest_communities_rename_list.csv",
                        excluded_communities_list_file="pentest_excluded_communities_list.csv",
                        community_size_threshold=750,
                        sub_community_size_threshold_divider=10)
    analyzer.analyze_all(community_graph_filename='pentest_community_graph.png',
                         community_graph_node_size_factor=1800,
                         community_graph_edge_width_factor=150,
                         community_graph_node_distance_factor=1000,
                         community_graph_font_size=6,
                         main_author_graph=True,
                         main_author_graph_show_edges=True,
                         main_author_graph_min_node_size=0.5,
                         main_author_graph_node_size_factor=1,
                         main_author_graph_citation_threshold=10,
                         main_author_graph_label_citation_threshold=100,
                         main_author_graph_node_distance_factor=15,
                         main_author_graph_font_size=1.5,
                         main_author_graph_edge_color='lightgray',
                         main_author_graph_filename='pentest_author_graph.png',
                         community_author_graphs=False,
                         community_author_graph_show_edges=True,
                         community_author_graph_min_node_size=0.1,
                         community_author_graph_node_size_factor=2.0,
                         community_author_graph_label_citation_threshold=1,
                         community_author_graph_node_distance_factor=15,
                         community_author_graph_edge_color='lightgray',
                         community_author_graph_font_size=5,
                         community_json_output=community_json_output,
                         community_line_graphs_filename_prefix='pentest')

# Actual code starts here.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_KEY_PATH

if len(sys.argv) == 1:
    print("This program can scrape Scopus in order to retrieve and store data about articles on the topics of information and cyber security.")
    print("It can also analyse the retrieved data, presenting which are the research communitites, what they are about, and how they relate to each other.")
    print("----------------------- >>> Options Menu <<< -----------------------")
    print("# Main Analysis (Global):")
    print(" 1. Retrieve data from the Scopus database. (FYI: A comprehensive retrieval may take really long time, like days or even weeks)")
    print(" 2. Retrieve affiliation names from the Scopus database.")
    print(" 3. Analyse the top level communitites based on previously retrieved data!")
    print(" 4. Try to scrape all highly cited but not fully scraped articles. (i.e. the ones with a * on anaylsis)")
    print(" 5. Perform database maintenance. (removal of duplicates, update author articles and article citiations, etc.)")
    print(" 6. Perform sensitivity analysis on the analysis results. (i.e. peform X runs with randomization and save the results)")
    print(" 7. Check and scrape any missing authors and/or keywords from the database. (Use this to recover from incomplete store to Datastore)")
    print("# Secondary Analyses:")
    print(" 8.  Retrieve all KTH articles from Scopus!")
    print(" 9.  Analyse KTH communities based on previously retrieved data!")
    print(" 10. Retrieve all Sweden affiliated articles from Scopus!")
    print(" 11. Analyse Swedish communities based on previously retrieved data!")
    print(" 12. Retrieve all attack/threat modeling, graphs and simulation related articles from Scopus!")
    print(" 13. Analyse attack/threat modeling, graphs and simulation communities based on previously retrieved data!")
    print(" 14. Retrieve all cyber security AND ML OR AI OR deep learning OR neural network related articles from Scopus!")
    print(" 15. Analyse the research communities based on previously (option 14) retrieved data!")
    print(" 16. Retrieve all pentesting related articles from Scopus!")
    print(" 17. Analyse the pentesting research communities based on previously (option 16) retrieved data!")
    print(" 0.  Exit")
    selection = eval(input())
    if selection == 1:
        retrieve_from_scopus()
    elif selection == 2:
        retrieve_affiliations_from_scopus(datastore_default_kind=False, datastore_kind_suffix="complete")
    elif selection == 3:
        if PRINT_TO_FILE:
            print("INFO: From now on all the prints will be written to a file...")
            sys.stdout = open('analysis_global_out.txt', 'w')
            print_versions()
            print("INFO: Start of output")
        # start_year_filter = "2016-10-17"
        # end_year_filter = "2019-12-30"
        # randomize = False
        start_year_filter = None
        end_year_filter = None
        randomize = 2335927275
        storage = initialize_storage(datastore_default_kind=False, datastore_kind_suffix="complete", start_year_filter=start_year_filter, end_year_filter=end_year_filter)
        analyze(storage, randomize=randomize, community_json_output=False, detailed_global_analysis=True, sub_com_analysis=True, sub2_com_analysis=False, start_year=start_year_filter, end_year=end_year_filter)
    elif selection == 4:
        if PRINT_TO_FILE:
            print("INFO: From now on all the prints will be written to a file...")
            sys.stdout = open('rescraping_out.txt', 'w')
            print("INFO: Start of output")
        scraper = Scraper(API_KEY)
        signal.signal(signal.SIGINT, partial(scraper_signal_handler, scraper))
        scraper.scrape_highly_cited_not_scraped_articles()
    elif selection == 5:
        storage = CloudStorage()
        storage.database.ensure_references_are_in_database()
        storage.database.identify_description_duplicates() # This identifies and removes duplicate articles
        storage.database.update_author_article_records()
        storage.database.ensure_authors_not_duplicated()
        storage.database.update_article_citation_records()
        # storage.store()
    elif selection == 6:
        storage = initialize_storage()
        offset = 0
        for i in range(SENS_ANAL_RUNS):
            random_seed = np.random.mtrand._rand.randint(0,2**32)
            sys.stdout = sys.__stdout__
            print("INFO: Run #" + str(i+offset) + ": From now on all the prints will be written to a file...")
            sys.stdout = open('output_run' + str(i+offset) + '.txt', 'w')
            print("INFO: Run #" + str(i+offset) + ": Start of output")
            analyze(storage, randomize=random_seed, community_json_output='output_run' + str(i+offset) + '.json', modularity_threshold=None)
    elif selection == 7:
        # if PRINT_TO_FILE:
        #     print("INFO: From now on all the prints will be written on a file...")
        #     sys.stdout = open('rescraping_out.txt', 'w')
        #     print("INFO: Start of output")
        scraper = Scraper(API_KEY)
        signal.signal(signal.SIGINT, partial(scraper_signal_handler, scraper))
        scraper.scrape_complete_incomplete_articles()
    elif selection == 8:
        retrieve_kth_only_from_scopus()
    elif selection == 9:
        if PRINT_TO_FILE:
            print("INFO: From now on all the prints will be written to a file...")
            sys.stdout = open('analysis_kth_out.txt', 'w')
            print_versions()
            print("INFO: Start of output")
        storage = initialize_storage(datastore_default_kind=False, datastore_kind_suffix='kth')
        analyze_kth(storage)
    elif selection == 10:
        retrieve_swedish_only_from_scopus()
    elif selection == 11:
        if PRINT_TO_FILE:
            print("INFO: From now on all the prints will be written to a file...")
            sys.stdout = open('analysis_swe_out.txt', 'w')
            print_versions()
            print("INFO: Start of output")
        storage = initialize_storage(datastore_default_kind=False, datastore_kind_suffix='swe')
        analyze_swe(storage)
    elif selection == 12:
        retrieve_ag_from_scopus()
    elif selection == 13:
        if PRINT_TO_FILE:
            print("INFO: From now on all the prints will be written to a file...")
            sys.stdout = open('analysis_AG_out.txt', 'w')
            print_versions()
            print("INFO: Start of output")
        storage = initialize_storage(datastore_default_kind=False, datastore_kind_suffix='ag')
        randomize = 4198446041
        analyze_ag(storage, randomize=randomize)
    elif selection == 14:
        retrieve_mlai_from_scopus()
    elif selection == 15:
        if PRINT_TO_FILE:
            print("INFO: From now on all the prints will be written to a file...")
            sys.stdout = open('analysis_ML_AI_out.txt', 'w')
            print_versions()
            print("INFO: Start of output")
        storage = initialize_storage(datastore_default_kind=False, datastore_kind_suffix='mlai', start_year_filter=None, end_year_filter=None)
        # randomize = 4198446041
        analyze_mlai(storage)
    elif selection == 16:
        retrieve_pentest_from_scopus(query_to_use=pentest_query)
    elif selection == 17:
        if PRINT_TO_FILE:
            print("INFO: From now on all the prints will be written to a file...")
            sys.stdout = open('analysis_pentest_out.txt', 'w')
            print_versions()
            print("INFO: Start of output")
        storage = initialize_storage(datastore_default_kind=False, datastore_kind_suffix='pentest', start_year_filter=None, end_year_filter=None)
        analyze_pentest(storage, detailed_global_analysis=True, sub_com_analysis=True)
    elif selection == 99:
        # if PRINT_TO_FILE:
        #     print("INFO: From now on all the prints will be written on a file...")
        #     sys.stdout = open('debug_out.txt', 'w')
        #     print("INFO: Start of debug output")
        print(">>> Scraping only one article <<<")
        scraper = Scraper(API_KEY)
        article_to_scrape = Article('2-s2.0-84974601983')
        # data = scraper.scrape_json_from_eid(article_to_scrape)
        scraper.complete_article_from_eid(article_to_scrape, assume_in_scope=True)
        sys.exit(0)
    elif selection == 0:
        print("Exiting...")
        exit()
    else:
        print("Error: Invalid option!\nExiting...")
        exit()
