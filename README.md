<!-- This template uses html code to offer a bit prettier formatting. This html
code is limited to the header and footer. The main body is and should be written
in markdown. -->

<h1 align="center" position="relative">
  <br>
  <img src=".images/top.jpg" alt="Decorative image">
  <br>
  <br>
  <span>Research Communities Atlas</span>
  <br>

</h1>

<p align="justify">
  Research Communities Atlas (a.k.a. Security Atlas) is a tool that allows to peform a comprehensive and systematic literature review of the cybersecurity and information security domain (as well as other research domains, if configured properly) using the Scopus database as a data source, with goal to detect the research communities that are active throughout the years based on the author citations of the most cited articles.

  This tool allows to peform a comprehensive and systematic literature review of any research domain. The code allows scraping data from the Scopus database, storing them in GCP Datastore, and then running the community detection and analysis locally. 
<p>

## Quick Info
Research communities in cyber security project info:

- Members:
  - Sotirios Katsikeas
  - Pontus Johnson
  - Mathias Ekstedt
  - Robert Lagerström
- Status: completed :orange_circle:
- Timeline: 2019-2021

## Application on cyber security research 

Data regarding the entire cyber security and information security research domain were gathered from the Scopus database, and by automatically analyzing these data, a comprehensive literature review of the entire domain, that also focuses on the social relations of the authors, was conducted. The Louvain community detection algorithm was applied to the created author graph in order to identify existing research communities. The analysis, which was based on 59,782 articles, identified twelve communities: access control, authentication, biometrics, cryptography (I & II), cyber--physical systems, information hiding, intrusion detection, malwares, quantum cryptography, sensor networks, and usable security. The analysis results are presented for each community in descriptive text, sub-community graphs, and tables with, for example, the most-cited papers and authors.

The Python code for scraping data from Scopus database, storing them in GCP Datastore, and then running the community analysis locally, is found on this repository.

## Application on cyber security vulnerability assessments

In order to provide a coherent overview of vulnerability assessments and penetration tests, 537,629 related articles from 1975 to 2022 were scraped from the Scopus database. A Python script was used for data mining as well as analysis and 23,459 articles were included in the final synthesis. The articles were authored by 53,495 authors and produced an aggregated total of 836,956 citations. The Louvain community detection algorithm was used to create research communities within the area. In total, 16 research communities were identified: smart grids, attack graphs, security testing, software vulnerabilities, Internet of Things (IoT), network vulnerability, vulnerability analysis, Android, cascading failures, authentication, Software-Defined Networking (SDN), spoofing attacks, malware, trust models, and red teaming. Each community had several individual subcommunities, together constituting a total of 126 subcommunities. From the trends of the published studies analyzed, it is clear that research interest in penetration testing and vulnerability assessment is increasing.

Authors: Fredrik Heiding, Sotirios Katsikeas, Robert Lagerström

## Publications
The journal article for the research communities in cyber security can be found [here](https://www.sciencedirect.com/science/article/pii/S157401372100071X?via%3Dihub)

The journal article for the research communities in cyber security vulnerability assessments can be found [here](https://www.sciencedirect.com/science/article/pii/S1574013723000187?via%3Dihub)

## Instructions for using the code (e.g. for analyzing other domains)
To run this project some Python modules must first be installed. This can be done by running:

pip install -r requirements.txt

Then, make sure to update `atlas_config.py` with the correct path towards your Google Cloud key (you need that because all the data are stored on GCP Datastore) and your Scopus API key (you can get one from: https://dev.elsevier.com). Of course, that means that you also need a Google Cloud Project with a Datastore ("Cloud Firestore in Datastore mode"
) database set up.

Finally, you would need to either edit the `general_query` (on `main.py`), or create new functions (such as `retrieve_X_from_scopus` and `analyze_X`) for the domain you want to analyze. Be careful that if you opt for the simple alternative, which is to change the `general_query`, you would also need to change the contents of the `communities_rename_list.json` and `excluded_communities_list.csv` files.

<br>

  <a href="https://www.kth.se/nse/research/software-systems-architecture-and-security/" >
    <img src=".images/kth-round.png" alt="KTH logo" width=80 align="right" />
  </a>

- - - -
This is a project run by the [Software Systems Architecture and Security research
group](https://www.kth.se/nse/research/software-systems-architecture-and-security/)
within the [Division of Network and Systems Engineering](https://kth.se/nse) at
the Department of Computer Science at the School of [Electrical Engineering and
Computer Science](https://www.kth.se/en/eecs) @ [KTH university](https://www.kth.se).

For more of our projects, see the [SSAS page at github.com](https://github.com/KTH-SSAS).
