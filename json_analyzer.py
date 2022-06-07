# This is an experimental post-processing tool that is applied on the generated .json analysis files and attempts to calculate the stability of the communities.
# The following algorithm and metric is used for calculating stability:
# i) generate N samples (i.e. N runs of the analysis with different random seeds that result in different graph modularities), 
# ii) for each community, compute the average n_intersection/n_union over all the sample combinations, where n_intersection is the number of authors present in both samples (of one combination), and n_union is the total number of authors in the two samples (of one combination) 
# The average n_intersection/n_union can be exhaustively computed by calculating the ratio for all pairs of samples.

import json
import numpy as np
import sys
import itertools

SILENT_LOADING = True
SKIP_ANALYSIS = False

samples = [None] * 100
modularities = []
id_to_iter = {"authentication": 0, "smart card": 0, "image encryptions": 0, "malwares": 1, "android": 1, "random oracle model": 2, "cryptography": 2, "encryption schemes": 2, "phishing": 3, "user authentication": 3, "protection motivation theory": 3, "security requirements": 3, "intrusion detection": 4, "passwords": 4, "steganography": 5, "sensor networks": 6, "traffic analysis": 6, "internet of things": 6, "internet of things (iot)": 6, "privacy": 6, "block ciphers": 7, "differential power analysis": 7, "physical unclonable functions": 7, "biometrics": 8, "quantum cryptography": 9, "physical layer security": 9, "access control": 10, "smart grid": 11, "grid computing": 12, "content distribution": 13, "security protocols": 14, "cloud computing": 15, "regular expressions": 16, "information flow": 16}
UNIQUE_COMMUNITIES = 17
combinations = list(itertools.combinations(list(range(0, 100)),2)) # First combination is combinations[0][0] and combinations[0][1]
num_of_combs = len(combinations)

unique_community_names = []
# First load all the data!
for j in range(100):
    filename = "output_run" + str(j) + ".json" # Also change this to the correct name
    community_authors = [[] for i in range(UNIQUE_COMMUNITIES)]
    if not SILENT_LOADING:
        print("# Filename: " + filename)
    with open(filename) as json_file:
        data = json.load(json_file)
        modularities.append(data['modularity'])
        for p in data['communities']:
            if not SILENT_LOADING:
                print('Name: ' + p['community_name'])
                print('id: ' + p['community_id'])
                print('Members: ' + p['member_count'])
            if p['community_name'] not in unique_community_names:
                unique_community_names.append(p['community_name'])
            i = id_to_iter.get(p['community_name']) # Uncomment this line for proper community matching, although manual
            if i is None:
                print("ERROR: KeyError when looking at community IDs. For community name = " + p['community_name'])
                continue
            for a in p['authors']:
                community_authors[i].append(a)
        samples[j] = community_authors
    j += 1
print("Finished loading!")
# print("Unique communtiy names: ", end='')
# print(unique_community_names)
# Now analyze the data
if not SKIP_ANALYSIS:
    print("### Starting further analysis...")
    n_unions = [[] for i in range(num_of_combs)] # the total number of authors in the two clusters
    n_intersections = [[] for i in range(num_of_combs)] # the number of authors present in both samples
    community_divs = [[] for i in range(num_of_combs)] # n_intersection/n_union 
    for i in range(num_of_combs):
        comb_samp_a = combinations[i][0]
        comb_samp_b = combinations[i][1] 
        samp_a = samples[comb_samp_a]
        samp_b = samples[comb_samp_b]
        n_intersection = 0
        for j in range(UNIQUE_COMMUNITIES):
            n_union = 0
            for k in samp_a[j]:
                if k in samp_b[j]:
                    n_union += 1
                if k in samp_b[0] or k in samp_b[1] or k in samp_b[2] or k in samp_b[3] or k in samp_b[4] or k in samp_b[5] or k in samp_b[6] or k in samp_b[7] or k in samp_b[8] or k in samp_b[9] or k in samp_b[10] + samp_b[11] + samp_b[12] + samp_b[13] + samp_b[14] +samp_b[15] + samp_b[16]:
                    n_intersection += 1
            n_unions[i].append(n_union)
            print("Sample combination (" +  str(comb_samp_a) +  "," + str(comb_samp_b) + ") " + "community #" + str(j) +  " n_union = " + str(n_union))
        n_intersections[i].append(n_intersection)
        # Now calculate the divisions
        for j in range(UNIQUE_COMMUNITIES):
            if n_unions[i][j] != 0:
                community_divs[i].append(n_intersections[i][0]/n_unions[i][j])
            else:
                community_divs[i].append(0)
        print("Sample combination (" +  str(comb_samp_a) +  "," + str(comb_samp_b) + ") " + "n_intersection = " + str(n_intersection))
    # print(n_unions)
    # print(n_intersections)
    # print(community_divs)
    community_final = [0] * UNIQUE_COMMUNITIES
    for l in range(len(community_divs)):
        for m in range(len(community_divs[l])):
            community_final[m] += community_divs[l][m]/len(community_divs)
    print("Communtiy averages printed below:")
    for i in range(len(community_final)):
        print("Community #" + str(i) +  " n_intersection/n_union = " + str(community_final[i]))
        
max_mod= max(modularities)
print("Maximum modularity = " + str(max_mod) + ". And was achieved on run(s): #", end='')
print([i for i, j in enumerate(modularities) if j == max_mod])
