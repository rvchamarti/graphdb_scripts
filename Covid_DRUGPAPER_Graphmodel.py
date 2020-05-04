import os
import json
import warnings
import re
warnings.simplefilter('ignore')

import spacy
from neo4j import GraphDatabase, basic_auth


######## Add node to graphdb
def insert_drug_ent(ent):
    
#    print("Adding new Drug Ent %s" % ent.text)
    with graphDB_Driver.session() as grpDB_Ses:
       #### Verify that the drug does not exist first
       cqlqry = "MATCH(x:DRUG {name:'"+ent.text+"'}) return x"
       nodes = grpDB_Ses.run(cqlqry)
       len = 0
       for n in nodes:
            len = len + 1
       if (len == 0):
            ##### DRUG entity does not exist in db
            cqlqry = "CREATE (d:DRUG {name:'"+ent.text+"'})"
            grpDB_Ses.run(cqlqry)
                      
#### Enter Paper entity            
def insert_paper_ent(pjson):

    sre = pjson['metadata']['title']
    ### Remove any special characters in the title 
    filter_title = re.sub('[^A-Za-z0-9]+', '', sre)
    with graphDB_Driver.session() as grpDB_Ses:
        cqlqry = "MATCH (x:PAPER {id:'"+pjson["paper_id"]+"'}) return x"
        nodes = grpDB_Ses.run(cqlqry)
        len = 0
        for n in nodes:
            len = len + 1
            
        if (len == 0):
           cqlqry = "CREATE (p:PAPER {id:'"+pjson["paper_id"]+"', title:'"+filter_title+"'} )"  
           grpDB_Ses.run(cqlqry)
#        else:
#          print("Node is present")
#           for n in nodes:
#               print(n)
        
                      
def  add_drug_paper_relate(paper, drug):
    print("Drug "+drug.text+" and Paper "+paper['paper_id']+" are related")
    with graphDB_Driver.session() as grpDB_Ses:
        #### Check to see if there is a relationship exists         
        cqledgeqry = "MATCH (d:DRUG {name:'"+drug.text+"'})-[r]-(p:PAPER {id:'"+paper["paper_id"]+"'}) return TYPE(r), PROPERTIES(r)"        
        nodes = grpDB_Ses.run(cqledgeqry)
        
        len = 0
        rcount = 0
        for n in nodes:
            len = len + 1;
            r = n["PROPERTIES(r)"]
            t = n["TYPE(r)"]

            ### Get count if the relationship exists
            for k in r.keys():
               kv = r.get(k)
               rcount = kv 
            
        if (len == 0):
          #### print("Node relationship  is not present. Create one")
          cqlrelins =  "MATCH (d:DRUG), (p:PAPER) WHERE d.name = '"+drug.text+"' AND p.id = '"+paper['paper_id']+"' CREATE (d)-[:REFERRED_IN {count: 1}]->(p)"
          #print("Associate Relationship:")
          #print(cqlrelins)
          grpDB_Ses.run(cqlrelins)   
        else:
            ###print("Node relationship with count:"+str(rcount))            
            cqlrelins = "MATCH (x:DRUG {name:'"+drug.text+"'})-[r]->(y:PAPER {id:'"+paper['paper_id']+"'}) SET r.count="+str(rcount+1)+" RETURN r.count" 
            print("Update qry :"+cqlrelins)
            grpDB_Ses.run(cqlrelins)   
    
    
def is_empty(any_structure):
    if any_structure:
        #print('Structure is not empty.')
        return False
    else:
        #print('Structure is empty.')
        return True

# Database Credentials
#uri = "bolt://xx.xx.xx.xx ### Set the IP Address
userName = "neo4j"
password = os.getenv("NEO4J_PASSWORD")


# Connect to the neo4j database server
graphDB_Driver = GraphDatabase.driver(uri, auth=(userName, password))


nlp = spacy.load("en_core_med7_lg")

# decide which text corpus to choose for analysis
JSON_PATH = './CORD-19-research-challenge/biorxiv_medrxiv/'


# fixed path
json_files = [pos_json for pos_json in os.listdir(JSON_PATH) if pos_json.endswith('.json')]

#print(json_files)

# initialize entities dict - drugs
drugs = {}

# loop through the files
for jfile in json_files[::]:
    # for each file open it and read as json
    with open(os.path.join(JSON_PATH, jfile)) as json_file:
        covid_json = json.load(json_file)
        # read paper id
        ### Add node
        insert_paper_ent(covid_json)
        for item in covid_json['abstract']:
            text = item['text']
            doc = nlp(text)
            ### Search for drug in the paper
            for ent in doc.ents:
                if ent.label_ == "DRUG":
                    
                    #Add Drug node to DB
                    insert_drug_ent(ent)
                    # if drug exists increment, else add
                    if ent.text in drugs.keys():
                        drugs[ent.text] += 1
                        add_drug_paper_relate(covid_json, ent)
                    else:
                        drugs[ent.text] = 1
                        add_drug_paper_relate(covid_json, ent)

print("Operation completed")

