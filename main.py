import requests
import json
import pandas as pd
import ast

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

import os

API_KEY = os.getenv("EXPERTS_API_KEY")

update = True

def fetch_and_process_articles(offset, output_filename, offset_filename):
    url = "https://experts.illinois.edu/ws/api/524/research-outputs"
    headers = {
        "Accept": "application/json",
    }

    fields = (
        "uuid,"
        "title.value,"
        "subTitle.value,"
        "publicationStatuses.publicationDate.year,"
        "personAssociations.person.uuid,"
        "personAssociations.person.name.text.value,"
        "electronicVersions.doi,"
        "abstract.text.value,"
        "journalAssociation.*,"
        "organisationalUnits.uuid,"
        "organisationalUnits.name.text.value"
    )
    
    json_body = {
    }

    all_refined_info = []
    params = {
        "apiKey": API_KEY,
        "size": 1000,
        "offset": offset,
        "fields": fields
    }
    while True:
        response = requests.post(url, headers=headers, params=params, json=json_body)
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            for item in items:
                journal_association = item.get("journalAssociation", {})
                journal_title = journal_association.get("title", {}).get("value", "N/A")
                journal_issn = journal_association.get("issn", {}).get("value", "N/A")

                article_info = {
                    "uuid": item.get("uuid"),
                    "title": item.get("title", {}).get("value", "No Title"),
                    "subtitle": item.get("subTitle", {}).get("value", "N/A"),
                    "publication_year": item.get("publicationStatuses", [{}])[0].get("publicationDate", {}).get("year", "N/A"),
                    "doi": next((ev.get("doi") for ev in item.get("electronicVersions", []) if ev.get("doi")), "No DOI"),
                    "authors": [],
                    "abstract": item.get("abstract", {}).get("text", [{}])[0].get("value", "N/A"),
                    "journal_title": journal_title,
                    "journal_issn": journal_issn
                }
                for pa in item.get("personAssociations", []):
                    if "person" in pa and "name" in pa["person"]:
                        name_texts = pa["person"]["name"].get("text", [])
                        if name_texts:
                            full_name = name_texts[0].get("value", "")
                            name_parts = full_name.strip().split()
                            if len(name_parts) >= 2:
                                first_name = name_parts[0]
                                last_name = " ".join(name_parts[1:])
                            else:
                                first_name = name_parts[0]
                                last_name = "Unknown"
                            author_name = f"{first_name} {last_name}"
                        else:
                            author_name = "Unknown"

                        author_uuid = pa["person"].get("uuid", "N/A")
                        article_info["authors"].append({"name": author_name, "uuid": author_uuid})

                all_refined_info.append(article_info)
            print(len(items) + params["offset"])
            if(len(items) < params["size"]):
                print("Finished new data retrieval")
                with open(offset_filename, "w") as file:
                    file.write(str(len(items) + params["offset"]))
                break
            params["offset"] += params["size"]
            #if(params["offset"] >= maxoffset):
            #    break
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}, Response content: {response.text}")
            break

    #filename = "research_data_filtered.json"
    #with open(filename, "w", encoding="utf-8") as f:
    #    json.dump(all_refined_info, f, indent=4)
    #print(f"Cleaned data saved to {filename}")

    #with open("research_data_new.json", "r") as file:
    #    data = json.load(file)

    df = pd.DataFrame(all_refined_info)

    if df.empty:
        print("No new data.")
        update = False
        return

    df["title"] = df.apply(lambda row: row["title"] + ": " + row["subtitle"] if row["subtitle"] != "N/A" else row["title"], axis=1)
    df["abstract"] = df["abstract"].str.replace("^<p>", "", regex=True).str.replace("</p>$", "", regex=True)
    df.drop(columns=["subtitle"], inplace=True)
    #df["keywords"] = df["keywords"].apply(lambda x: ", ".join(x))
    df.to_csv(output_filename, sep="\t", index=False)

def fetch_gies_uuids():
    url = "https://experts.illinois.edu/ws/api/524/organisational-units"
    headers = {"Accept": "application/json"}
    params = {
        "apiKey": API_KEY,
        "size": 1000,
        "offset": 0
    }

    gies_identifiers = {
        "gies-college-of-business",
        "college-of-business",
        "finance",
        "accountancy",
        "business-administration"
    }

    gies_uuids = []

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            break

        data = response.json()
        items = data.get("items", [])
        
        # Process each organisational unit
        for item in items:
            pretty_identifiers = item.get("info", {}).get("prettyURLIdentifiers", [])
            if any(identifier in gies_identifiers for identifier in pretty_identifiers):
                gies_uuids.append(item["uuid"])

        # Pagination handling
        params["offset"] += params["size"]
        if len(items) < params["size"]:
            break  # Exit when there are no more items to fetch

    return gies_uuids

def fetch_and_process_persons(offset, filter_uuids, output_filename, offset_filename):
    url = "https://experts.illinois.edu/ws/api/524/persons"

    # Adjusted fields parameter to include details explicitly
    fields = "uuid,externalId,name.firstName,name.lastName,staffOrganisationAssociations.organisationalUnit.name.text.value,profileInformations.value.text.value"

    # Headers to request a JSON response
    headers = {"Accept": "application/json"}

    # Json content of POST request
    json_body = {
        "forOrganisations": {
            "uuids": filter_uuids
        }
    }

    # Initialize the list to hold all refined person information
    all_refined_info = []

    # Initial parameters for pagination
    params = {
        "apiKey": API_KEY,
        "size": 500,  # Adjust as per the API"s limits and requirements
        "offset": offset,
        "fields": fields
    }
    while True:
        response = requests.post(url, headers=headers, params=params, json=json_body)
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])

            for item in items:
                # Construct full name and extract information
                full_name = f"{item.get('name', {}).get('firstName', '')} {item.get('name', {}).get('lastName', '')}".strip()
                organisational_units = [ou.get("organisationalUnit", {}).get("name", {}).get("text", [{}])[0].get("value", "N/A")
                                        for ou in item.get("staffOrganisationAssociations", [])
                                        if ou.get("organisationalUnit")]

                research_interests = "N/A"
                profile_info = item.get("profileInformations", [])
                if profile_info:
                    research_interests = profile_info[0].get("value", {}).get("text", [{}])[0].get("value", "N/A")

                person_info = {
                    "uuid": item.get("uuid", "N/A"),
                    "email": item.get("externalId", "N/A"),
                    "name": full_name,
                    "organization": organisational_units if organisational_units else ["N/A"],
                    "about": research_interests
                }

                all_refined_info.append(person_info)
            print(len(items) + params["offset"])
            if(len(items) < params["size"]):
                print("Finished new data retrieval")
                with open(offset_filename, "w") as file:
                    file.write(str(len(items) + params["offset"]))
                break
            params["offset"] += params["size"]
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}, Response content: {response.text}")
            break

    # Convert and save or print the refined person information
    #refined_json = json.dumps(all_refined_info, indent=4)
    #filename = "researchers.json"
    #with open(filename, "w") as f:
    #    f.write(refined_json)
    #print(f"Refined person data saved to {filename}")

    #file_path = "researchers.json"
    #with open(file_path, "r") as file:
    #    data = json.load(file)
    
    df = pd.DataFrame(all_refined_info)

    df["organization"] = df["organization"].apply(lambda units: list(set(units)))
    
    name_counts = {}
    
    # Take account on duplicate researchers
    def update_name(name):
        if name in name_counts:
            name_counts[name] += 1
            return f"{name} {name_counts[name]}"
        else:
            name_counts[name] = 1
            return name
    
    df["name"] = df["name"].apply(update_name)
    
    df["organization"] = df["organization"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    df["about"] = df["about"].str.replace("<[^>]+>", "", regex=True)
    df.to_csv(output_filename, sep="\t", index=False)

def process_and_merge_data(people_file, articles_file, output_file):
    # Load the data
    people_df = pd.read_csv(people_file, sep="\t")
    articles_df = pd.read_csv(articles_file, sep="\t")

    # Extract author UUIDs
    articles_df["author_uuids"] = articles_df["authors"].apply(
        lambda authors: [
            author["uuid"] for author in ast.literal_eval(authors) if "uuid" in author
        ]
        if isinstance(authors, str) and authors.startswith("[")
        else []
    )

    # Rename the original "uuid" column in articles_df to "article_id"
    articles_df = articles_df.rename(columns={"uuid": "article_id"})

    # Explode the articles_df to have one row per author uuid, renaming the exploded uuid to "people_id"
    articles_exploded_df = articles_df.explode("author_uuids").rename(columns={"author_uuids": "people_id"})

    # Rename the "uuid" column in people_df to "people_id" for consistency
    people_df = people_df.rename(columns={"uuid": "people_id"})

    # Merge the two dataframes based on "people_id"
    merged_df = pd.merge(articles_exploded_df, people_df, on="people_id", how="left")

    # Filter out entries where author uuids were not found
    filtered_df = merged_df.dropna(subset=["name"])

    # Drop unnecessary columns
    final_df = filtered_df.drop(columns=["people_id", "authors"])

    # Append to output file if it exists, otherwise create a new one
    if os.path.isfile(output_file):
        final_df.to_csv(output_file, sep="\t", index=False, mode="a", header=False)
    else:
        final_df.to_csv(output_file, sep="\t", index=False)

offset = 0
filename = "research_offset.txt"
if os.path.exists(filename):
    with open(filename, "r") as file:
        offset = int(file.read().strip())
    
#Import new GIES research from research-outputs with offset 0, saved to articles_final_new.tsv. store offset in research_offset.txt
fetch_and_process_articles(offset, "articles_update.tsv", "research_offset.txt")

if update:
    #Import new persons from /persons with offset 0, saved to people.tsv. store offset in research_offset.txt
    fetch_and_process_persons(0, fetch_gies_uuids(), "people.tsv", "researcher_offset.txt")

    process_and_merge_data("people.tsv", "articles_update.tsv", "final_data.tsv")
    final_df = pd.read_csv("final_data.tsv", sep="\t")
    print(len(final_df))