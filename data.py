import requests
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# -------------------------------------------------------------------
# Configuration and API Key
# -------------------------------------------------------------------
API_KEY = os.getenv("EXPERTS_API_KEY")


# =========================
# Part 1: Combine API & Selenium Data
# =========================


def fetch_gies_uuids():
    """
    Fetches UUIDs for organisational units matching specific identifiers from the Experts API.
    Returns:
        A list of UUIDs.
    """
    url = "https://experts.illinois.edu/ws/api/524/organisational-units"
    headers = {"Accept": "application/json"}
    params = {
        "apiKey": API_KEY,
        "size": 1000,  # Assuming all items are returned in one call
    }

    gies_identifiers = {
        "gies-college-of-business",
        "college-of-business",
        "finance",
        "accountancy",
        "business-administration",
    }

    gies_uuids = []

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print(
            f"Failed to retrieve organisational unit data. Status code: {response.status_code}"
        )
    else:
        data = response.json()
        items = data.get("items", [])
        for item in items:
            pretty_identifiers = item.get("info", {}).get("prettyURLIdentifiers", [])
            if any(identifier in gies_identifiers for identifier in pretty_identifiers):
                gies_uuids.append(item["uuid"])
    return gies_uuids


def fetch_and_process_persons(filter_uuids):
    """
    Fetches person records using the Experts API, processes the results,
    and returns a DataFrame.
    """
    url = "https://experts.illinois.edu/ws/api/524/persons"

    # Define fields to include in the API response
    fields = (
        "uuid,externalId,name.firstName,name.lastName,"
        "staffOrganisationAssociations.organisationalUnit.name.text.value,"
        "profileInformations.value.text.value"
    )

    headers = {"Accept": "application/json"}
    json_body = {"forOrganisations": {"uuids": filter_uuids}}

    params = {
        "apiKey": API_KEY,
        "size": 500,  # Adjust based on the API's limits
        "fields": fields,
    }

    all_refined_info = []

    response = requests.post(url, headers=headers, params=params, json=json_body)
    if response.status_code == 200:
        data = response.json()
        items = data.get("items", [])
        for item in items:
            # Construct full name from first and last names
            full_name = f"{item.get('name', {}).get('firstName', '')} {item.get('name', {}).get('lastName', '')}".strip()

            # Extract organisational unit names (if needed)
            organisational_units = [
                ou.get("organisationalUnit", {})
                .get("name", {})
                .get("text", [{}])[0]
                .get("value", "N/A")
                for ou in item.get("staffOrganisationAssociations", [])
                if ou.get("organisationalUnit")
            ]

            # Extract research interests from profile information (if needed)
            research_interests = "N/A"
            profile_info = item.get("profileInformations", [])
            if profile_info:
                research_interests = (
                    profile_info[0]
                    .get("value", {})
                    .get("text", [{}])[0]
                    .get("value", "N/A")
                )

            person_info = {
                "uuid": item.get("uuid", "N/A"),
                "email": item.get("externalId", "N/A"),
                "name": full_name,
                "organization": (
                    organisational_units if organisational_units else ["N/A"]
                ),
                "about": research_interests,
            }
            all_refined_info.append(person_info)
    else:
        print(
            f"Failed to retrieve person data. Status code: {response.status_code}, Response: {response.text}"
        )

    df = pd.DataFrame(all_refined_info)
    df["organization"] = df["organization"].apply(lambda units: list(set(units)))
    # Handle duplicate names by appending a count if necessary
    name_counts = {}

    def update_name(name):
        if name in name_counts:
            name_counts[name] += 1
            return f"{name} {name_counts[name]}"
        else:
            name_counts[name] = 1
            return name

    df["name"] = df["name"].apply(update_name)
    df["organization"] = df["organization"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else x
    )
    df["about"] = df["about"].str.replace("<[^>]+>", "", regex=True)

    return df


def scrape_faculty_profiles(driver):
    """
    Uses Selenium and BeautifulSoup to scrape the faculty profiles page.
    Returns:
        A list of dictionaries with scraped data.
    """
    wait = WebDriverWait(driver, 10)

    # Wait for and set the pagination dropdown to show more results
    select_pagination = wait.until(
        EC.element_to_be_clickable((By.ID, "pagination-top"))
    )
    Select(select_pagination).select_by_value("999")

    # Wait for and set the display type to "list"
    select_display_type = wait.until(
        EC.element_to_be_clickable((By.ID, "display-type"))
    )
    Select(select_display_type).select_by_value("list")

    # Pause to allow the page to refresh with the new settings
    time.sleep(5)

    # Parse the page source with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find("table", class_="results")
    data_list = []

    if table:
        tbody = table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")
            # If header row is present, skip it (check if first row contains <th>)
            if rows and rows[0].find_all("th"):
                rows = rows[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue
                # Extract name and description from the first column
                name_department = cols[0].get_text(separator="|").split("|")
                name = name_department[0].strip()
                description = (
                    name_department[1].strip()
                    if len(name_department) > 1
                    else "Description not found"
                )
                # Extract department from the second column
                department = cols[1].get_text(strip=True)
                # Extract contact (email) from the fourth column
                contact = (
                    cols[3].find("a").get_text(strip=True)
                    if cols[3].find("a")
                    else "Contact not found"
                )
                # Extract URL from the first column
                url_link = (
                    cols[0].find("a")["href"] if cols[0].find("a") else "URL not found"
                )

                data_list.append(
                    {
                        "name": name,
                        "department": department,
                        "description": description,
                        "contact": contact,  # This field holds the email from the faculty page
                        "url": url_link,
                    }
                )
    return data_list


def scrape_faculty_profiles_selenium():
    """
    Initializes Selenium, navigates to the faculty profiles page,
    scrapes the data, and returns it as a pandas DataFrame.
    """
    options = webdriver.ChromeOptions()
    # Uncomment the following line to run Chrome in headless mode
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    url = "https://giesbusiness.illinois.edu/faculty-research/faculty-profiles#page-1"
    driver.get(url)

    data_list = scrape_faculty_profiles(driver)
    driver.quit()

    df = pd.DataFrame(data_list)
    return df


def combine_api_and_selenium(return_df=False):
    """
    Combines the API and Selenium data by merging on the email field.
    Produces a CSV file "merged_output.csv" with columns:
      - name (from Selenium)
      - department (from Selenium)
      - uuid (from API)
      - email (from API)

    If return_df is True, returns the final DataFrame instead of saving to CSV.
    """
    print("Fetching GIES organisational unit UUIDs...")
    uuids = fetch_gies_uuids()
    print("Retrieved UUIDs:", uuids)

    print("Fetching and processing person data from API...")
    df_api = fetch_and_process_persons(uuids)
    print("API data retrieved. Number of records:", len(df_api))

    print("Scraping faculty profiles from website...")
    df_selenium = scrape_faculty_profiles_selenium()
    print("Faculty profiles scraped. Number of records:", len(df_selenium))

    # Merge on email (API's "email" and Selenium's "contact")
    merged_df = pd.merge(
        df_selenium, df_api, left_on="contact", right_on="email", how="inner"
    )
    final_df = merged_df[["name_x", "department", "uuid", "email"]].copy()
    final_df.rename(columns={"name_x": "name"}, inplace=True)

    output_filename = "merged_output.csv"
    if return_df:
        return final_df
    else:
        final_df.to_csv(output_filename, index=False)
        print(f"Final merged data saved to '{output_filename}'.")


def fetch_research_outputs_for_person(person_uuid):
    """
    Given a person UUID, calls the /persons/{id}/research-outputs API endpoint
    to fetch all research outputs for that person.
    """
    url = f"https://experts.illinois.edu/ws/api/524/persons/{person_uuid}/research-outputs"
    headers = {"Accept": "application/json"}
    params = {
        "apiKey": API_KEY,
        "size": 1000,  # Adjust if needed
        "offset": 0,
        "fields": (
            "uuid,"
            "title.value,"
            "subTitle.value,"
            "publicationStatuses.publicationDate.year,"
            "electronicVersions.doi,"
            "abstract.text.value,"
            "journalAssociation.*"
        ),
    }
    all_outputs = []
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            all_outputs.extend(items)
            if len(items) < params["size"]:
                break
            params["offset"] += params["size"]
        else:
            print(
                f"Failed to retrieve research outputs for person {person_uuid}. Status code: {response.status_code}"
            )
            break
    return all_outputs


def process_research_outputs(outputs, person_uuid):
    """
    Processes a list of research output items and returns a list of dictionaries.
    """
    processed = []
    for item in outputs:
        journal_association = item.get("journalAssociation", {})
        journal_title = journal_association.get("title", {}).get("value", "N/A")
        journal_issn = journal_association.get("issn", {}).get("value", "N/A")
        processed_item = {
            "person_uuid": person_uuid,
            "article_uuid": item.get("uuid", ""),
            "title": item.get("title", {}).get("value", "No Title"),
            "subtitle": item.get("subTitle", {}).get("value", "N/A"),
            "publication_year": (
                item.get("publicationStatuses", [{}])[0]
                .get("publicationDate", {})
                .get("year", "N/A")
            ),
            "doi": next(
                (
                    ev.get("doi")
                    for ev in item.get("electronicVersions", [])
                    if ev.get("doi")
                ),
                "No DOI",
            ),
            "abstract": item.get("abstract", {})
            .get("text", [{}])[0]
            .get("value", "N/A"),
            "journal_title": journal_title,
            "journal_issn": journal_issn,
        }
        processed.append(processed_item)
    return processed


def fetch_and_process_research_outputs(return_df=False):
    """
    Reads the "merged_output.csv" produced in Part 1 to get person details.
    For each person, fetches and processes their research outputs.
    Then merges each research output with the corresponding person details.
    The final DataFrame is saved to "person_research_outputs.csv" unless return_df is True.
    """
    merged_file = "merged_output.csv"
    df_person = pd.read_csv(merged_file)
    df_person_info = df_person[
        ["uuid", "name", "email", "department"]
    ].drop_duplicates()

    person_ids = df_person_info["uuid"].unique()
    print(f"Found {len(person_ids)} unique person IDs for research output retrieval.")

    all_research_outputs = []
    for person_uuid in person_ids:
        print(f"Fetching research outputs for person {person_uuid} ...")
        outputs = fetch_research_outputs_for_person(person_uuid)
        processed_outputs = process_research_outputs(outputs, person_uuid)
        all_research_outputs.extend(processed_outputs)
        print(
            f"  Retrieved {len(processed_outputs)} research outputs for person {person_uuid}."
        )

    df_outputs = pd.DataFrame(all_research_outputs)
    print(f"Total research outputs fetched: {len(df_outputs)}")

    # Merge with person details
    df_final = pd.merge(
        df_outputs, df_person_info, left_on="person_uuid", right_on="uuid", how="left"
    )
    df_final.drop(columns=["uuid"], inplace=True)

    # Combine title and subtitle
    df_final["title"] = df_final.apply(
        lambda row: (
            f"{row['title']}: {row['subtitle']}"
            if row["subtitle"] != "N/A" and row["subtitle"].strip() != ""
            else row["title"]
        ),
        axis=1,
    )
    df_final.drop(columns=["subtitle"], inplace=True)

    final_columns = [
        "person_uuid",
        "name",
        "email",
        "department",
        "article_uuid",
        "title",
        "publication_year",
        "doi",
        "abstract",
        "journal_title",
        "journal_issn",
    ]
    df_final = df_final[final_columns]

    output_filename = "person_research_outputs.csv"
    if return_df:
        return df_final
    else:
        df_final.to_csv(output_filename, index=False)
        print(
            f"Final research outputs with person details saved to '{output_filename}'."
        )


# =========================
# Part 3: Update Journal Rankings
# =========================


def add_journal_rankings(person_research_file, journals_coded_file):
    """
    Merges journal ranking data with the person research outputs and updates
    the existing person_research_outputs.csv file (in place) rather than creating a new file.
    Also, updates the journals coded Excel file with any new journal titles.
    """
    df = pd.read_csv(person_research_file)

    journals_df = pd.read_excel(journals_coded_file, sheet_name="journals")

    # Merge on 'journal_title', adding the new ranking columns
    merged_df = df.merge(
        journals_df[
            ["journal_title", "Financial Times", "UT Dallas", "General Business"]
        ],
        on="journal_title",
        how="left",
    )

    # Fill NaN values with 0 for the ranking columns
    merged_df[["Financial Times", "UT Dallas", "General Business"]] = merged_df[
        ["Financial Times", "UT Dallas", "General Business"]
    ].fillna(0)

    # Identify journal titles in the research output that are not in the journals file
    unmatched_journals = set(df["journal_title"]) - set(journals_df["journal_title"])

    # If there are any new journals, create rows with default ranking values and append them
    if unmatched_journals:
        new_journal_rows = pd.DataFrame(
            {
                "journal_title": list(unmatched_journals),
                "count": [0] * len(unmatched_journals),
                "Financial Times": [0] * len(unmatched_journals),
                "UT Dallas": [0] * len(unmatched_journals),
                "General Business": [0] * len(unmatched_journals),
            }
        )
        updated_journals_df = pd.concat(
            [journals_df, new_journal_rows], ignore_index=True
        )
    else:
        updated_journals_df = journals_df

    merged_df.to_csv(person_research_file, index=False)

    with pd.ExcelWriter(journals_coded_file, engine="xlsxwriter") as writer:
        updated_journals_df.to_excel(writer, sheet_name="journals", index=False)

    print(f"Updated person research outputs saved at: {person_research_file}")
    print(f"Updated journals file saved at: {journals_coded_file}")


# =========================
# Main Pipeline Function
# =========================


def explore_data():
    print("===== Part 1: Combining API and Selenium Data =====")
    combine_api_and_selenium()

    print(
        "\n===== Part 2: Fetching Research Outputs and Merging with Person Details ====="
    )
    fetch_and_process_research_outputs()

    print("\n===== Part 3: Updating Journal Rankings =====")
    person_research_file = "person_research_outputs.csv"
    journals_coded_file = "journals.xlsx"
    add_journal_rankings(person_research_file, journals_coded_file)


if __name__ == "__main__":
    explore_data()
