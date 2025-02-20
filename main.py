import os
import pandas as pd
import data
import determine


def update_merged_faculty():
    # Get new merged data from API+Selenium
    new_merged_df = data.combine_api_and_selenium(return_df=True)
    # (Modify combine_api_and_selenium in data.py to accept an optional parameter to return final_df.)

    output_file = "merged_output.csv"
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        # Compare on a unique key (e.g., email)
        combined_df = pd.concat([existing_df, new_merged_df]).drop_duplicates(
            subset="email", keep="first"
        )
    else:
        combined_df = new_merged_df

    combined_df.to_csv(output_file, index=False)
    print(f"Updated merged faculty data saved to '{output_file}'.")
    return combined_df


def update_research_outputs():
    # Get new research outputs (from API for each person)
    new_research_df = data.fetch_and_process_research_outputs(return_df=True)
    # (Modify fetch_and_process_research_outputs in data.py to return df)

    output_file = "person_research_outputs.csv"
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        # Compare on a unique article identifier
        combined_df = pd.concat([existing_df, new_research_df]).drop_duplicates(
            subset="article_uuid", keep="first"
        )
    else:
        combined_df = new_research_df

    combined_df.to_csv(output_file, index=False)
    print(f"Updated research outputs saved to '{output_file}'.")
    return combined_df


def update_sdg_classifications(research_df):
    sdg_file = "person_research_outputs_with_sdg.csv"
    # If SDG classifications already exist, load them and note which articles have been processed.
    if os.path.exists(sdg_file):
        existing_sdg_df = pd.read_csv(sdg_file)
        processed_ids = set(existing_sdg_df["article_uuid"])
    else:
        existing_sdg_df = pd.DataFrame()
        processed_ids = set()

    # Identify new research articles that need SDG processing.
    new_rows = research_df[~research_df["article_uuid"].isin(processed_ids)]
    if new_rows.empty:
        print("No new research articles to classify for SDG relevance.")
        return

    print("Classifying SDG relevance for new research articles...")
    # Run the SDG classification LLM calls only on the new rows.
    new_rows = determine.classify_sdg_relevance(new_rows)
    new_rows = determine.determine_relevant_goals(new_rows)

    # Append the new classifications to the existing SDG file.
    if not existing_sdg_df.empty:
        updated_df = pd.concat([existing_sdg_df, new_rows], ignore_index=True)
    else:
        updated_df = new_rows

    updated_df.to_csv(sdg_file, index=False)
    print(f"Updated SDG classifications saved to '{sdg_file}'.")


def main():
    print("=== Incremental Update Pipeline ===")

    # Step 1: Update merged faculty data (only new faculty entries will be appended)
    merged_df = update_merged_faculty()

    # Step 2: Update research outputs (append only new articles)
    research_df = update_research_outputs()

    # Step 3: Update SDG classification (only process articles that are new)
    update_sdg_classifications(research_df)

    data.add_journal_rankings("person_research_outputs.csv", "journals.xlsx")
    # (Optionally, you can also update your journals.xlsx file using your existing logic.)
    print("=== Incremental Update Complete ===")


if __name__ == "__main__":
    main()
