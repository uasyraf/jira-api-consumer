from dotenv import dotenv_values
from jira_api import JiraAPIConsumer
from utils import (
    update_input_dataframe,
    convert_date,
    extract_users_from_issues,
    extract_projects_from_issues,
    extract_issues_from_response,
    generate_output_dataframe,
)
from datetime import datetime

import argparse
import json
import pandas as pd

# Load the environment variables from the .env file
config = dotenv_values(".env")
jira_api_consumer = JiraAPIConsumer(
    config["MY_JIRA_INSTANCE"], config["MY_EMAIL"], config["MY_API_TOKEN"]
)


def create_issues_script():
    # the input Dataframe contains the 10 issues to be created
    input_dataframe: pd.DataFrame = pd.DataFrame(
        {
            "summary": [
                "Issue 1",
                "Issue 2",
                "Issue 3",
                "Issue 4",
                "Issue 5",
                "Issue 6",
                "Issue 7",
                "Issue 8",
                "Issue 9",
                "Invalid summary\ndue to newlines",
            ],
            "description": [
                "Description 1",
                "Description 2",
                "Description 3",
                "Description 4",
                "Description 5",
                "Description 6",
                "Description 7",
                "Description 8",
                "Description 9",
                "Description 10",
            ],
            "issuetype": [
                "Task",
                "Task",
                "Task",
                "Task",
                "Task",
                "Task",
                "Task",
                "Task",
                "Task",
                "Task",
            ],
        }
    )

    # Use the bulk issue method to create issues from the input_dataframe
    response = jira_api_consumer.create_bulk_issues_from_dataframe(
        input_dataframe, project_key=config["MY_PROJECT"]
    )
    input_dataframe = update_input_dataframe(input_dataframe, response)
    print(input_dataframe)


def query_objects_script():
    # Query the latest objects from JIRA
    objects_with_latest_updated_date = {
        "issues": "2021-09-01T00:00:00.000+0000",
        "projects": "2021-09-01T00:00:00.000+0000",
        "users": "2021-09-01T00:00:00.000+0000",
    }

    issues_response = jira_api_consumer.get_issues_updated_after(
        convert_date(objects_with_latest_updated_date["issues"])
    )

    # Query the latest projects from JIRA
    projects_response = jira_api_consumer.get_projects_updated_after(
        convert_date(objects_with_latest_updated_date["projects"])
    )

    # Query the latest users from JIRA
    filtered_issues_response = (
        jira_api_consumer.get_issues_filtered_by_users_updated_after(
            convert_date(objects_with_latest_updated_date["users"])
        )
    )

    # Extract the issues from the response
    issues, latest_updated_date = extract_issues_from_response(issues_response)
    objects_with_latest_updated_date["issues"] = latest_updated_date

    # Extract the projects from the issues response
    projects, latest_updated_date = extract_projects_from_issues(
        projects_response["issues"]
    )
    objects_with_latest_updated_date["projects"] = latest_updated_date

    # Extract the users from the issues response
    users, latest_updated_date = extract_users_from_issues(
        filtered_issues_response["issues"]
    )
    objects_with_latest_updated_date["users"] = latest_updated_date

    # Generate the output dataframe
    output_dataframe = generate_output_dataframe(issues, projects, users)
    
    # get the second column object for the first row and print it
    print(projects[0])

if __name__ == "__main__":
    # Setup argparse
    parser = argparse.ArgumentParser(description="Run Jira issue creation script.")
    parser.add_argument(
        "--create-issues",
        action="store_true",
        help="Enable bulk issue creation in JIRA",
    )
    parser.add_argument(
        "--query-objects",
        action="store_true",
        help="Query latest objects ['issues', 'projects', 'users'] from JIRA",
    )
    args = parser.parse_args()

    if args.create_issues:
        print("Creating issues...")
        create_issues_script()
    elif args.query_objects:
        print("Querying objects...")
        query_objects_script()
    else:
        print("No option such as that.")
