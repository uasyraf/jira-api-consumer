import pandas as pd
from typing import Tuple


import pandas as pd


def update_input_dataframe(
    input_dataframe: pd.DataFrame, api_response: dict
) -> pd.DataFrame:
    """
    Utility function to loop through the results in the main.py file and update the input_dataframe
    by adding load_success and load_result based on the result.

    Args:
        input_dataframe (pd.DataFrame): The input dataframe to be updated.
        api_response (dict): The API response containing errors and issues.

    Returns:
        pd.DataFrame: The updated input dataframe.

    Raises:
        None

    Example:
        >>> input_df = pd.DataFrame({'id': [1, 2, 3]})
        >>> response = {'errors': [], 'issues': [{'key': 'ABC-123'}, {'key': 'DEF-456'}]}
        >>> updated_df = update_input_dataframe(input_df, response)
        >>> print(updated_df)
           id  load_success load_result
        0   1          True     ABC-123
        1   2          True     DEF-456
        2   3         False  Unknown error
    """
    # Extract errors and issues from the response
    errors = {
        error["failedElementNumber"]: error for error in api_response.get("errors", [])
    }
    issues = iter(api_response.get("issues", []))

    # Iterate over the dataframe rows
    for index, _ in input_dataframe.iterrows():
        if index in errors:
            # Update the row based on error information
            error = errors[index]
            error_messages = "; ".join(
                [
                    f"{k}: {v}"
                    for k, v in error.get("elementErrors", {}).get("errors", {}).items()
                ]
            )
            input_dataframe.at[index, "load_success"] = False
            input_dataframe.at[index, "load_result"] = error_messages or "Unknown error"
        else:
            # Update the row based on issue information, if available
            try:
                issue = next(issues)
                input_dataframe.at[index, "load_success"] = True
                input_dataframe.at[index, "load_result"] = issue["key"]
            except StopIteration:
                # No more issues, mark remaining as not successful if needed
                break

    return input_dataframe


# utility to convert date str '2021-09-01T00:00:00.000+0000' to '2021-09-01'
def convert_date(date_str: str) -> str:
    return date_str[:10]


def extract_issues_from_response(response: dict) -> Tuple[list[dict], str]:
    """
    Extracts the list of issues and the latest updated date from the given response.

    Args:
        response (dict): The response received from the Jira API.

    Returns:
        Tuple[list[dict], str]: A tuple containing the list of issues and the latest updated date.
    """
    issues = response.get("issues", [])
    latest_updated_date = ""
    if issues:
        latest_updated_date = issues[-1].get("fields").get("updated")
        # loop through the issues
        for issue in issues:
            # compare the latest updated date
            if latest_updated_date < issue.get("fields").get("updated"):
                latest_updated_date = issue.get("fields").get("updated")

    return issues, latest_updated_date


def extract_projects_from_issues(issues: list[dict]) -> list[str]:
    """
    Extracts the project names from a list of issues.

    Args:
        issues (list[dict]): A list of dictionaries representing the issues.

    Returns:
        tuple: A tuple containing the list of project names and the latest updated date.

    """
    projects = []
    latest_updated_date = issues[-1].get("fields").get("updated")
    for issue in issues:
        if latest_updated_date < issue.get("fields").get("updated"):
            latest_updated_date = issue.get("fields").get("updated")
        project = issue.get("fields").get("project")
        if project:
            projects.append(project)
    return projects, latest_updated_date


def extract_users_from_issues(issues: list[dict]) -> list[dict]:
    """
    Extracts users from a list of issues.

    Args:
        issues (list[dict]): A list of dictionaries representing issues.

    Returns:
        tuple: A tuple containing the extracted users and the latest updated date.

    """
    users = []
    latest_updated_date = issues[-1].get('fields').get("updated")
    for issue in issues:
        if latest_updated_date < issue.get("fields").get("updated"):
            latest_updated_date = issue.get("fields").get("updated")
        for field in ["assignee", "reporter", "approvals", "voter", "watcher"]:
            user = issue.get("fields").get(field)
            if user:
                users.append(user)
    return users, latest_updated_date


def generate_output_dataframe(
    issues: list[dict], projects: list[str], users: list[dict]
) -> pd.DataFrame:
    """
    Generates an output dataframe from the given issues, projects, and users.

    Args:
        issues (list[dict]): A list of dictionaries representing issues.
        projects (list[str]): A list of project names.
        users (list[dict]): A list of dictionaries representing users.

    Returns:
        pd.DataFrame: A dataframe containing the object names and their respective data.

    Example:
        >>> issues = [{'key': 'ABC-123', 'summary': 'Issue 1'}, {'key': 'DEF-456', 'summary': 'Issue 2'}]
        >>> projects = ['Project 1', 'Project 2']
        >>> users = [{'name': 'User 1', 'email': 'test@mail.com'}, {'name': 'User 2', 'email': 'test2@mail.com'}]
        >>> output_df = generate_output_dataframe(issues, projects, users)
        >>> print(output_df)
        OBJECT_NAME | RECORD_DATA
        issues      | {'key': 'ABC-123', 'summary': 'Issue 1'}
        issues      | {'key': 'DEF-456', 'summary': 'Issue 2'}
        projects    | 'Project 1'
        projects    | 'Project 2'
        users       | {'name': 'User 1', 'email': 'test@mail.com}
        users       | {'name': 'User 2', 'email': 'test2@mail.com}
    """
    data = []
    for issue in issues:
        data.append(("issues", issue))
    for project in projects:
        data.append(("projects", project))
    for user in users:
        data.append(("users", user))

    return pd.DataFrame(data, columns=["OBJECT_NAME", "RECORD_DATA"])
