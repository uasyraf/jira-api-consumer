from datetime import datetime
from requests.auth import HTTPBasicAuth

import requests


class JiraAPIConsumer:
    """
    A class for consuming the JIRA API and performing various operations.

    Args:
        jira_instance (str): The URL of the JIRA instance.
        email (str): The email address of the user.
        api_token (str): The API token for authentication.

    Attributes:
        jira_instance (str): The URL of the JIRA instance.
        auth (requests.auth.HTTPBasicAuth): The authentication object for API requests.
        headers (dict): The headers to be included in API requests.

    Methods:
        get_issues_updated_after(date: str) -> dict:
            Query JIRA for issues updated after a specific date.

        get_projects_updated_after(date: str) -> dict:
            Query JIRA for projects updated after a specific date.

        get_issues_filtered_by_users_updated_after(date: str) -> dict:
            Query JIRA for issues updated after a specific date and return the assignees, reporters, approvals, voters, and watchers.

        create_bulk_issues(issues_data: list[dict]) -> dict:
            Creates multiple issues in Jira.

        create_bulk_issues_from_dataframe(input_dataframe, project_key: str) -> dict:
            Creates and returns multiple issues in Jira based on the data in the input dataframe.
    """

    def __init__(self, jira_instance: str, email: str, api_token: str) -> None:
        self.jira_instance = jira_instance
        self.auth = HTTPBasicAuth(email, api_token)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get_issues_updated_after(self, date: str) -> dict:
        """
        Query JIRA for issues updated after a specific date.

        :param date: The date to filter issues by their last updated timestamp.
        :return: A dictionary containing the query results.
        """
        jql_query = f"updated >= '{date}'"
        url = f"{self.jira_instance}/rest/api/3/search?jql={jql_query}"
        response = requests.get(url, headers=self.headers, auth=self.auth)

        return response.json()

    def get_projects_updated_after(self, date: str) -> dict:
        """
        Query JIRA for projects updated after a specific date.

        :param date: The date to filter projects by their last updated timestamp.
        :return: A dictionary containing the query results.
        """
        jql_query = (
            f"project in projectsWhereUserHasPermission('Edit Issues') AND updated >= '{date}'"
        )
        url = f"{self.jira_instance}/rest/api/3/search?jql={jql_query}"
        response = requests.get(url, headers=self.headers, auth=self.auth)

        return response.json()
        
    def get_issues_filtered_by_users_updated_after(self, date: str) -> dict:
        """
        Query JIRA for issues updated after a specific date and return the assignees or reporters or approvals or voters or watchers.
    
        :param date: The date to filter issues by their last updated timestamp.
        :return: A dictionary containing the query results.
        """
        jql_query = f"updated >= '{date}'"
        url = f"{self.jira_instance}/rest/api/3/search"
        params = {
            'jql': jql_query,
            'fields': 'assignee,reporter,approvals,voter,watcher,updated,',  # Adjust fields as needed
            'maxResults': '1000'  # Adjust pagination as needed
        }
        response = requests.get(url, headers=self.headers, auth=self.auth, params=params)

        return response.json()

    def create_bulk_issues(self, issues_data: list[dict]) -> dict:
        """
        Creates multiple issues in Jira.

        Args:
            issues_data (list[dict]): A list of dictionaries containing the data for each issue.

        Returns:
            dict: A dictionary containing the response from the Jira API.

        Raises:
            None
        """
        url = f"{self.jira_instance}/rest/api/3/issue/bulk"
        response = requests.post(
            url,
            json={"issueUpdates": issues_data},
            headers=self.headers,
            auth=self.auth,
        )

        return response.json()

    def create_bulk_issues_from_dataframe(self, input_dataframe, project_key: str) -> dict:
        """
        Creates and returns multiple issues in Jira based on the data in the input dataframe.

        Args:
            input_dataframe (pandas.DataFrame): The dataframe containing the data for creating the issues.
            project_key (str): The key of the Jira project where the issues will be created.

        Returns:
            dict: A dictionary containing the response from the Jira API after creating the bulk issues.
        """
        bulk_issues = []
        for _, row in input_dataframe.iterrows():
            issue_data = self._create_issue_data(row, project_key)
            bulk_issues.append(issue_data)

        return self.create_bulk_issues(bulk_issues)

    @staticmethod
    def _create_issue_data(row: dict, project_key: str) -> dict:
        """
        Generate the issue data for creating a new issue in Jira.

        Args:
            row (dict): A dictionary containing the row data.
            project_key (str): The key of the project in Jira.

        Returns:
            dict: The issue data in the required format for creating a new issue in Jira.
        """
        return {
            "fields": {
                "project": {"key": project_key},
                "summary": row["summary"],
                "description": {
                    "content": [
                        {
                            "content": [
                                {
                                    "text": row["description"],
                                    "type": "text",
                                }
                            ],
                            "type": "paragraph",
                        }
                    ],
                    "type": "doc",
                    "version": 1,
                },
                "issuetype": {"name": row["issuetype"]},
            }
        }
