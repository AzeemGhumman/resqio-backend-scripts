#!/usr/bin/env python3

import json
import csv
import uuid
from io import StringIO
from datetime import datetime, timedelta
from jira import JIRA
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.queue import QueueServiceClient


def upload_csv_to_azure(azure_auth, business_id, csv_data, timestamp, agent_id):
    try:
        account_url = azure_auth["account_url"]
        storage_account_name = azure_auth["account_name"]
        account_key = azure_auth["account_key"]
        container_name = azure_auth["container_name"]

        credential = AzureNamedKeyCredential(storage_account_name, account_key)
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        container_client = blob_service_client.get_container_client(container_name)

        base_folder = f"{business_id}/interactions/incidents/jira"
        blob_name = f"{base_folder}/request-{business_id}-{agent_id}-{timestamp}.csv"

        blob_client = container_client.get_blob_client(blob_name)

        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        columns = [
            "channel_id",
            "title",
            "original_summary",
            "description",
            "entries",
            "resolved_by",
            "resolved_timestamp",
        ]
        writer.writerow(columns)

        for item in csv_data:
            writer.writerow([item[col] for col in columns])

        csv_content = csv_buffer.getvalue()

        blob_client.upload_blob(csv_content, overwrite=False)
        return {"success": True, "base_folder": base_folder}, 200

    except Exception as e:
        return {
            "error": (
                "Azure permissions issue"
                if "Authentication" in str(e)
                else "New file cannot be created in Azure"
            )
        }, 500


def fetch_jira_issues(jira_auth, total_days, max_issues):
    try:
        jira_url = jira_auth["jira_url"]
        jira_username = jira_auth["username"]
        jira_token = jira_auth["token"]

        jira = JIRA(server=jira_url, basic_auth=(jira_username, jira_token))

        end_date = datetime.now()
        start_date = end_date - timedelta(days=total_days)

        jql_query = (
            f"created >= '{start_date.strftime('%Y-%m-%d')}' ORDER BY created DESC"
        )

        issues = jira.search_issues(
            jql_str=jql_query, maxResults=min(max_issues, 100), expand="changelog"
        )

        if len(issues) == 0:
            return {"error": "No tickets found"}, 404

        issues_data = []

        for issue in issues:

            comments = issue.fields.comment.comments
            concatenated_comments = ""

            if len(comments) > 0:
                for i, comment in enumerate(comments, start=1):
                    concatenated_comments += f"# Entry {i}: {comment.body}\n"
            else:
                concatenated_comments = ""

            resolved_by = ""
            resolved_timestamp = ""
            is_status_found = False
            for history in issue.changelog.histories:
                for item in history.items:
                    if item.field == "status":
                        if item.toString.lower() == "done":
                            resolved_by = history.author.displayName
                            resolved_timestamp = history.created
                        is_status_found = True
                        break
                if is_status_found:
                    break

            if resolved_timestamp != "":
                resolved_timestamp = str(
                    int(
                        datetime.strptime(
                            resolved_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z"
                        ).timestamp()
                    )
                )

            issue_data = {
                "channel_id": "jira",
                "title": issue.fields.summary if issue.fields.summary else "",
                "original_summary": "",
                "description": (
                    issue.fields.description if issue.fields.description else ""
                ),
                "entries": concatenated_comments,
                "resolved_by": resolved_by,
                "resolved_timestamp": resolved_timestamp,
            }
            issues_data.append(issue_data)

        return issues_data, 200
    except Exception as e:
        return {"error": "Unable to login to JIRA"}, 401


def send_message_to_queue(queue_auth, message):
    try:
        account_url = queue_auth["account_url"]
        account_name = queue_auth["account_name"]
        account_key = queue_auth["account_key"]
        queue_name = queue_auth["queue_name"]

        credential = AzureNamedKeyCredential(account_name, account_key)
        queue_service_client = QueueServiceClient(
            account_url=account_url, credential=credential
        )
        queue_client = queue_service_client.get_queue_client(queue_name)

        queue_client.send_message(json.dumps(message))
        print(f"Message added to the queue: {message}")

    except Exception as e:
        print(f"Error sending message to queue: {e}")


def process_issues(
    jira_auth,
    azure_auth,
    queue_auth,
    total_days,
    max_issues,
    business_id,
    timestamp,
    agent_id,
):

    seconds_in_a_day = 86400
    total_days_seconds = total_days * seconds_in_a_day
    end_date = int(datetime.now().timestamp())
    start_date = end_date - total_days_seconds

    jira_issues, status_code = fetch_jira_issues(jira_auth, total_days, max_issues)
    print("JIRA Issues (Formatted):")
    print(json.dumps(jira_issues, indent=4))

    if status_code != 200:
        return jira_issues, status_code

    response, status_code = upload_csv_to_azure(
        azure_auth, business_id, jira_issues, timestamp, agent_id
    )

    if status_code == 200:
        message = {
            "interaction_types": ["incidents"],
            "channels": ["jira"],
            "org_id": business_id,
            "request_id": f"{agent_id}-{timestamp}",
            "request_mode": ["historical"],
            "requested_features": ["topic-classification"],
            "start_date": str(start_date),
            "end_date": str(end_date),
        }
        send_message_to_queue(queue_auth, message)
    return response, status_code


def main(
    jira_auth,
    azure_auth,
    queue_auth,
    total_days,
    max_issues,
    business_id,
    timestamp,
    agent_id,
):
    result, status = process_issues(
        jira_auth,
        azure_auth,
        queue_auth,
        total_days,
        max_issues,
        business_id,
        timestamp,
        agent_id,
    )
    print(f"Result: {result}, Status Code: {status}")


if __name__ == "__main__":
    
    # add the creadential of jira_auth , azure_auth and queue_auth in dict from 
    jira_auth = {
        "jira_url": "",
        "username": "",
        "token": "",
    }

    azure_auth = {
        "account_url": "",
        "account_name": "",
        "account_key": "",
        "container_name": "",
    }

    queue_auth = {
        "account_url": "",
        "account_name": "",
        "account_key": "",
        "queue_name": "",
    }

    total_days = 30
    max_issues = 50
    business_id = "12345"
    timestamp = str(int(datetime.now().timestamp()))

    agent_id = str(uuid.uuid4())[:6]

    main(
        jira_auth,
        azure_auth,
        queue_auth,
        total_days,
        max_issues,
        business_id,
        timestamp,
        agent_id,
    )
