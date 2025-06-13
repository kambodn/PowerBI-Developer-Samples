import requests
import base64
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Azure DevOps organization and project details
organization = os.getenv('organization')
project = os.getenv('project')
pat = os.getenv('pat')
parent_work_item_id = 27079  # ID of the parent work item

# Encode PAT for Basic Authentication
auth = base64.b64encode(f":{pat}".encode()).decode()
headers = {
    "Content-Type": "application/json-patch+json",
    "Authorization": f"Basic {auth}"
}

def delete_work_item(work_item_id):
    """
    Deletes a work item.
    """
    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/{work_item_id}?api-version=7.1-preview.3"
    response = requests.delete(url, headers=headers)
    if response.status_code == 204:
        logging.info(f"Successfully deleted Work Item ID {work_item_id}")
        print(f"Successfully deleted Work Item ID {work_item_id}")
    else:
        logging.error(f"{response.status_code} - Failed to delete Work Item ID {work_item_id}: {response.text}")
        print(f"{response.status_code} - Failed to delete Work Item ID {work_item_id}: {response.text}")

def get_descendants(work_item_id):
    """
    Retrieves the IDs of all child work items of a given work item.
    """
    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/{work_item_id}?$expand=relations&api-version=7.1-preview.3"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"{response.status_code} - Failed to retrieve descendants for Work Item ID {work_item_id}: {response.text}")
        return []
    
    work_item = response.json()
    child_work_item_ids = []
    if "relations" in work_item:
        for relation in work_item["relations"]:
            if relation["rel"] == "System.LinkTypes.Hierarchy-Forward":
                child_id = relation["url"].split("/")[-1]
                child_work_item_ids.append(child_id)
    return child_work_item_ids

def delete_hierarchy_recursive(work_item_id):
    """
    Recursively deletes the specified work item and all its descendants.
    """
    
    # Get immediate descendants
    child_ids = get_descendants(work_item_id)
    
    # Recursively delete each descendant
    for child_id in child_ids:
        delete_hierarchy_recursive(child_id)

    # Delete the current work item
    delete_work_item(work_item_id)

if __name__ == "__main__":
    delete_hierarchy_recursive(parent_work_item_id)