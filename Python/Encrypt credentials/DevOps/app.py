import requests
import base64
import logging

# Azure DevOps organization and project details
organization = "DS-GroupICT"
project = "Digital%20Division"
pat = ""
parent_work_item_id = 21569  # ID of the parent work item
field_to_set = "Custom.DS_Year"
field_value = "2024"

# Encode PAT for Basic Authentication
auth = base64.b64encode(f":{pat}".encode()).decode()
headers = {
    "Content-Type": "application/json-patch+json",
    "Authorization": f"Basic {auth}"
}

def update_work_item(work_item_id, field_name, value):
    """
    Updates the specified field of a work item.
    """
    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/{work_item_id}?api-version=7.1-preview.3"
    update_data = [
        {
            "op": "add", 
            "path": f"/fields/{field_name}",
            "value": value
        }
    ]
    response = requests.patch(url, headers=headers, json=update_data)
    if response.status_code == 200:
        logging.info(f"Successfully updated Work Item ID {work_item_id}")
        print(f"Successfully updated Work Item ID {work_item_id}")
    else:
        logging.error(f"Failed to update Work Item ID {work_item_id}: {response.text}")
        print(f"Failed to update Work Item ID {work_item_id}: {response.text}")

def get_descendants(work_item_id):
    """
    Retrieves the IDs of all child work items of a given work item.
    """
    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/{work_item_id}?$expand=relations&api-version=7.1-preview.3"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve descendants for Work Item ID {work_item_id}: {response.text}")
        return []
    
    work_item = response.json()
    child_work_item_ids = []
    if "relations" in work_item:
        for relation in work_item["relations"]:
            if relation["rel"] == "System.LinkTypes.Hierarchy-Forward":
                child_id = relation["url"].split("/")[-1]
                child_work_item_ids.append(child_id)
    return child_work_item_ids

def update_hierarchy_recursive(work_item_id, field_name, value):
    """
    Recursively updates the specified field for the given work item and all its descendants.
    """
    # Update the current work item
    print(f"Updating Work Item ID {work_item_id}")
    update_work_item(work_item_id, field_name, value)
    
    # Get immediate descendants
    child_ids = get_descendants(work_item_id)
    
    # Recursively update each descendant
    for child_id in child_ids:
        update_hierarchy_recursive(child_id, field_name, value)

# Main Execution
if __name__ == "__main__":
    update_hierarchy_recursive(parent_work_item_id, field_to_set, field_value)