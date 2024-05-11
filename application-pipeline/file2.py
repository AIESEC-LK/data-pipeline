# https://gis-api.aiesec.org/graphql

# 'cb2028d9b0bf17ba331951626a444ee036d856b18f499480b91538c69ea65084'
import requests
import csv



# Define the GraphQL endpoint URL
url = "https://gis-api.aiesec.org/graphql"

# Define the GraphQL query
query = """
query GetallApplications {
  allOpportunityApplication(
    page: 401
    per_page: 25
    filters: {
      person_home_mc: 1623,
      programmes: [2,8,9],
      created_at:{
            from: "01/07/2020" 
            to:"11/05/2024"
            }
    }
    sort: "created_at:desc"
  ) {
    data {
      id
      an_signed_at
      branch {
        id
      }
      created_at
      current_status
      date_approved
      date_matched
      date_realized
      followed_up_date
      cv {
        url
      }
      home_mc {
        name
      }
      host_lc {
        name
      }
      opportunity {
        home_lc {
          id
          name
        }
        home_mc {
          id
          name
        }
        project_name
        title
        programme {
          id
          short_name
        }
        id
        organisation {
          name
          id
        }
      }
      person {
        created_at
        full_name
        id
        home_lc {
          id
          name
        }
        home_mc {
          id
          name
        }
        lc_alignment {
          id
        }
        email
        contact_detail {
          phone
          email
          country_code
        }
      }
      status
    }
    paging {
      current_page
      total_items
      total_pages
    }
  }
}
"""

# Define the headers, including authorization if needed
headers = {
    "Authorization": "cb2028d9b0bf17ba331951626a444ee036d856b18f499480b91538c69ea65084",
    "Content-Type": "application/json"
}

# Send the GraphQL query with headers
response = requests.post(url, json={"query": query}, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data
    data = response.json()["data"]["allOpportunityApplication"]["data"]

    # Open a CSV file for writing
    with open("applications5.csv", "w", newline="", encoding="utf-8-sig") as csvfile:
        fieldnames = [
            "EP ID",
            "Application ID",
            "Opportunity ID",
            "Home LC",
            "Home MC",
            "Host LC",
            "Host MC",
            "Current Status",
            "Applied Date",
            "Approved Date",
            "Accepted Date",
            "Realized Date",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header row
        writer.writeheader()

        # Write each application to the CSV file
        for application in data:
            writer.writerow(
                {
                    "EP ID": application["person"]["id"],
                    "Application ID": application["id"],
                    "Opportunity ID": application["opportunity"]["id"],
                    "Home LC": application["person"]["home_lc"]["name"],
                    "Home MC": application["home_mc"]["name"],
                    "Host LC": application["host_lc"]["name"],
                    "Host MC": application["opportunity"]["home_mc"]["name"],
                    "Current Status": application["current_status"],
                    "Applied Date": application["created_at"],
                    "Approved Date": application["date_approved"],
                    "Accepted Date": application["date_matched"],
                    "Realized Date": application["date_realized"],
                }
            )

    print("Data saved to applications.csv")
else:
    print("Error:", response.status_code)