import requests
import csv

# Define the GraphQL query
graphql_query = '''
query GetallApplications{
  allOpportunityApplication(
    page: 1
    per_page: 10
    filters: {
      person_home_mc: 1623,
      programmes: [2,8,9]
    }
  ) {
    data {
      id
      an_signed_at
      branch{
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
      id
      opportunity {
        home_lc{
          id
          name
        }
        home_mc{
          id
          name
        }
        project_name
        title
        programme{
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
        home_lc{
          id
          name
        }
        home_mc{
          id 
          name
        }
        lc_alignment{
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
'''

def get_all_applications():
    url = 'https://gis-api.aiesec.org/graphql'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'cb2028d9b0bf17ba331951626a444ee036d856b18f499480b91538c69ea65084'
    }
    data = {
        'query': graphql_query
    }

    response = requests.post(url, json=data, headers=headers)
    response.raise_for_status()

    try:
        return response.json()['data']['allOpportunityApplication']['data']
    except KeyError:
        print("Error: Unexpected response format.")
        print(response.json())

def parse_application(application):
    return [
        application.get('id', ''),
        application['person']['full_name'] if application.get('person') else '',
        application['person']['contact_detail']['phone'] if application.get('person') and 'contact_detail' in application['person'] else '',
        application['person']['id'] if application.get('person') else '',
        application['opportunity']['id'] if application.get('opportunity') else '',
        application['person']['home_mc']['name'] if application.get('person') and 'home_mc' in application['person'] else '',
        application['person']['home_lc']['name'] if application.get('person') and 'home_lc' in application['person'] else '',
        # application['person']['lc_alignment']['id'] if application.get('person') and 'lc_alignment' in application['person'] else '',
        application['opportunity']['title'] if application.get('opportunity') else '',
        application['home_mc']['name'] if 'home_mc' in application else '',
        application['host_lc']['name'] if 'host_lc' in application else '',
        application['opportunity']['programme']['id'] if application.get('opportunity') and 'programme' in application['opportunity'] else '',
        application.get('status', ''),
        application.get('created_at', ''),
        application.get('date_matched', ''),
        application.get('date_approved', ''),
        application.get('date_realized', '')
    ]

def export_to_csv(applications, sheet_headers):
    with open('applications.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(sheet_headers)
        for application in applications:
            writer.writerow(parse_application(application))

if __name__ == "__main__":
    sheet_headers = [
        "ID",
        "EP Name",
        "Phone Number",
        "EP ID",
        "OP ID",
        "Home MC",
        "Home LC",
        "LC Alignment",
        "Title",
        "Host MC",
        "Host LC",
        "Product",
        "Status",
        "Applied At",
        "Accepted At",
        "Approved At",
        "Realized At"
    ]

    applications = get_all_applications()
    if applications:
        export_to_csv(applications, sheet_headers)
        print(f"Total records: {len(applications)}")
    else:
        print("No applications found.")
