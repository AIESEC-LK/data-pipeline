

# # Replace with your GraphQL endpoint URL
# graphql_endpoint = "https://gis-api.aiesec.org/graphql"

# # Replace with your authorization token
# authorization_header = {"Authorization": "Bearer dabc507c31ac93d34380a64e085e7d2c9bdfedea3b47893fa5a6599265daa7ee"}

import requests
import json

def fetch_all_opportunity_applications(query_params, headers=None):
    endpoint = 'https://gis-api.aiesec.org/graphql'
    page = 1
    all_data = []

    # Define the GraphQL query
    query = """
        query ApplicationIndexQuery($applied_at: Boolean!, $status: Boolean!, $experience_start_date: Boolean!, $slot: Boolean!, $tags: Boolean!, $opportunity: Boolean!, $applicant_name: Boolean!, $host_mc: Boolean!, $host_lc: Boolean!, $home_mc: Boolean!, $home_lc: Boolean!, $date_marked_approved: Boolean!, $date_ep_accepted_offer: Boolean!, $shortlisted: Boolean!, $date_marked_accepted_by_host: Boolean!, $backgrounds: Boolean!, $languages: Boolean!, $sub_product: Boolean!, $skills: Boolean!, $date_marked_realized: Boolean!, $phone_number: Boolean!, $campaign_id: Boolean!, $page: Int, $perPage: Int, $filters: ApplicationFilter, $sort: String, $q: String) {
          allOpportunityApplication(
            page: $page
            per_page: $perPage
            q: $q
            filters: $filters
            sort: $sort
          ) {
            ...ApplicationList
            __typename
          }
        }
        
        fragment ApplicationList on OpportunityApplicationList {
          data {
            id
            status
            favourite @include(if: $shortlisted)
            date_realized @include(if: $date_marked_realized)
            nps_grade
            has_started_standards_survey
            date_approved @include(if: $date_marked_approved)
            an_signed_at @include(if: $date_ep_accepted_offer)
            meta @include(if: $date_marked_accepted_by_host) {
              date_matched
              __typename
            }
            opportunity {
              id
              title @include(if: $opportunity)
              branch {
                company {
                  name
                  id
                  is_gep
                  __typename
                }
                __typename
              }
              __typename
            }
            person {
              id
              full_name @include(if: $applicant_name)
              profile_photo @include(if: $applicant_name)
              email
              contact_detail {
                phone @include(if: $phone_number)
                country_code @include(if: $phone_number)
                __typename
              }
              home_lc @include(if: $home_lc) {
                name
                __typename
              }
              home_mc @include(if: $home_mc) {
                name
                __typename
              }
              __typename
            }
            status @include(if: $status)
            created_at @include(if: $applied_at)
            experience_end_date
            experience_start_date @include(if: $experience_start_date)
            __typename
          }
          paging {
            total_pages
            current_page
            total_items
            __typename
          }
          __typename
        }
    """

    while True:
        query_params['pagination'] = {'page': page}
        response = requests.post(endpoint, json={'query': query, 'variables': query_params}, headers=headers)

        # Print the response content and status code for debugging
        print("Response status code:", response.status_code)
        print("Response content:", response.text)

        data = response.json()

        # Extract data from the response
        opportunity_applications = data.get('data', {}).get('allOpportunityApplication', {}).get('data', [])
        all_data.extend(opportunity_applications)

        # Check if there are more pages
        total_pages = data.get('data', {}).get('allOpportunityApplication', {}).get('paging', {}).get('total_pages', 0)
        if page >= total_pages:
            break
        else:
            page += 1

    return all_data

# Replace query_params with your actual parameters
query_params = {
    "page": 1,
    "count": 10000,
    "filters": {
        "person_home_mc": 1623,
        "programmes": [2, 8, 9]
    },
    # Add other parameters here
}

# Replace headers with your actual headers if needed
headers = {
    "Authorization": "Bearer dabc507c31ac93d34380a64e085e7d2c9bdfedea3b47893fa5a6599265daa7ee",
    # Add other headers here
}

all_opportunity_applications = fetch_all_opportunity_applications(query_params, headers)

# Print or process all the opportunity applications
for application in all_opportunity_applications:
    print(application)
