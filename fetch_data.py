import os
import time
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

REPO_OWNER = "PostHog"
REPO_NAME = "posthog"
DAYS = 90
MAX_RETRIES = 5

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

GRAPHQL_URL = "https://api.github.com/graphql"
headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json",
}

PULL_REQUESTS_QUERY = """
query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(
      first: 100
      after: $cursor
      states: [MERGED, CLOSED]
      orderBy: { field: UPDATED_AT, direction: DESC }
    ) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        databaseId
        number
        title
        state
        createdAt
        mergedAt
        updatedAt
        author { login }
        additions
        deletions
        changedFiles
        reviews(first: 100) {
          nodes {
            databaseId
            author { login }
            state
            submittedAt
          }
        }
      }
    }
  }
}
"""


def graphql_request(query: str, variables: dict) -> dict:
    for attempt in range(MAX_RETRIES):
        resp = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=headers,
        )

        if resp.status_code in (403, 429):
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) + 1 if retry_after else 2 ** attempt * 10
            print(f"  Rate limited, waiting {wait}s... (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        return data["data"]

    raise Exception(f"Failed after {MAX_RETRIES} retries")


def get_existing_pr_numbers() -> set:
    numbers = set()
    page_size = 1000
    offset = 0
    while True:
        result = supabase.table("pull_requests").select("number").range(offset, offset + page_size - 1).execute()
        for row in result.data:
            numbers.add(row["number"])
        if len(result.data) < page_size:
            break
        offset += page_size
    return numbers


def fetch_all_prs(since: datetime, existing: set):
    prs = []
    cursor = None
    page = 1

    while True:
        data = graphql_request(PULL_REQUESTS_QUERY, {
            "owner": REPO_OWNER,
            "name": REPO_NAME,
            "cursor": cursor,
        })

        pr_data = data["repository"]["pullRequests"]
        nodes = pr_data["nodes"]
        page_info = pr_data["pageInfo"]

        new_in_page = 0
        stop = False
        for pr in nodes:
            updated = datetime.fromisoformat(pr["updatedAt"].replace("Z", "+00:00"))
            if updated < since:
                stop = True
                break
            if pr["number"] not in existing:
                prs.append(pr)
                new_in_page += 1

        print(f"  Page {page}: {new_in_page} new PRs (skipped {len(nodes) - new_in_page} existing), total new: {len(prs)}")

        # If the entire page was already known PRs, no point paginating further
        if stop or not page_info["hasNextPage"] or (new_in_page == 0 and len(nodes) > 0):
            break

        cursor = page_info["endCursor"]
        page += 1

    return prs


def upsert_prs(prs: list):
    rows = []
    for pr in prs:
        rows.append({
            "id": pr["databaseId"],
            "number": pr["number"],
            "title": pr["title"],
            "author": pr["author"]["login"] if pr["author"] else "ghost",
            "state": pr["state"],
            "merged": pr["mergedAt"] is not None,
            "created_at": pr["createdAt"],
            "merged_at": pr["mergedAt"],
            "additions": pr["additions"],
            "deletions": pr["deletions"],
            "changed_files": pr["changedFiles"],
        })
    supabase.table("pull_requests").upsert(rows, on_conflict="id").execute()


def upsert_reviews(prs: list):
    rows = []
    for pr in prs:
        pr_number = pr["number"]
        for review in pr["reviews"]["nodes"]:
            if review["state"] not in ("APPROVED", "CHANGES_REQUESTED", "COMMENTED"):
                continue
            if not review.get("submittedAt"):
                continue
            if not review["author"]:
                continue
            rows.append({
                "id": review["databaseId"],
                "pull_request_number": pr_number,
                "reviewer": review["author"]["login"],
                "state": review["state"],
                "submitted_at": review["submittedAt"],
            })

    if rows:
        supabase.table("reviews").upsert(rows, on_conflict="id").execute()


def main():
    since = datetime.now(timezone.utc) - timedelta(days=DAYS)
    print(f"Fetching PRs updated since {since.date()} for {REPO_OWNER}/{REPO_NAME}...")
    print("Using GraphQL — fetching PR details and reviews in a single query.\n")

    existing = get_existing_pr_numbers()
    print(f"Found {len(existing)} PRs already in database, skipping those.\n")

    prs = fetch_all_prs(since, existing)
    print(f"\nFetched {len(prs)} new PRs. Upserting to Supabase...")

    if not prs:
        print("Nothing new to store. Done.")
        return

    upsert_prs(prs)
    upsert_reviews(prs)

    print("Done.")


if __name__ == "__main__":
    main()
