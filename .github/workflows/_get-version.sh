#!/bin/bash
# Outputs a version string in the format yyyy.mm.dd.XX, incrementing XX if tags exist for today.
# Usage: ./_get-version.sh <github_token>

set -e

gh_token="$1"
repo="${GITHUB_REPOSITORY:-dpembo/orchelium}"
today=$(date +'%Y.%m.%d')

echo "DEBUG: Fetching tags from ${repo}..." >&2
echo "DEBUG: Today's date: ${today}" >&2
echo "DEBUG: Using token: ${gh_token:0:10}..." >&2

# Get all tags for today, sort, and find the highest counter
# Use GitHub API v3 with proper authentication
echo "DEBUG: Making API call..." >&2
api_response=$(curl -s -w "\n%{http_code}" -H "Authorization: token $gh_token" "https://api.github.com/repos/$repo/tags?per_page=100")
http_code=$(echo "$api_response" | tail -1)
api_response=$(echo "$api_response" | head -n-1)

echo "DEBUG: HTTP response code: $http_code" >&2

# Check if API call succeeded
if [ "$http_code" != "200" ]; then
  echo "ERROR: GitHub API returned HTTP $http_code" >&2
  echo "DEBUG: Response: $api_response" >&2
  exit 1
else
  # Show raw API response (first 500 chars)
  echo "DEBUG: Raw API response (first 500 chars): ${api_response:0:500}" >&2
  
  # Extract tag names
  all_tag_names=$(echo "$api_response" | jq -r '.[].name' 2>/dev/null)
  jq_exit=$?
  
  if [ $jq_exit -ne 0 ]; then
    echo "ERROR: jq parsing failed (exit code $jq_exit)" >&2
    echo "DEBUG: Response was: $api_response" >&2
    exit 1
  else
    echo "DEBUG: All tags returned by API:" >&2
    echo "$all_tag_names" | sed 's/^/  /' >&2
    
    # Filter for today's date (tags have 'v' prefix, e.g. v2026.03.28.01 or v2026.03.28.01-rc)
    today_tags=$(echo "$all_tag_names" | grep -E "^v${today}\.[0-9]{2}(-rc)?$" | sort -V || true)
    
    if [ -z "$today_tags" ]; then
      echo "DEBUG: No tags matching pattern ^v${today}\\. found" >&2
      echo "DEBUG: Sample tags from API: $(echo "$all_tag_names" | head -5)" >&2
      next_counter="01"
    else
      echo "DEBUG: Found tags for today:" >&2
      echo "$today_tags" | sed 's/^/  /' >&2
      latest_tag=$(echo "$today_tags" | tail -n1)
      echo "DEBUG: Latest tag: $latest_tag" >&2
      
      # Extract counter from latest tag (remove 'v' prefix first)
      latest_tag_no_v="${latest_tag#v}"
      if [[ "$latest_tag_no_v" =~ ^${today}\.([0-9]{2})(-rc)?$ ]]; then
        counter="${BASH_REMATCH[1]}"
        echo "DEBUG: Extracted counter: $counter" >&2
        next_counter=$(printf "%02d" $((10#$counter + 1)))
        echo "DEBUG: Next counter: $next_counter" >&2
      else
        echo "DEBUG: Latest tag doesn't match expected format: $latest_tag" >&2
        echo "DEBUG: Expected pattern: ^v${today}\\\.([0-9]{2})$" >&2
        next_counter="01"
      fi
    fi
  fi
fi

version="${today}.${next_counter}"
echo "INFO: Generated version: $version" >&2
echo "$version"
