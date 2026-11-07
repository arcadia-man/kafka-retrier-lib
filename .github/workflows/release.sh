#!/bin/bash
set -e

COMMIT=$(git log -1 --pretty=%B)

if echo "$COMMIT" | grep -qE '^feat!|^fix!|BREAKING CHANGE'; then
  VERSION=major
elif echo "$COMMIT" | grep -qE '^feat(\(.+\))?:'; then
  VERSION=minor
elif echo "$COMMIT" | grep -qE '^fix(\(.+\))?:'; then
  VERSION=patch
else
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

npm version "$VERSION"

git push --follow-tags

npm publish