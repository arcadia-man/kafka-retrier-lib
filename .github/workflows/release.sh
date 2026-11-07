#!/bin/bash
set -e

COMMIT=$(git log -1 --pretty=%B)

if echo "$COMMIT" | grep -qE '^feat!|^fix!|BREAKING CHANGE'; then
  npm version major
elif echo "$COMMIT" | grep -qE '^feat(\(.+\))?:'; then
  npm version minor
elif echo "$COMMIT" | grep -qE '^fix(\(.+\))?:'; then
  npm version patch
else
  exit 0
fi

git push --follow-tags
npm publish