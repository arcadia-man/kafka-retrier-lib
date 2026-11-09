#!/bin/bash
set -e

echo "========================================"
echo "Starting release"
echo "========================================"

echo "Current branch:"
git branch --show-current

echo "Latest commit:"
git log -1 --pretty=fuller

COMMIT=$(git log -1 --pretty=%B)

echo "Commit message:"
echo "$COMMIT"

VERSION=""

if echo "$COMMIT" | grep -qE '^feat!|^fix!|BREAKING CHANGE'; then
  VERSION=major
elif echo "$COMMIT" | grep -qE '^feat(\(.+\))?:'; then
  VERSION=minor
elif echo "$COMMIT" | grep -qE '^fix(\(.+\))?:'; then
  VERSION=patch
fi

echo "Detected release type: ${VERSION:-none}"

if [ -z "$VERSION" ]; then
  echo "No release required."
  exit 0
fi

echo "Configuring git identity..."

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

echo "Current package version:"
node -p "require('./package.json').version"

echo "Running npm version: $VERSION"

npm version "$VERSION"

echo "New package version:"
node -p "require('./package.json').version"

echo "Configuring GitHub App authentication..."

git config --global url."https://x-access-token:${GH_TOKEN}@github.com/".insteadOf "https://github.com/"

echo "Pushing version commit and tag..."

git push --follow-tags

echo "Publishing package to npm..."

npm publish --access public

echo "========================================"
echo "Release completed successfully"
echo "========================================"