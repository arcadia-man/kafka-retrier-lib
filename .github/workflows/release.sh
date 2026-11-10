#!/bin/bash
set -e

echo "========================================"
echo "Starting release"
echo "========================================"

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

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

echo "========================================"
echo "Syncing with latest main"
echo "========================================"

git fetch origin main
git reset --hard origin/main

echo "Current commit:"
git rev-parse HEAD

echo "Current package version:"
node -p "require('./package.json').version"

echo "========================================"
echo "Calculating new version"
echo "========================================"

npm version "$VERSION" --no-git-tag-version

NEW_VERSION=$(node -p "require('./package.json').version")
TAG="v$NEW_VERSION"

echo "New package version: $NEW_VERSION"
echo "Release tag: $TAG"

echo "========================================"
echo "Publishing package to npm"
echo "========================================"

npm publish --access public

echo "========================================"
echo "npm publish succeeded"
echo "========================================"

echo "Published version:"
npm view "@arcadia-man/kafka-retrier@$NEW_VERSION" version

echo "========================================"
echo "Creating release commit"
echo "========================================"

git add package.json package-lock.json

git commit -m "gitbot: release $NEW_VERSION"

git tag -a "$TAG" -m "gitbot: release $NEW_VERSION"

echo "========================================"
echo "Pushing release commit and tag"
echo "========================================"

git config --global \
  url."https://x-access-token:${GH_TOKEN}@github.com/".insteadOf \
  "https://github.com/"

git push origin main
git push origin "$TAG"

echo "========================================"
echo "Creating GitHub Release"
echo "========================================"

gh release create "$TAG" \
  --title "$TAG" \
  --generate-notes

echo "========================================"
echo "Release completed successfully"
echo "========================================"

echo "Version: $NEW_VERSION"
echo "Tag: $TAG"