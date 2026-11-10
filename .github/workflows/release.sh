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

echo "========================================"
echo "Release authentication check"
echo "========================================"

if [ -n "${NODE_AUTH_TOKEN:-}" ]; then
  echo "ERROR: NODE_AUTH_TOKEN must not be set for npm Trusted Publishing"
  exit 1
fi

if [ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ]; then
  echo "ERROR: GitHub OIDC request URL is missing"
  exit 1
fi

if [ -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]; then
  echo "ERROR: GitHub OIDC request token is missing"
  exit 1
fi

echo "NODE_AUTH_TOKEN: NOT SET"
echo "GitHub OIDC environment: AVAILABLE"

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


echo "========================================"
echo "GitHub OIDC identity diagnostics"
echo "========================================"

echo "Repository:       ${GITHUB_REPOSITORY:-MISSING}"
echo "Workflow name:    ${GITHUB_WORKFLOW:-MISSING}"
echo "Workflow ref:     ${GITHUB_WORKFLOW_REF:-MISSING}"
echo "Git ref:          ${GITHUB_REF:-MISSING}"
echo "Event:            ${GITHUB_EVENT_NAME:-MISSING}"
echo "Node:             $(node --version)"
echo "npm:              $(npm --version)"
echo "Registry:         $(npm config get registry)"

echo "OIDC URL:         $([ -n "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] && echo SET || echo MISSING)"
echo "OIDC token:       $([ -n "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ] && echo SET || echo MISSING)"

echo "========================================"

npm publish --access public

echo "========================================"
echo "npm publish succeeded"
echo "========================================"

echo "Verifying published version..."

PUBLISHED_VERSION=$(npm view "@arcadia-man/kafka-retrier@$NEW_VERSION" version)

if [ "$PUBLISHED_VERSION" != "$NEW_VERSION" ]; then
  echo "ERROR: npm published version verification failed"
  echo "Expected: $NEW_VERSION"
  echo "Found: $PUBLISHED_VERSION"
  exit 1
fi

echo "npm published version: $PUBLISHED_VERSION"

echo "========================================"
echo "Creating release commit"
echo "========================================"

git add package.json package-lock.json

git commit -m "gitbot: release $NEW_VERSION"

git tag -a "$TAG" -m "gitbot: release $NEW_VERSION"

echo "========================================"
echo "Pushing release commit"
echo "========================================"

git config --global \
  url."https://x-access-token:${GH_TOKEN}@github.com/".insteadOf \
  "https://github.com/"

git push origin main

echo "========================================"
echo "Pushing release tag"
echo "========================================"

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

echo "Package: @arcadia-man/kafka-retrier"
echo "Version: $NEW_VERSION"
echo "Tag: $TAG"