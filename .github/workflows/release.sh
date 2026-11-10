#!/bin/bash

set -euo pipefail

PACKAGE_NAME="@arcadia-man/kafka-retrier"

echo "========================================"
echo "Starting release"
echo "========================================"

# -----------------------------------------
# 1. Verify release environment
# -----------------------------------------

if [[ "${GITHUB_REF:-}" != "refs/heads/main" ]]; then
  echo "ERROR: Releases are allowed only from main"
  exit 1
fi

if [[ "${GITHUB_EVENT_NAME:-}" != "push" ]]; then
  echo "ERROR: Releases are allowed only for push events"
  exit 1
fi

if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "ERROR: GitHub App token is missing"
  exit 1
fi

if [[ -n "${NODE_AUTH_TOKEN:-}" ]]; then
  echo "ERROR: NODE_AUTH_TOKEN must not be set"
  exit 1
fi

if [[ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ]]; then
  echo "ERROR: GitHub OIDC request URL is missing"
  exit 1
fi

if [[ -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]]; then
  echo "ERROR: GitHub OIDC request token is missing"
  exit 1
fi

echo "GitHub OIDC environment: AVAILABLE"
echo "GitHub App token: AVAILABLE"

# -----------------------------------------
# 2. Determine release type
# Supports normal commits and merge commits
# -----------------------------------------

COMMIT=$(git log -1 --pretty=%B)

echo "Commit message:"
echo "$COMMIT"

RELEASE_TYPE=""

if printf '%s\n' "$COMMIT" |
     grep -Eq '^[[:alnum:]_-]+(\([^)]*\))?!:' ||
   printf '%s\n' "$COMMIT" |
     grep -q '^BREAKING CHANGE:'; then

  RELEASE_TYPE="major"

elif printf '%s\n' "$COMMIT" |
       grep -Eq '^feat(\([^)]*\))?:'; then

  RELEASE_TYPE="minor"

elif printf '%s\n' "$COMMIT" |
       grep -Eq '^fix(\([^)]*\))?:'; then

  RELEASE_TYPE="patch"

fi

echo "Detected release type: ${RELEASE_TYPE:-none}"

if [[ -z "$RELEASE_TYPE" ]]; then
  echo "No release required."
  exit 0
fi

# -----------------------------------------
# 3. Verify package identity
# -----------------------------------------

LOCAL_PACKAGE_NAME=$(node -p "require('./package.json').name")

if [[ "$LOCAL_PACKAGE_NAME" != "$PACKAGE_NAME" ]]; then
  echo "ERROR: Unexpected package name"
  echo "Expected: $PACKAGE_NAME"
  echo "Found: $LOCAL_PACKAGE_NAME"
  exit 1
fi

# -----------------------------------------
# 4. Get latest published npm version
# -----------------------------------------

echo "========================================"
echo "Fetching latest npm version"
echo "========================================"

LATEST_VERSION=$(npm view "${PACKAGE_NAME}@latest" version)

if [[ -z "$LATEST_VERSION" ]]; then
  echo "ERROR: Could not determine latest npm version"
  exit 1
fi

echo "Latest published version: $LATEST_VERSION"

# -----------------------------------------
# 5. Synchronize local version with npm
# -----------------------------------------

echo "========================================"
echo "Synchronizing package version"
echo "========================================"

npm version "$LATEST_VERSION" \
  --no-git-tag-version \
  --allow-same-version

# -----------------------------------------
# 6. Calculate next version
# -----------------------------------------

echo "========================================"
echo "Calculating next version"
echo "========================================"

npm version "$RELEASE_TYPE" --no-git-tag-version

NEW_VERSION=$(node -p "require('./package.json').version")

TAG="v$NEW_VERSION"

echo "Previous npm version: $LATEST_VERSION"
echo "New package version: $NEW_VERSION"
echo "Release tag: $TAG"

# -----------------------------------------
# 7. Check tag before publishing
# -----------------------------------------

if git show-ref --verify --quiet "refs/tags/$TAG"; then
  echo "ERROR: Git tag $TAG already exists"
  echo "Stopping before npm publication"
  exit 1
fi

# -----------------------------------------
# 8. Publish to npm using GitHub OIDC
# -----------------------------------------

echo "========================================"
echo "Publishing to npm"
echo "========================================"

echo "Repository: ${GITHUB_REPOSITORY:-MISSING}"
echo "Workflow: ${GITHUB_WORKFLOW:-MISSING}"
echo "Workflow ref: ${GITHUB_WORKFLOW_REF:-MISSING}"
echo "Node: $(node --version)"
echo "npm: $(npm --version)"

npm publish --access public

echo "========================================"
echo "npm publish command succeeded"
echo "========================================"

# -----------------------------------------
# 9. Verify npm publication
# -----------------------------------------

echo "Verifying published version..."

PUBLISHED_VERSION=""

for attempt in $(seq 1 12); do

  if PUBLISHED_VERSION=$(npm view "${PACKAGE_NAME}@${NEW_VERSION}" version 2>/dev/null); then

    if [[ "$PUBLISHED_VERSION" == "$NEW_VERSION" ]]; then
      break
    fi

  fi

  echo "Publication not yet visible. Attempt $attempt/12"

  sleep 5

done

if [[ "$PUBLISHED_VERSION" != "$NEW_VERSION" ]]; then
  echo "ERROR: npm publication verification failed"
  echo "Expected: $NEW_VERSION"
  echo "Found: ${PUBLISHED_VERSION:-NOT AVAILABLE}"
  exit 1
fi

echo "Verified npm version: $PUBLISHED_VERSION"

# -----------------------------------------
# 10. Configure Git identity
# -----------------------------------------

echo "========================================"
echo "Preparing release commit"
echo "========================================"

git config user.name "github-actions[bot]"

git config user.email \
  "41898282+github-actions[bot]@users.noreply.github.com"

# -----------------------------------------
# 11. Commit version update
# -----------------------------------------

git add package.json package-lock.json

git commit -m "gitbot: release $NEW_VERSION"

# -----------------------------------------
# 12. Create Git tag
# -----------------------------------------

git tag -a "$TAG" -m "gitbot: release $NEW_VERSION"

# -----------------------------------------
# 13. Push commit using GitHub App
# -----------------------------------------

echo "========================================"
echo "Pushing release commit"
echo "========================================"

git config --global \
  url."https://x-access-token:${GH_TOKEN}@github.com/".insteadOf \
  "https://github.com/"

git push origin HEAD:main

# -----------------------------------------
# 14. Push tag
# -----------------------------------------

echo "========================================"
echo "Pushing release tag"
echo "========================================"

git push origin "$TAG"

# -----------------------------------------
# 15. Create GitHub Release
# -----------------------------------------

echo "========================================"
echo "Creating GitHub Release"
echo "========================================"

gh release create "$TAG" \
  --title "$TAG" \
  --generate-notes

# -----------------------------------------
# SUCCESS
# -----------------------------------------

echo "========================================"
echo "RELEASE COMPLETED SUCCESSFULLY"
echo "========================================"

echo "Package: $PACKAGE_NAME"
echo "Version: $NEW_VERSION"
echo "Tag: $TAG"