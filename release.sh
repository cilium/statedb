#!/bin/bash
# Release all modules at the same version.
# Usage: ./release.sh v1.2.0
#
# This script:
# 1. Replaces replace directives and v0.0.0 placeholders with real versions
# 2. Runs go mod tidy on each module
# 3. Commits and tags each module in dependency order
# 4. Restores replace directives for development
#
# After running, push with: git push origin main $VERSION part/$VERSION hive/$VERSION reconciler/$VERSION

set -euo pipefail

VERSION=${1:?usage: release.sh VERSION (e.g. v1.2.0)}

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: VERSION must match vX.Y.Z" >&2
    exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
    echo "error: working tree is dirty, commit or stash changes first" >&2
    exit 1
fi

MODULE=github.com/cilium/statedb

update_dep() {
    local file=$1 dep=$2 version=$3
    sed -i '' "s|${dep} v[^ ]*|${dep} ${version}|g" "$file"
}

remove_replace_block() {
    local file=$1
    # Remove replace ( ... ) block
    sed -i '' '/^replace ($/,/^)$/d' "$file"
    # Remove single-line replace directives
    sed -i '' '/^replace .* => \.\.\//d' "$file"
}

# Step 1: Update go.mod files with real versions and remove replace directives.
echo "Updating go.mod files for ${VERSION}..."

# Root: depends on part
update_dep go.mod "${MODULE}/part" "${VERSION}"
sed -i '' '/^replace.*statedb\/part/d' go.mod

# Hive: depends on statedb, part
update_dep hive/go.mod "${MODULE}" "${VERSION}"
update_dep hive/go.mod "${MODULE}/part" "${VERSION}"
remove_replace_block hive/go.mod

# Reconciler: depends on statedb, hive, part
update_dep reconciler/go.mod "${MODULE}" "${VERSION}"
update_dep reconciler/go.mod "${MODULE}/hive" "${VERSION}"
update_dep reconciler/go.mod "${MODULE}/part" "${VERSION}"
remove_replace_block reconciler/go.mod

# Step 2: Tidy all modules (part first since others depend on it).
echo "Running go mod tidy..."
(cd part && go mod tidy)
go mod tidy
(cd hive && go mod tidy)
(cd reconciler && go mod tidy)

# Step 3: Commit and tag in dependency order.
echo "Committing and tagging..."
git add go.mod go.sum part/go.mod part/go.sum hive/go.mod hive/go.sum reconciler/go.mod reconciler/go.sum
git commit -m "Release ${VERSION}"

git tag "part/${VERSION}"
git tag "${VERSION}"
git tag "hive/${VERSION}"
git tag "reconciler/${VERSION}"

# Step 4: Restore replace directives for development.
echo "Restoring replace directives..."
cat >> go.mod <<EOF

replace github.com/cilium/statedb/part => ./part
EOF

cat >> hive/go.mod <<EOF

replace (
	github.com/cilium/statedb => ../
	github.com/cilium/statedb/part => ../part
)
EOF

cat >> reconciler/go.mod <<EOF

replace (
	github.com/cilium/statedb => ../
	github.com/cilium/statedb/hive => ../hive
	github.com/cilium/statedb/part => ../part
)
EOF

go mod tidy
(cd hive && go mod tidy)
(cd reconciler && go mod tidy)

git add go.mod go.sum hive/go.mod hive/go.sum reconciler/go.mod reconciler/go.sum
git commit -m "Post-release: restore replace directives"

echo ""
echo "Done. Push with:"
echo "  git push origin main ${VERSION} part/${VERSION} hive/${VERSION} reconciler/${VERSION}"
