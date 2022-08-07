#!/bin/zsh

echo "Bumping Version"
bump2version $1
echo "Updating changelog"
conventional-changelog -i CHANGELOG.md -s -r 2
git add CHANGELOG.md
git commit -m "doc: Updated CHANGELOG.md"
echo "Pushing"
git push
echo "Pushing tag"
git push --tag