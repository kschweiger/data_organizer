#!/bin/zsh

echo "Bumping Version"
bump2version $1
if [[ $? != 0 ]]
then
  echo "bump2version failed. Exiting..."
  exit 1
fi
echo "Updating changelog"
conventional-changelog -i CHANGELOG.md -s -r 2
if [[ $? != 0 ]]
then
  echo "Generating changelog failed. Exiting..."
  exit 1
fi
git add CHANGELOG.md
git commit -m "doc: Updated CHANGELOG.md"
echo "Pushing"
git push
echo "Pushing tag"
git push --tag