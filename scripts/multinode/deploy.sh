#!/bin/bash
# Ship the current branch to both experiment machines and prove it landed.
#
# A bundle carries prerequisites: `git bundle create f BASE..branch` can only be applied
# by a repo that already has BASE. Bundling from a local commit the servers never received
# makes `git fetch` fail with "Repository lacks these prerequisite commits", and if that
# error is discarded the following `git checkout FETCH_HEAD` silently re-checks-out the
# stale FETCH_HEAD from an earlier bundle. Three verification runs were spent on a binary
# three commits old that way, including one where a probe read 0 because it was not in the
# build at all. So: base the bundle on each server's own HEAD, and verify afterwards.
set -eu
cd "$(dirname "$0")/../.."
BRANCH=$(git rev-parse --abbrev-ref HEAD)
WANT=$(git rev-parse HEAD)
HOSTS=${HOSTS:-"tikv240 tikv241"}
BUILD=${BUILD:-1}   # BUILD=0 to skip rebuilding the node binaries

for h in $HOSTS; do
  have=$(ssh "$h" "cd ~/work/Nezha && git rev-parse HEAD" 2>/dev/null | tr -d '\r')
  [ -n "$have" ] || { echo "DEPLOY_FAIL $h: cannot read remote HEAD"; exit 1; }
  if [ "$have" = "$WANT" ]; then
    echo "$h already at ${WANT:0:7}"
  else
    git cat-file -e "$have^{commit}" 2>/dev/null || { echo "DEPLOY_FAIL $h: remote HEAD $have unknown locally"; exit 1; }
    b=$(mktemp -t nezha-bundle).bundle
    git bundle create "$b" "$have..$BRANCH" >/dev/null
    scp -q "$b" "$h:~/deploy.bundle"
    rm -f "$b"
    # No 2>/dev/null here: a fetch failure must be visible, and checkout must not run on a
    # stale FETCH_HEAD, hence the &&.
    ssh "$h" "cd ~/work/Nezha && git fetch ~/deploy.bundle $BRANCH && git checkout -B $BRANCH FETCH_HEAD" || {
      echo "DEPLOY_FAIL $h: fetch/checkout"; exit 1; }
  fi
  got=$(ssh "$h" "cd ~/work/Nezha && git rev-parse HEAD" 2>/dev/null | tr -d '\r')
  [ "$got" = "$WANT" ] || { echo "DEPLOY_FAIL $h: HEAD is $got, wanted $WANT"; exit 1; }
  if [ "$BUILD" = 1 ]; then
    ssh "$h" "source ~/env.sh; cd ~/work/Nezha && go build -o /tmp/nezha-three-normal ./cmd/nezha/" || {
      echo "DEPLOY_FAIL $h: build"; exit 1; }
  fi
  scp -q scripts/multinode/three-node.sh "$h:~/three-node.sh"
  echo "DEPLOY_OK $h ${WANT:0:7}"
done
