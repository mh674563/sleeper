#!/usr/bin/env bash
# Copyright 2022-2023 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

ENVIRONMENTS_DIR=$(cd "$HOME/.sleeper/environments" && pwd)

if [ "$#" -gt 0 ]; then
  SSH_PARAMS=("$@")
else
  SSH_PARAMS=(screen -d -RR)
fi

echo "SSH_PARAMS: " "${SSH_PARAMS[@]}"

ENVIRONMENT_ID=$(cat "$ENVIRONMENTS_DIR/current.txt")
USERNAME=$(cat "$ENVIRONMENTS_DIR/currentUser.txt")

ENVIRONMENT_DIR="$ENVIRONMENTS_DIR/$ENVIRONMENT_ID"
OUTPUTS_FILE="$ENVIRONMENT_DIR/outputs.json"
KNOWN_HOSTS_FILE="$ENVIRONMENT_DIR/known_hosts"

EC2_IP=$(jq ".[\"$ENVIRONMENT_ID-BuildEC2\"].PublicIP" "$OUTPUTS_FILE" --raw-output)
INSTANCE_ID=$(jq ".[\"$ENVIRONMENT_ID-BuildEC2\"].InstanceId" "$OUTPUTS_FILE" --raw-output)
TEMP_KEY_DIR=/tmp/sleeper/temp_keys
TEMP_KEY_PATH="$TEMP_KEY_DIR/$RANDOM"
mkdir -p "$TEMP_KEY_DIR"
rm -f "$TEMP_KEY_DIR/*"

print_time() {
  date -u +"%T UTC"
}

echo "[$(print_time)] Generating temporary SSH key..."
ssh-keygen -q -t rsa -N '' -f "$TEMP_KEY_PATH"
echo "[$(print_time)] Uploading public key..."
aws ec2-instance-connect send-ssh-public-key \
  --instance-id "$INSTANCE_ID" \
  --instance-os-user "$USERNAME" \
  --ssh-public-key "file://$TEMP_KEY_PATH.pub"
echo "[$(print_time)] Connecting..."
ssh -o "UserKnownHostsFile=$KNOWN_HOSTS_FILE" -o "IdentitiesOnly=yes" -i "$TEMP_KEY_PATH" -t \
  -o "ProxyCommand=sh -c \"aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters 'portNumber=%p'\"" \
  "$USERNAME@$INSTANCE_ID" "${SSH_PARAMS[@]}"

rm -f "$TEMP_KEY_DIR/*"
