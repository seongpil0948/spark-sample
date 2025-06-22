#!/bin/bash
#
# Setup AWS environment variables from profile
#

PROFILE="${AWS_PROFILE:-toy-root}"
AWS_CREDS_FILE="${HOME}/.aws/credentials"
AWS_CONFIG_FILE="${HOME}/.aws/config"

# Extract credentials from profile
if [ -f "$AWS_CREDS_FILE" ]; then
    # Find the profile section and extract keys
    ACCESS_KEY=$(awk -v profile="[$PROFILE]" '$0 == profile {getline; print}' "$AWS_CREDS_FILE" | grep -o 'aws_access_key_id.*' | cut -d= -f2- | tr -d ' ')
    SECRET_KEY=$(awk -v profile="[$PROFILE]" '$0 == profile {getline; getline; print}' "$AWS_CREDS_FILE" | grep -o 'aws_secret_access_key.*' | cut -d= -f2- | tr -d ' ')
    
    if [ -n "$ACCESS_KEY" ] && [ -n "$SECRET_KEY" ]; then
        export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
        export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
    else
        echo "Warning: Could not extract credentials for profile: $PROFILE" >&2
    fi
fi

# Extract region from config or use default
if [ -f "$AWS_CONFIG_FILE" ]; then
    # Look for region in the profile section
    REGION=$(awk -v profile="[profile $PROFILE]" '
        $0 == profile {found=1; next}
        found && /^region/ {gsub(/^region[[:space:]]*=[[:space:]]*/, ""); print; exit}
        found && /^\[/ {exit}
    ' "$AWS_CONFIG_FILE")
fi

export AWS_REGION="${REGION:-ap-northeast-2}"
export AWS_DEFAULT_REGION="${AWS_REGION}"

# Debug output (optional)
if [ "${DEBUG:-0}" = "1" ]; then
    echo "AWS_PROFILE=$PROFILE"
    echo "AWS_REGION=$AWS_REGION"
    echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:0:10}..."
fi