#!/bin/bash

## Ensure the user is logged into Azure
echo ">Checking azure login ..."
if ! az account show > /dev/null 2>&1; then
  echo "You are not logged in to Azure. Opening login prompt..."
  az login
  if [ $? -ne 0 ]; then
    echo "Azure login failed. Exiting."
    exit 1
  fi
else
  echo "You are already logged in."
fi


## Check logic app directory in local repo
echo ""
echo ">Checking app directory in local repo ..."
# Local repo
LOCAL_DIR=".."

LOCAL_FULL_PATH=$(realpath "$LOCAL_DIR")
echo "The app is located at $LOCAL_FULL_PATH in local repo"

# Ensure the local directory exists
mkdir -p "$LOCAL_DIR" || {
  echo "Error: Failed to create or access the app directory"
  exit 1
}

# Ask confirmation if app directory is not empty
if [ "$(ls -A "$LOCAL_DIR")" ]; then
  read -p "Warning: app directory is not empty. Overwrite existing files? [y/n] " confirm
  confirm=${confirm,,}
  if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted by user."
    exit 0
  fi
fi


## Download zip package
echo ""
echo ">Downloading zip package of the app ..."

# Connection for currency logic appp
SUBSCRIPTION_ID="lake-npd-subs"
RESOURCE_GROUP="lake-cac-npd-rg-devcurrency"
LOGIC_APP_NAME="lake-cac-npd-logic-devcurrency01"

# Set Subscription ID
az account set --subscription "$SUBSCRIPTION_ID"
if [ $? -ne 0 ]; then
  echo "Failed to set subscription. Please check the ID."
  exit 1
fi

# Fetch publishing credentials (MSDeploy)
PUBLISH_URL=$(az webapp deployment list-publishing-profiles \
  --name "$LOGIC_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "[?publishMethod=='MSDeploy'].publishUrl" -o tsv | cut -d ':' -f1)

USER_NAME=$(az webapp deployment list-publishing-profiles \
  --name "$LOGIC_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "[?publishMethod=='MSDeploy'].userName" -o tsv)

PASSWORD=$(az webapp deployment list-publishing-profiles \
  --name "$LOGIC_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "[?publishMethod=='MSDeploy'].userPWD" -o tsv)

if [ -z "$PUBLISH_URL" ] || [ -z "$USER_NAME" ] || [ -z "$PASSWORD" ]; then
  echo "Failed to retrieve publishing credentials. Ensure it's a Standard Logic App and you have sufficient permissions."
  exit 1
fi

# Build Kudu ZIP API URL
KUDU_ZIP_URL="https://${PUBLISH_URL}/api/zip/site/wwwroot/"
TEMP_ZIP="/tmp/${LOGIC_APP_NAME}.zip"

# Download the ZIP file
curl -L -u "$USER_NAME:$PASSWORD" --insecure "$KUDU_ZIP_URL" --output "$TEMP_ZIP"
if [ $? -ne 0 ] || [ ! -s "$TEMP_ZIP" ]; then
  echo "Failed to download ZIP file from Kudu. File is missing or empty."
  exit 1
fi
echo "The zip package is downloaded at $TEMP_ZIP"

## Extract to a temporary directory
echo ""
echo ">Extracting files to temp directory ..."
TEMP_DIR=$(mktemp -d)
unzip -q "$TEMP_ZIP" -d "$TEMP_DIR"
echo "The package was extracted at $TEMP_DIR"


## Sync files to local repo
echo ""
echo ">Sync files from temp directory to local repo ..."
for file in $(find "$TEMP_DIR" -type f); do
  rel_path="${file#$TEMP_DIR/}"
  target="$LOCAL_DIR/$rel_path"

  if [ -f "$target" ]; then
    if ! cmp -s "$file" "$target"; then
      echo "Conflict: $rel_path already exists and has local changes."
      read -p "Do you want to overwrite it? [y/n] " answer
      answer=${answer,,}
      if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
        cp "$file" "$target"
        echo "Overwritten: $rel_path"
      else
        echo "Skipped: $rel_path"
      fi
    else
      # Files are identical; no action needed
      :
    fi
  else
    # File doesn't exist locally, safe to copy
    mkdir -p "$(dirname "$target")"
    cp "$file" "$target"
    echo "Added: $rel_path"
  fi
done
echo "file synced"


# Cleanup
echo ""
echo ">Cleaning up ..."
rm -rf "$TEMP_DIR"
echo "done"


# Done
echo ""
echo "*Sync complete*"