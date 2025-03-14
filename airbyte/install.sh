# Install Airbyte
curl -LsfS https://get.airbyte.com | bash -

sleep 10

# Run Airbyte
abctl local install --low-resource-mode --insecure-cookies

sleep 2
# Change password
abctl local credentials --password admin
