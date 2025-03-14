#!/bin/bash

# Prompt the user for their choice
echo "Choose an option:"
echo "1) Stop container and keep data"
echo "2) Stop container and remove all data"
read -p "Enter 1 or 2: " option

# Handle the user's input and run the corresponding command
if [ "$option" -eq 1 ]; then
    echo "You chose option 1: Stop container and keep data"
    abctl local uninstall
elif [ "$option" -eq 2 ]; then
    echo "You chose option 2: Stop container and remove all data"
    abctl local uninstall --persisted
else
    echo "Invalid choice. Exiting."
    exit 1
fi

# Remove the directory
rm -rf ~/.airbyte/abctl


# Clear any remaining information abctl created
rm -rf ~/.airbyte/abctl
