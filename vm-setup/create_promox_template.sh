#!/bin/bash

# Required variables
CLOUD_IMAGE_NAME=""  # e.g., noble
TEMPLATE_ID=""       # e.g., 9000
MEMORY_ALLOCATION="" # e.g., 2048

# Check if variables are set
[[ -z "$CLOUD_IMAGE_NAME" || -z "$TEMPLATE_ID" || -z "$MEMORY_ALLOCATION" ]] && {
  echo "Error: Set CLOUD_IMAGE_NAME, TEMPLATE_ID, and MEMORY_ALLOCATION"
  exit 1
}

IMG="https://cloud-images.ubuntu.com/${CLOUD_IMAGE_NAME}/current/${CLOUD_IMAGE_NAME}-server-cloudimg-amd64.img"
DEST="/var/lib/vz/template/iso/${CLOUD_IMAGE_NAME}-server-cloudimg-amd64.img"

# Download image and create VM template
wget "$IMG" -O "$DEST" &&
qm create "$TEMPLATE_ID" -memory "$MEMORY_ALLOCATION" -net0 virtio,bridge=vmbr0 -scsihw virtio-scsi-pci &&
qm set "$TEMPLATE_ID" -scsi0 local-lvm:0,import-from="$DEST" &&
qm set "$TEMPLATE_ID" -ide2 local-lvm:cloudinit -boot order=scsi0 -serial0 socket -vga serial0 &&
qm template "$TEMPLATE_ID" &&
echo "VM Template created successfully."
