from azure.storage.blob import BlobServiceClient
import os
import yaml
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import logging
import sys
class ADLSModule:

    def __init__(self,sa_name,connenction_string,key):
        """
        Azure Data Lake Storage module, the class contains common function that you need to interact with ADLS
        :param _sa_name: Your storage account name
        :param _connection_string: storage account's connection string
        :param _key: storage account's key
        """
        self._sa_name=sa_name
        self._connection_string=connenction_string
        self._key=key
        self._blob_service_client=BlobServiceClient.from_connection_string(self._connection_string)

    def upload_file_to_container(self,container_name,file,blob_name,metadata=None):
        if file is not None:
            container_client = self._blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(file, overwrite=True)
            blob_client.set_blob_metadata(metadata=metadata)
            print(f"File {blob_name} uploaded successfully")
            return True
        else:
            print(f"File {blob_name} uploaded failed")
            return False
        
    def delete_files_in_path(self, container_name, path):
        container_client = self._blob_service_client.get_container_client(container_name)
        if path is not None:
             # Delete all blobs in the directory
            blobs = container_client.list_blobs(name_starts_with=path)
            for blob in blobs:
                container_client.delete_blob(blob.name)    
                print(f"{blob.name} deleted from {path} sucessfully") 
            print(f"All items deleted in directory {path}.")
        else:
            print(f"Please provide path to delete in storage account")

    def read_files_in_path(self, container_name, path):
        container_client = self._blob_service_client.get_container_client(container_name)
        files = []
        if path is not None:
            blobs = container_client.list_blobs(name_starts_with=path)
            for blob in blobs:
                blob_client = container_client.get_blob_client(blob.name)
                stream = blob_client.download_blob()
                files.append({"name": blob.name, "url": blob_client.url, "data": stream.readall()})
            return files
        else:
            print(f"Please provide path to read in storage account")
            return None
        

    def move_data_between_container(self, src_container = None, dest_container= None):
        """
        Detect files in source container and move those files to destination container
        :param src_container: name of source container
        :param dest_container: name of destination container
        """

        # If no argument passed then ends the function
        if src_container is None or dest_container is None:
            print("Please specify the conntainer name")
            return
        
        # Create a ContainerClient object
        src_container_client = self._blob_service_client.get_container_client(src_container)
        dest_container_client= self._blob_service_client.get_container_client(dest_container)
        print("Starts moving data process")
        print(f"Inside move_data_between_container : source container set is : {src_container}\n destination container set is : {dest_container}")

        # cleanup destination container
        self.clean_container(dest_container)

        # Move files to working zone
        for blob_name in self.get_files(src_container):
            # Create reference for each blob file
            src_blob=src_container_client.get_blob_client(blob_name)
            dest_blob=dest_container_client.get_blob_client(blob_name)

            # Upload file from remote URL of blob's source container
            dest_blob.start_copy_from_url(src_blob.url)

        # cleanup source container
        self.clean_container(src_container)
        print("Moving process's successful")

    def clean_container(self, container_name):
        """
        Clean the container, delete all files
        :param container_name: container name to clean
        """

        # Create a ContainerClient object
        container_client = self._blob_service_client.get_container_client(container_name)

        # Get list of blob names in container
        blob_list = container_client.list_blobs()
        blob_names = [blob.name for blob in blob_list]

        # Delete blobs
        container_client.delete_blobs(*blob_names)
        
        print(f"All files have been deleted from '{container_name}' container.")

    def upload_folder_to_container(self,container_name,local_folder_path):
        # Set the local paths to the folders containing the files you want to upload
        local_folder_paths = [local_folder_path]

        # Create a ContainerClient object
        container_client = self._blob_service_client.get_container_client(container_name)

        # Loop through all the local folders
        for local_folder_path in local_folder_paths:
            # Loop through all the files in the local folder
            for root, _, files in os.walk(local_folder_path):
                for file in files:
                    # Get the local path and name of the file
                    local_file_path = os.path.join(root, file)
                    blob_name = os.path.relpath(local_file_path, local_folder_path).replace("\\", "/")

                    print("blob name:",blob_name)
                    print("********local file path:",local_file_path)
                    

                    # Create a BlobClient object for the file
                    blob_client = container_client.get_blob_client(blob_name)

                    # Upload the file to Azure Blob Storage
                    with open(local_file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
                        print("File uploaded successfully\n")



    
        