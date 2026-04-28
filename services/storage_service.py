from minio import Minio
from minio.error import S3Error
from minio.helpers import ObjectWriteResult
from minio.commonconfig import CopySource
from minio.datatypes import Object

from typing import Iterator

import os
from dotenv import load_dotenv

class StorageService:
    def __init__(self,
                 bucket_name : str = "datalake-stocks",
                 client_endpoint : str = "aistor-server:9000",
                 access_key : str = None,
                 secret_key : str = None,
                 secure : bool = False,
                 cert_check : bool = False
                ):
                
        
        self.bucket_name = bucket_name

        self.client = Minio(
            client_endpoint,
            access_key if access_key is not None else os.getenv("ACCESS_KEY"),
            secret_key if secret_key is not None else os.getenv("SECRET_KEY"),
            secure,
            cert_check,    
        )

        self.check_bucket()

    def check_bucket(self):
        if not(self.client.bucket_exists(self.bucket_name)):
            self.client.make_bucket(bucket_name=self.bucket_name)

    
    def append_object(self, object_name : str, data : object, length : int = -1):
        """
        Append a data stream to an existing object
        """

        try:
            result = self.client.append_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=data,
                length=length
            )
        except S3Error as e:
            print(f"Error '{e.message}' occurred while appending object")

        return result


    def get_object(self, object_name : str, offset : int = 0, length : int = 0, version_id : str = None):
        """
        Returns a object from the storage, or None when a S3Error occurs
        """
        object = None
        try:
            response = self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                offset=offset,
                length=length,
                version_id=version_id
            )
            object = response.data

        except S3Error as e:
            print(f"Error '{e.message}' occurred while getting object")
        
        finally:
            response.close()
            response.release_conn()

        return object
    
    def put_object(self, object_name : str, data : object, length : int = -1):
        try:
            result = self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data = data,
                length=length,
            )

        except S3Error as e:
            print(f"Error '{e.message}' occurred while putting object")

        return result
    
    def remove_object(self, object_name : str, version_id : str = None):
        try:
            self.client.remove_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                version_id=version_id
            )
        except S3Error as e:
            print(f"Error '{e.message}' occurred while removing object")

    def copy_object(self, object_name : str, source : CopySource):
        result = None

        try:
            result = self.client.copy_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                source=source
            )
        except S3Error as e:
            print(f"Error '{e.message}' occurred while copying object")

        return result
    
    def list_objects(self, prefix : str = None, 
                     recursive : bool = False, 
                     include_version : bool = False,
                      print_objects : bool = False
                    ) -> Iterator[Object]:
        '''
        Returns an iterator for the found objects, or none if an error occurred.
        '''

        objects = None

        try:
            objects = self.client.list_objects(bucket_name=self.bucket_name,
                                            prefix=prefix,
                                            recursive=recursive,
                                            include_version=include_version
                                            )    
            if print_objects:
                for o in objects:
                    print(o)
        except S3Error as e:
            print(f"Error '{e.message}' occurred while listing objects")
                
        return objects
