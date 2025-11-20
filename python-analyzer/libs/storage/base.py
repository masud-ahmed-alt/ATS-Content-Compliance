"""
Abstract base class for storage services (MinIO, S3, etc.).
"""
from abc import ABC, abstractmethod
from typing import BinaryIO


class StorageClient(ABC):
    """Abstract storage client interface."""
    
    @abstractmethod
    def upload_object(
        self,
        bucket: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        content_type: str = "application/octet-stream"
    ) -> str:
        """Upload object and return URL."""
        pass
    
    @abstractmethod
    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Check if object exists."""
        pass
    
    @abstractmethod
    def create_bucket(self, bucket: str) -> None:
        """Create bucket if it doesn't exist."""
        pass
    
    @abstractmethod
    def bucket_exists(self, bucket: str) -> bool:
        """Check if bucket exists."""
        pass
    
    @abstractmethod
    def get_public_url(self, bucket: str, object_name: str) -> str:
        """Get public URL for object."""
        pass

