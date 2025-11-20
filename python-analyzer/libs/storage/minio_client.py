"""
MinIO storage client implementation.
"""
import io
import logging
from typing import BinaryIO
from minio import Minio
from minio.error import S3Error

from ..common.exceptions import StorageError, RetryableError
from ..common.config import MinioConfig
from ..common.retry import retry_with_backoff, RetryConfig
from .base import StorageClient

logger = logging.getLogger(__name__)


class MinioStorageClient(StorageClient):
    """MinIO implementation of StorageClient."""
    
    def __init__(self, config: MinioConfig):
        self.config = config
        self.client = Minio(
            config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure
        )
        self._ensure_bucket()
        self._setup_lifecycle()
    
    def _ensure_bucket(self) -> None:
        """Ensure bucket exists."""
        try:
            if not self.client.bucket_exists(self.config.bucket):
                self.client.make_bucket(self.config.bucket)
                logger.info(f"[storage] Created bucket: {self.config.bucket}")
        except Exception as e:
            logger.warning(f"[storage] Failed to ensure bucket: {e}")
    
    def _setup_lifecycle(self) -> None:
        """Setup bucket lifecycle policy."""
        try:
            from xml.etree.ElementTree import Element, SubElement, tostring
            
            rule = Element("LifecycleConfiguration")
            rule1 = SubElement(rule, "Rule")
            SubElement(rule1, "ID").text = "AutoDelete"
            SubElement(rule1, "Status").text = "Enabled"
            exp = SubElement(rule1, "Expiration")
            SubElement(exp, "Days").text = str(self.config.expiry_days)
            
            xml_config = tostring(rule, encoding="utf-8", method="xml")
            self.client.set_bucket_lifecycle(self.config.bucket, xml_config)
            logger.info(f"[storage] Lifecycle policy set: {self.config.expiry_days} days")
        except Exception as e:
            logger.warning(f"[storage] Failed to setup lifecycle: {e}")
    
    def upload_object(
        self,
        bucket: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        content_type: str = "application/octet-stream"
    ) -> str:
        """Upload object with retry logic."""
        def _upload():
            try:
                # Reset stream position if needed
                if hasattr(data, 'seek'):
                    data.seek(0)
                    
                self.client.put_object(
                    bucket,
                    object_name,
                    data,
                    length=length,
                    content_type=content_type
                )
                return self.get_public_url(bucket, object_name)
            except S3Error as e:
                error_msg = str(e).lower()
                is_transient = any(keyword in error_msg for keyword in [
                    "insufficient", "timeout", "connection", "network",
                    "temporary", "retry", "unable to write", "no online disks"
                ])
                if is_transient:
                    raise RetryableError(f"Transient storage error: {e}", service="storage")
                else:
                    raise StorageError(f"Storage error: {e}", operation="upload")
        
        retry_config = RetryConfig(max_retries=3, initial_delay=1.0)
        return retry_with_backoff(
            _upload,
            config=retry_config,
            operation=f"upload {object_name}"
        )
    
    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Check if object exists."""
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error:
            return False
    
    def create_bucket(self, bucket: str) -> None:
        """Create bucket."""
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
        except Exception as e:
            raise StorageError(f"Failed to create bucket: {e}", operation="create_bucket")
    
    def bucket_exists(self, bucket: str) -> bool:
        """Check if bucket exists."""
        try:
            return self.client.bucket_exists(bucket)
        except Exception as e:
            logger.error(f"[storage] Error checking bucket: {e}")
            return False
    
    def get_public_url(self, bucket: str, object_name: str) -> str:
        """Get public URL for object."""
        endpoint = self.config.endpoint.split("://")[-1].rstrip("/")
        return f"{endpoint}/{bucket}/{object_name}" if endpoint else f"{bucket}/{object_name}"

