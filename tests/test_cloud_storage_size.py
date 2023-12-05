import unittest
from ying.cloud_storage_size import get_bucket_objects_count_and_bytes

from dotenv import load_dotenv

load_dotenv()


class TestCloudStorageSize(unittest.TestCase):
    def test_get_bucket_objects_count_and_bytes(self):
        bucket_uri = "gs://bucketname"
        result = get_bucket_objects_count_and_bytes(bucket_uri, engine="auto")
        self.assertIsInstance(result, dict)
        self.assertIn("bytes", result)
        self.assertIn("count", result)


if __name__ == "__main__":
    unittest.main()
