import os
import logging
from ying.cloud_storage import get_bucket_objects_count_and_bytes
from cloud_sheets_slim import CloudSheetsSlim
from ying.utils import formatter
import config

logger = logging.getLogger(__name__)

google_service_account_json = config.google_service_account_json
google_project_id = config.google_project_id
google_sheets_url = config.google_sheets_url
googel_sheets_name = config.googel_sheets_name
cloud_storage_bucket_column_name = config.cloud_storage_bucket_column_name
cloud_storage_bytes_column_name = config.cloud_storage_bytes_column_name
cloud_storage_count_column_name = config.cloud_storage_count_column_name


def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_service_account_json
    cloud_sheet = CloudSheetsSlim(google_sheets_url, googel_sheets_name)

    rows = cloud_sheet.find({})

    bucket_uri_list = [row.get(cloud_storage_bucket_column_name, None) for row in rows]

    filtered_bucket_uri_list = list(filter(lambda x: x is not None, bucket_uri_list))

    logger.info(filtered_bucket_uri_list)
    for bucket_uri in filtered_bucket_uri_list:
        value = get_bucket_objects_count_and_bytes(
            bucket_uri, project_id=google_project_id
        )
        logger.info(value)
        cloud_sheet.update_one(
            {cloud_storage_bucket_column_name: bucket_uri},
            {
                cloud_storage_bytes_column_name: formatter.human_readable_bytes(
                    value["bytes"], decimal_places=4
                ),
                cloud_storage_count_column_name: value["count"],
            },
            upsert=True,
        )


if __name__ == "__main__":
    main()
