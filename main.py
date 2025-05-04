import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypedDict, cast

import psycopg2
import psycopg2.extras
import requests
from environs import env
from psycopg2._psycopg import connection

env.read_env()


LARGE_VIDEO_QUERY = """
SELECT
    assets.id,
    assets."encodedVideoPath",
    assets."originalPath",
    assets.type,
    exif."exifImageWidth",
    exif."exifImageHeight",
    exif."fileSizeInByte" FROM assets
INNER JOIN exif ON exif."assetId" = assets.id
WHERE
    type='VIDEO'
    and exif."exifImageHeight" > 1080
    and exif."exifImageWidth" > 1080
"""


def refresh_metadata(api_url: str, api_key: str):
    response = requests.request(
        "PUT",
        f"{api_url}/api/jobs/metadataExtraction",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-api-key": api_key,
        },
        json={"command": "start", "force": True},
    )

    print(response.json())


class Video(TypedDict):
    id: str
    encoded_path: Path | None
    original_path: Path | None
    width: int
    height: int
    size: int


@dataclass(kw_only=True, slots=True)
class Compressor:
    library_path: Path

    db_host: str
    db_name: str
    db_port: str
    db_user: str
    db_password: str

    conn: connection = field(init=False)

    def __post_init__(self):
        self.conn = self._get_db()

    def _get_db(self) -> connection:
        return psycopg2.connect(
            database=self.db_name,
            host=self.db_host,
            user=self.db_user,
            password=self.db_password,
            port=self.db_port,
        )

    def get_large_videos(self) -> list[Video]:
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cursor.execute(LARGE_VIDEO_QUERY)
        return [
            {
                "id": cast(str, row["id"]),
                "encoded_path": self.get_actual_path(
                    cast(str, row["encodedVideoPath"])
                ),
                "original_path": self.get_actual_path(cast(str, row["originalPath"])),
                "width": cast(int, row["exifImageWidth"]),
                "height": cast(int, row["exifImageHeight"]),
                "size": cast(int, row["fileSizeInByte"]),
            }
            for row in cursor.fetchall()
        ]

    def get_actual_path(self, db_path: str) -> Path | None:
        if not db_path.startswith("upload/"):
            return None

        return self.library_path / db_path[7:]


def main():
    api_url = env.str("API_URL")
    api_key = env.str("API_KEY")

    compressor = Compressor(
        library_path=env.path("LIBRARY_PATH"),
        db_host=env.str("DB_HOST"),
        db_name=env.str("DB_NAME"),
        db_port=env.str("DB_PORT"),
        db_user=env.str("DB_USER"),
        db_password=env.str("DB_PASSWORD"),
    )

    large_videos = compressor.get_large_videos()

    print(f"found {len(large_videos)} large videos to compress")

    for large_video in large_videos:
        print(f"processing {large_video['original_path']}")
        if large_video["encoded_path"] is None or large_video["original_path"] is None:
            print(
                f"Path '{large_video['encoded_path']}' for {large_video['original_path']} doesn't start with 'upload', skipping!"
            )
            continue

        # copy the encoded video to a temporary file
        tmp_file = shutil.copyfile(
            large_video["encoded_path"],
            large_video["original_path"].parent / "tmp",
        )

        # copy the exif metadata from the _original_ file on the encoded one
        subprocess.run(
            [
                "exiftool",
                "-tagsFromFile",
                large_video["original_path"],
                tmp_file,
                "-overwrite_original",
            ]
        )

        # remove the width/height exif attributes, as those are wrong now
        subprocess.run(
            [
                "exiftool",
                "-ImageHeight=",
                "-ImageWidth=",
                tmp_file,
                "-overwrite_original",
            ]
        )

        # overwrite the original file with the temporary one
        shutil.move(tmp_file, large_video["original_path"])

    # Trigger a rescan of all metadata
    refresh_metadata(api_url=api_url, api_key=api_key)


if __name__ == "__main__":
    main()
