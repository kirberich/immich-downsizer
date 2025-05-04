# Immich Downsizer

⚠️ Do not trust this script with your data! It works for me, but it's still completely new and untested.

Python script for downsizing video files in immich by replacing the original files with transcoded ones.

It checks the db for any video files larger than 1080p, and replaces them with the transcoded file (720p by default, but that can be changed in immich)
