# Dropbox-Offload

A simple script that moves files between an _active_ and an _offload_ directory while keeping the number of files or bytes in the _active_ directory limited by a configurable maximum.

This script's use is not restricted to [Dropbox](https://www.dropbox.com/) but this is where I'm currently using is. I keep the _active_ directory (called `queue_dir` in the source) somewhere in my Dropbox and the _offload_ directory somewhere on my server with a lot of storage. Then I put all the files (mostly large media files) in the _active_ directory. This script will move all but the first few to the _offload_ directory to prevent by Dropbox from overflowing. While I consume and delete the files in the _active_ directory, the script will move more files from _offload_ directory.
