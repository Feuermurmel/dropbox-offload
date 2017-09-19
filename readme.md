# Media Queue
 
A simple script that moves files between an _active_ and an _offload_ directory while keeping the number of files or bytes in the _active_ directory limited by a configurable maximum.
 
I keep the _active_ directory somewhere in a folder that is synced across my devices and the _offload_ directory somewhere on my server with a lot of storage. Then I put all the files (mostly large media files) in the _active_ directory. This script will move all but the first few to the _offload_ directory to prevent my devices from overflowing. While I consume and delete the files in the _active_ directory, the script will move more files from _offload_ directory.
