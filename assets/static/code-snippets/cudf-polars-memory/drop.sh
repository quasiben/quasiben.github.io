# /usr/local/sbin/drop_caches.sh
#!/bin/bash
set -e

sync
echo 3 > /proc/sys/vm/drop_caches
