#!/bin/zsh
set +x
start=$(date +%s)
cd /Volumes/GoogleDrive/My\ Drive/VisualStudio/Longleaf
python3 DLTXT00P_transform.py > /Volumes/GoogleDrive/My\ Drive/UNC\ Press-Longleaf/Run\ Logs/DLTXT00P_run_log_`date +'%Y%m%d'`.txt
end=$(date +%s)
echo "Elapsed Time: $(($end-$start)) sec, $((($end-$start)/60)):$((($end-$start)%60)) min"

