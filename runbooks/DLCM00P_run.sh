#!/bin/zsh
set +x
start=$(date +%s)
cd /Volumes/GoogleDrive/My\ Drive/VisualStudio/Longleaf
python3 DLCM00P_transform.py > /Volumes/GoogleDrive/My\ Drive/UNC\ Press-Longleaf/Run\ Logs/DLCM00P_run_log_`date +'%Y%m%d'`.txt
echo "DLCM00P_transform.py complete"
python3 DLCM00P_transform_2.py >> /Volumes/GoogleDrive/My\ Drive/UNC\ Press-Longleaf/Run\ Logs/DLCM00P_run_log_`date +'%Y%m%d'`.txt
echo "DLCM00P_transform_2.py complete"
python3 DLCM00P_transform_3.py >> /Volumes/GoogleDrive/My\ Drive/UNC\ Press-Longleaf/Run\ Logs/DLCM00P_run_log_`date +'%Y%m%d'`.txt
echo "DLCM00P_transform_3.py complete"
python3 DLCM00P_transform_4.py >> /Volumes/GoogleDrive/My\ Drive/UNC\ Press-Longleaf/Run\ Logs/DLCM00P_run_log_`date +'%Y%m%d'`.txt
echo "DLCM00P_transform_4.py complete"
python3 DLCM00P_transform_5.py >> /Volumes/GoogleDrive/My\ Drive/UNC\ Press-Longleaf/Run\ Logs/DLCM00P_run_log_`date +'%Y%m%d'`.txt
echo "DLCM00P_transform_5.py complete"
python3 DLCM00P_transform_6.py >> /Volumes/GoogleDrive/My\ Drive/UNC\ Press-Longleaf/Run\ Logs/DLCM00P_run_log_`date +'%Y%m%d'`.txt
echo "DLCM00P_transform_6.py complete"
end=$(date +%s)
echo "Elapsed Time: $(($end-$start)) seconds"
