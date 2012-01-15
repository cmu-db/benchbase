echo "------------------- MONITORING TOOl --------------"
. setenv
if [ ""$DSTAT_CONFIGURED != "true" ]
then
 echo "I believe you haven't configured the environment variables in the \"setenv\" script yet... please do so and relaunch monitor.sh"
else
 echo "I'm recording in "$DSTAT_HOMEDIR"/log_exp_"$DSTAT_EXPERIMENT_ID".csv the system load every "$DSTAT_MONITORING_FREQUENCY" seconds...  Generate your load..."
 if [ ""$DSTAT_MONITOR_MYSQL == "true" ]
  then 
   echo "monitoring mysql"
   $DSTAT_HOMEDIR/dstat --noupdate -T -l -f -c -m -n -d -r --aio -s -g --vm --fs  -i -y -p --disk-util --mysql5-all1 --output $DSTAT_HOMEDIR"/log_exp_"$DSTAT_EXPERIMENT_ID".csv"  $DSTAT_MONITORING_FREQUENCY 
  else
   $DSTAT_HOMEDIR/dstat --noupdate -T -l -f -c -m -n -d -r --aio -s -g --vm --fs  -i -y -p --disk-util --output  $DSTAT_HOMEDIR"/log_exp_"$DSTAT_EXPERIMENT_ID".csv"  $DSTAT_MONITORING_FREQUENCY  
  fi > /dev/null
fi
echo "----------------------------------------------------"
