# #!/bin/sh
/Users/s.lafaurie/opt/anaconda3/envs/eda_env/bin/python /Users/s.lafaurie/Documents/pricing-local/MENA/dag-hs-mapping-dps-session/dag.py -d 2
echo "Cronjob ran successfully at $(date)" >> /var/log/hs_rdf_dag.log
