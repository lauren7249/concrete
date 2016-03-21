#create cluster $278.97/month
gcloud beta dataproc clusters create cluster-1 --zone us-east1-b --master-machine-type n1-standard-1 --master-boot-disk-size 4000 --num-workers 2 --worker-machine-type n1-standard-1 --worker-boot-disk-size 100 --project advisorconnect-1238 --initialization-actions 'gs://advisorconnect-jupyter/ipython.sh','gs://advisorconnect-jupyter/zookeeper.sh'


#create tunnel for jupyter
ssh -i ~/.ssh/google_compute_engine -N -f -L localhost:8888:localhost:8123 lauren@104.196.40.160
#go to localhost:8888 to see the notebook

#HACK 
#be able to go to localhost:5001 to see running docker
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ~/.ssh/google_compute_engine -N -f -L localhost:5001:localhost:5000 lauren@104.196.114.58

#REAL  WAY
gcloud compute firewall-rules create allow-http-5000 --description "Incoming http allowed." --allow tcp:5000 --format json
gcloud compute firewall-rules create allow-https --description "Incoming https allowed." --allow tcp:443 --format json