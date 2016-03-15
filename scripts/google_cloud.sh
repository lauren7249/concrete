#create cluster
gcloud beta dataproc clusters create cluster-1 --zone us-east1-b --master-machine-type n1-highmem-4 --master-boot-disk-size 50 --num-workers 10 --worker-machine-type n1-highmem-4 --worker-boot-disk-size 50 --project advisorconnect-1238 --initialization-actions 'gs://advisorconnect-jupyter/ipython.sh'

#create tunnel for jupyter
gcloud compute ssh  --zone=us-east1-b  --ssh-flag="-D 1080" --ssh-flag="-N" --ssh-flag="-n" cluster-1-m
#open jupyter browser
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-datdir=/tmp/

#ssh into master node
gcloud compute --project "advisorconnect-1238" ssh --zone "us-east1-b" "cluster-1-m"