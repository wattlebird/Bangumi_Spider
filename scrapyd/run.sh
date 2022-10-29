htpasswd -b -c /etc/nginx/htpasswd $USERNAME $PASSWORD
scrapyd &
sleep 2s
service nginx start
sleep infinity