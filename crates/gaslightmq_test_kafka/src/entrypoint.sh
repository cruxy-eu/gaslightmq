# What the container of a cluster runs. `docker.rs` starts it, and explains it.
set -eu
exec 2>&1 3<&0

echo 'gaslightmq: awaiting configuration'
broker=''
while IFS= read -r line <&3; do
  case "$line" in
    '# broker '*)
      broker="/tmp/kafka/${line#\# broker }"
      mkdir -p "$broker/mounted" "$broker/config"
      ;;
    '# start') break ;;
    *) printf '%s\n' "$line" >> "$broker/mounted/server.properties" ;;
  esac
done

for broker in /tmp/kafka/*; do
  /opt/kafka/kafka.Kafka setup \
    --default-configs-dir /etc/kafka/docker \
    --mounted-configs-dir "$broker/mounted" \
    --final-configs-dir "$broker/config"
  /opt/kafka/kafka.Kafka start \
    --config "$broker/config/server.properties" \
    "-Dlog4j2.configurationFile=file:$broker/config/log4j2.yaml" \
    "-Dkafka.logs.dir=$broker/logs" &
done

# The container ends when its input closes, and when a broker ends.
cat <&3 > /dev/null &
wait -n
