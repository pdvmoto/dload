(
  echo "name,payload"
  for i in $(seq 1 10000); do
    printf "name%02d,payload_name_value_%03d\n" "$i" "$i"
  done
) > datafile.csv
