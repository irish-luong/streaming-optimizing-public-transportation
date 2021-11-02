#!/usr/bin/env bash
#set -e

SLEEP_SEC=10;


print_banner() {
  # shellcheck disable=SC2028
  echo "Welcome to streaming application"
  echo "waiting for setting environment"
  echo "================"
}


check_turnstile() {
  echo "Check turnstile topics"
  for i in {0..5};
  do
    http_response=$(curl -s -o response.txt -w "%{http_code}" http://rest-proxy:8082/topics/turnstile);

    if [ "$http_response" != "200" ]; then
        echo "turnstile not found";
        echo "Sleep Zzzzzzzzz";
        sleep $SLEEP_SEC;
        if [ "$i" == "5" ]; then echo "Stop process because turnstile topic is not found" && exit 1; fi;
    else
        echo "KSQL is ready:";
        cat response.txt;
        echo "Go"
        break;
    fi;

  done;

}


heath_check_cluster() {
  echo "Heath check some services"
  for i in {0..5};
  do
    http_response=$(curl -s -o response.txt -w "%{http_code}" http://ksql:8088/info);

    if [ "$http_response" != "200" ]; then
        echo "KSQL is not ready";
        echo "Sleep Zzzzzzzzz";
        sleep $SLEEP_SEC;
        if [ "$i" == "5" ]; then echo "Stop process because KSQL is not ready" && exit 1; fi;
    else
        echo "KSQL is ready:";
        cat response.txt;
        echo "Go"
        break;
    fi;

  done;
}

print_banner;
heath_check_cluster;

case "$1" in
        faust_app)
            echo "*** Start Fault App ***";
            faust -A faust_stream worker -l info;
            ;;

        ksql_app)
            echo "*** Start KSQL App ***";
            check_turnstile
            python /app/consumers/ksql.py;
            ;;
        web_app)
            echo "*** Start Web App ***";
            python /app/consumers/server.py
          ;;
        simulation)
           echo "*** Start Producer Simulation ***";
           python /app/producers/simulation.py;
           sleep 10;
           ;;
        *)
            echo $"Usage: $0 {faust_app|ksql_app|web_app|simulation}"
            exit 1
esac