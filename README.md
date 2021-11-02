### Run application
```buildoutcfg
cd <project> && make build;
```

### Services
1. producer
	- Simulate events
2. ksql-app:
	- Create KSQL table
	- Waiting for KSQL server start first
3. faust-app:
   	- Run fault stream
4. web-app:
	- Run Tornado web app and consume message 
	- Depend on ksql-app and faust-app
	