## Requirements

Install presto python client
```
$ pip install presto-python-client
```

## Running the load test

```
./runner.py --host $HOST --port $PORT --parallelism "5,25,50,60,70,75" --duration 300
```

## Help

```
./runner.py --help
```
