# Setup for Automated Tests

There is a dependency on the 1.13.5 release of the topology Python APIs. To install them, run: 

```
pip install -Iv streamsx==1.13.5
```

To run the tests, you will also need Redis intalled and in your classpath. 

https://redis.io/download

# Run the tests
```
ant test
```
