# Setup for Automated Tests

As of Aug-08-17, there is a dependency on the 1.7 alpha release of the topology Python APIs. To install them, run: 

```
pip install -Iv streamsx==1.7a3
```

To run the tests, you will also need Redis intalled and in your classpath. 

https://redis.io/download

# Run the tests
```
ant test
```
