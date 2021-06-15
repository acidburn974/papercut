# Custom Papercut NNTP server fork

Papercut fork from : [Papercut](https://github.com/jgrassler/papercut). Code is now in Python 3
and works only with MariaDB as backend. A lot of modules has been removed for usage inside a microservice in a Docker container.

Almost all commands working except feeding from other servers. This script was not used in production at the moment.

## Development

```
docker-compose run -p 119:119 papercut bash
```

## References

https://github.com/jpm/papercut
https://github.com/jgrassler/papercut