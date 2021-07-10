# hookup

Hookup is a server that establishes exchange of messages between users. It is my pet project and playground for experiments.

## Under the hood

Hookup uses kafka to store and retrieve messages. Each user has own kafka topic.

## API

### Register user

``` text
/register?user=foo
```

### Send message

``` text
/send?message=Hello&from=bar&to=foo
```

### Read messages

``` text
/read?for=foo
```

## Example

Start `hookup` server.

``` zsh
jim@jim-macbook-pro hookup % ./hookup -addr :8085 -broker localhost:9092
```

Use curl to register user name `foo`.

``` zsh
jim@jim-macbook-pro hookup % curl "http://localhost:8085/register?user=foo"
```

Use curl to send massage to `foo`.

``` zsh
jim@jim-macbook-pro hookup % curl "http://localhost:8085/send?message=Hi&from=bar&to=foo"
```

Use curl to pick up `foo`'s message.

``` zsh
jim@jim-macbook-pro hookup % curl "http://localhost:8085/read?for=foo"
message: Hi, from: bar
```
